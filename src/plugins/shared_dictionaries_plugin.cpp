#include "shared_dictionaries_plugin.hpp"
#include "resolve_type.hpp"
#include "utils/size_estimation_utils.hpp"

namespace opossum {

std::string SharedDictionariesPlugin::description() const { return "Shared dictionaries plugin"; }

void SharedDictionariesPlugin::start() {
  reset();
  std::cout << "SHARED DICTIONARIES PLUGIN: Processing starts" << std::endl;
  const auto env_jaccard_index_threshold = std::getenv(_env_variable_name);
  if (env_jaccard_index_threshold) {
    jaccard_index_threshold = std::stod(env_jaccard_index_threshold);
  }
  std::cout << "Jaccard-index threshold is set to: " << jaccard_index_threshold << std::endl;
  _process_for_every_column();
  std::cout << "SHARED DICTIONARIES PLUGIN: Processing ended:" << std::endl;
  _print_processing_result();
}

void SharedDictionariesPlugin::stop() { reset(); }

void SharedDictionariesPlugin::_process_for_every_column() {
  auto table_names = storage_manager.table_names();
  std::sort(table_names.begin(), table_names.end());
  for (const auto& table_name : table_names) {
    const auto table = storage_manager.get_table(table_name);
    const auto column_count = table->column_count();
    for (auto column_id = ColumnID{0}; column_id < column_count; ++column_id) {
      const auto column_data_type = table->column_definitions()[column_id].data_type;
      const auto column_name = table->column_definitions()[column_id].name;
      resolve_data_type(column_data_type, [&](const auto type) {
        using ColumnDataType = typename decltype(type)::type;
        _process_column<ColumnDataType>(table, table_name, column_id, column_name);
      });
    }
  }
}

template <typename T>
void SharedDictionariesPlugin::_process_column(const std::shared_ptr<Table> table, const std::string& table_name,
                                               const ColumnID column_id, const std::string& column_name) {
  const auto allocator = PolymorphicAllocator<T>{};
  auto segments_to_merge_at = std::vector<std::vector<SegmentToMergeInfo<T>>>{};
  auto shared_dictionaries = std::vector<std::shared_ptr<const pmr_vector<T>>>{};
  _initialize_shared_dictionaries(shared_dictionaries, segments_to_merge_at, table, column_id, column_name);

  std::optional<SegmentToMergeInfo<T>> previous_segment_info_opt = std::nullopt;

  const auto chunk_count = table->chunk_count();
  for (auto chunk_id = ChunkID{0}; chunk_id < chunk_count; ++chunk_id) {
    const auto chunk = table->get_chunk(chunk_id);
    const auto segment = chunk->get_segment(column_id);
    _total_previous_bytes += segment->memory_usage(MemoryUsageCalculationMode::Full);

    auto current_dictionary_segment = std::dynamic_pointer_cast<DictionarySegment<T>>(segment);
    if (current_dictionary_segment && !current_dictionary_segment->uses_dictionary_sharing()) {
      const auto current_dictionary = current_dictionary_segment->dictionary();
      const auto current_segment_info =
          SegmentToMergeInfo<T>{current_dictionary_segment, chunk, column_id, column_name, false};

      const auto [best_shared_dictionary_index, best_shared_dictionary] = _compare_with_existing_shared_dictionaries<T>(
          current_dictionary, shared_dictionaries, segments_to_merge_at, allocator);

      auto merged_current_dictionary = false;
      if (best_shared_dictionary_index >= 0 && best_shared_dictionary) {
        // Merge with existing shared dictionary
        shared_dictionaries[best_shared_dictionary_index] = best_shared_dictionary;
        segments_to_merge_at[best_shared_dictionary_index].push_back(current_segment_info);
        merged_current_dictionary = true;
      } else if (previous_segment_info_opt) {
        // Check with previous segment
        const auto shared_dictionary_with_previous =
            _compare_with_previous_dictionary(current_dictionary, *previous_segment_info_opt, allocator);
        if (shared_dictionary_with_previous) {
          // Merge with previous dictionary
          shared_dictionaries.emplace_back(shared_dictionary_with_previous);
          segments_to_merge_at.emplace_back(
              std::vector<SegmentToMergeInfo<T>>{current_segment_info, *previous_segment_info_opt});
          merged_current_dictionary = true;
        }
      }

      // Save unmerged current dictionary for possible later merge
      previous_segment_info_opt = merged_current_dictionary ? std::nullopt : std::make_optional(current_segment_info);
    }
  }
  _apply_shared_dictionaries(shared_dictionaries, segments_to_merge_at, table_name, allocator);
}

template <typename T>
void SharedDictionariesPlugin::_initialize_shared_dictionaries(
    std::vector<std::shared_ptr<const pmr_vector<T>>>& shared_dictionaries,
    std::vector<std::vector<SegmentToMergeInfo<T>>>& segments_to_merge_at, const std::shared_ptr<Table> table,
    const ColumnID column_id, const std::string& column_name) {
  auto shared_dictionaries_set = std::set<std::shared_ptr<const pmr_vector<T>>>{};
  auto existing_merged_segments = 0;
  const auto chunk_count = table->chunk_count();
  for (auto chunk_id = ChunkID{0}; chunk_id < chunk_count; ++chunk_id) {
    const auto chunk = table->get_chunk(chunk_id);
    const auto segment = chunk->get_segment(column_id);

    auto dictionary_segment = std::dynamic_pointer_cast<DictionarySegment<T>>(segment);
    if (dictionary_segment && dictionary_segment->uses_dictionary_sharing()) {
      existing_merged_segments++;
      const auto [iter, inserted] = shared_dictionaries_set.insert(dictionary_segment->dictionary());
      const auto segment_info = SegmentToMergeInfo<T>{dictionary_segment, chunk, column_id, column_name, true};
      if (inserted) {
        segments_to_merge_at.emplace_back(std::vector<SegmentToMergeInfo<T>>{segment_info});
      } else {
        const auto insert_index = std::distance(shared_dictionaries_set.begin(), iter);
        segments_to_merge_at[insert_index].push_back(segment_info);
      }
    }
  }
  shared_dictionaries.insert(shared_dictionaries.begin(), shared_dictionaries_set.begin(),
                             shared_dictionaries_set.end());
}

template <typename T>
std::pair<int32_t, std::shared_ptr<const pmr_vector<T>>>
SharedDictionariesPlugin::_compare_with_existing_shared_dictionaries(
    const std::shared_ptr<const pmr_vector<T>> current_dictionary,
    const std::vector<std::shared_ptr<const pmr_vector<T>>>& shared_dictionaries,
    const std::vector<std::vector<SegmentToMergeInfo<T>>>& segments_to_merge_at,
    const PolymorphicAllocator<T>& allocator) {
  auto best_shared_dictionary_index = -1;
  auto best_jaccard_index = -1;
  std::shared_ptr<const pmr_vector<T>> best_shared_dictionary = nullptr;

  for (auto shared_dictionary_index = 0ul; shared_dictionary_index < shared_dictionaries.size();
       ++shared_dictionary_index) {
    auto shared_dictionary = shared_dictionaries[shared_dictionary_index];
    auto union_result = std::make_shared<pmr_vector<T>>(allocator);
    union_result->reserve(std::max(current_dictionary->size(), shared_dictionary->size()));
    std::set_union(current_dictionary->cbegin(), current_dictionary->cend(), shared_dictionary->cbegin(),
                   shared_dictionary->cend(), std::back_inserter(*union_result));
    const auto total_size = current_dictionary->size() + shared_dictionary->size();
    const auto union_size = union_result->size();
    const auto jaccard_index = _calc_jaccard_index(union_size, total_size - union_size);
    if (jaccard_index > best_jaccard_index) {
      if (_should_merge(jaccard_index, current_dictionary->size(), union_size,
                        segments_to_merge_at[shared_dictionary_index])) {
        best_shared_dictionary_index = static_cast<int32_t>(shared_dictionary_index);
        best_jaccard_index = jaccard_index;
        union_result->shrink_to_fit();
        best_shared_dictionary = union_result;
      }
    }
  }

  return std::make_pair(best_shared_dictionary_index, best_shared_dictionary);
}

template <typename T>
std::shared_ptr<const pmr_vector<T>> SharedDictionariesPlugin::_compare_with_previous_dictionary(
    const std::shared_ptr<const pmr_vector<T>> current_dictionary, const SegmentToMergeInfo<T>& previous_segment_info,
    const PolymorphicAllocator<T>& allocator) {
  const auto previous_dictionary = previous_segment_info.segment->dictionary();
  auto union_result = std::make_shared<pmr_vector<T>>(allocator);
  union_result->reserve(std::max(current_dictionary->size(), previous_dictionary->size()));
  std::set_union(current_dictionary->cbegin(), current_dictionary->cend(), previous_dictionary->cbegin(),
                 previous_dictionary->cend(), std::back_inserter(*union_result));
  const auto total_size = current_dictionary->size() + previous_dictionary->size();
  const auto union_size = union_result->size();
  const auto jaccard_index = _calc_jaccard_index(union_size, total_size - union_size);
  if (_should_merge(jaccard_index, current_dictionary->size(), union_size,
                    std::vector<SegmentToMergeInfo<T>>{previous_segment_info})) {
    union_result->shrink_to_fit();
    return union_result;
  } else {
    return nullptr;
  }
}

template <typename T>
void SharedDictionariesPlugin::_apply_shared_dictionaries(
    const std::vector<std::shared_ptr<const pmr_vector<T>>>& shared_dictionaries,
    const std::vector<std::vector<SegmentToMergeInfo<T>>>& segments_to_merge_at, const std::string& table_name,
    const PolymorphicAllocator<T>& allocator) {
  Assert(shared_dictionaries.size() == segments_to_merge_at.size(),
         "The two vectors used for the merging must have the same size.");
  const auto shared_dictionaries_size = shared_dictionaries.size();
  for (auto shared_dictionary_index = 0u; shared_dictionary_index < shared_dictionaries_size;
       ++shared_dictionary_index) {
    const auto shared_dictionary = shared_dictionaries[shared_dictionary_index];
    const auto segments_to_merge = segments_to_merge_at[shared_dictionary_index];
    Assert(segments_to_merge.size() >= 2, "At least 2 segments should be merged.");
    if (std::any_of(segments_to_merge.cbegin(), segments_to_merge.cend(),
                    [](const SegmentToMergeInfo<T> segment) { return !segment.already_merged; })) {
      _num_shared_dictionaries++;
      auto shared_dictionary_memory_usage = _calc_dictionary_memory_usage<T>(shared_dictionary);
      const auto new_dictionary_memory_usage = shared_dictionary_memory_usage;

      auto previous_dictionary_memory_usage = 0ul;
      if (std::any_of(segments_to_merge.cbegin(), segments_to_merge.cend(),
                      [](const SegmentToMergeInfo<T> segment) { return segment.already_merged; })) {
        previous_dictionary_memory_usage += shared_dictionary_memory_usage;
      }

      for (auto segment_to_merge : segments_to_merge) {
        const auto segment = segment_to_merge.segment;
        _num_merged_dictionaries++;
        if (!segment_to_merge.already_merged){
          previous_dictionary_memory_usage += _calc_dictionary_memory_usage(segment->dictionary());
          _modified_previous_bytes += segment->memory_usage(MemoryUsageCalculationMode::Full);
        }

        // Create new dictionary encoded segment with adjusted attribute vector and shared dictionary
        const auto new_attribute_vector = _create_new_attribute_vector<T>(segment, shared_dictionary, allocator);
        const auto new_dictionary_segment =
            std::make_shared<DictionarySegment<T>>(shared_dictionary, new_attribute_vector, true);

        // Replace segment in chunk
        segment_to_merge.chunk->replace_segment(segment_to_merge.column_id, new_dictionary_segment);
      }

      Assert(new_dictionary_memory_usage < previous_dictionary_memory_usage,
             "New dictionary memory usage should be lower than previous");
      const auto bytes_saved = previous_dictionary_memory_usage - new_dictionary_memory_usage;
      _total_bytes_saved += bytes_saved;

      std::cout << "Merged " << segments_to_merge.size() << " dictionaries saving " << bytes_saved
                << " bytes @ Table=" << table_name << ", Column=" << segments_to_merge[0].column_name << std::endl;
    }
  }
}

template <typename T>
std::shared_ptr<const BaseCompressedVector> SharedDictionariesPlugin::_create_new_attribute_vector(
    const std::shared_ptr<DictionarySegment<T>> segment, const std::shared_ptr<const pmr_vector<T>> shared_dictionary,
    const PolymorphicAllocator<T>& allocator) {
  const auto chunk_size = segment->size();
  const auto max_value_id = static_cast<uint32_t>(shared_dictionary->size());

  auto uncompressed_attribute_vector = pmr_vector<uint32_t>{allocator};
  uncompressed_attribute_vector.reserve(chunk_size);

  for (auto chunk_index = ChunkOffset{0}; chunk_index < chunk_size; ++chunk_index) {
    const auto search_value_opt = segment->get_typed_value(chunk_index);
    if (search_value_opt) {
      // Find and add new value id using binary search
      const auto search_iter =
          std::lower_bound(shared_dictionary->cbegin(), shared_dictionary->cend(), search_value_opt.value());
      Assert(search_iter != shared_dictionary->end(), "Shared dictionary does not contain value.");
      const auto found_index = std::distance(shared_dictionary->cbegin(), search_iter);
      uncompressed_attribute_vector.emplace_back(found_index);
    } else {
      // Assume that search value is NULL
      uncompressed_attribute_vector.emplace_back(max_value_id);
    }
  }

  return std::shared_ptr<const BaseCompressedVector>(compress_vector(
      uncompressed_attribute_vector, VectorCompressionType::FixedWidthInteger, allocator, {max_value_id}));
}

// Copied from DictionarySegment::memory_usage
template <typename T>
size_t SharedDictionariesPlugin::_calc_dictionary_memory_usage(const std::shared_ptr<const pmr_vector<T>> dictionary) {
  if constexpr (std::is_same_v<T, pmr_string>) {
    return string_vector_memory_usage(*dictionary, MemoryUsageCalculationMode::Full);
  }
  return dictionary->size() * sizeof(typename decltype(dictionary)::element_type::value_type);
}

double SharedDictionariesPlugin::_calc_jaccard_index(size_t union_size, size_t intersection_size) {
  return static_cast<double>(intersection_size) / static_cast<double>(union_size);
}

bool _increases_attribute_vector_width(const size_t shared_dictionary_size, const size_t current_dictionary_size) {
  if (current_dictionary_size <= std::numeric_limits<uint8_t>::max() &&
      shared_dictionary_size > std::numeric_limits<uint8_t>::max()) {
    return true;
  }

  if (current_dictionary_size <= std::numeric_limits<uint16_t>::max() &&
      shared_dictionary_size > std::numeric_limits<uint16_t>::max()) {
    return true;
  }

  return false;
}

template <typename T>
bool SharedDictionariesPlugin::_should_merge(const double jaccard_index, const size_t current_dictionary_size,
                                             const size_t shared_dictionary_size,
                                             const std::vector<SegmentToMergeInfo<T>>& shared_segments) {
  if (jaccard_index >= jaccard_index_threshold) {
    if (!_increases_attribute_vector_width(shared_dictionary_size, current_dictionary_size)) {
      return std::none_of(shared_segments.cbegin(), shared_segments.cend(),
                          [shared_dictionary_size](const SegmentToMergeInfo<T> segment) {
                            return _increases_attribute_vector_width(shared_dictionary_size,
                                                                     segment.segment->dictionary()->size());
                          });
    }
  }
  return false;
}

void SharedDictionariesPlugin::_print_processing_result() {
  const auto total_save_percentage =
      _total_previous_bytes == 0
          ? 0.0
          : (static_cast<double>(_total_bytes_saved) / static_cast<double>(_total_previous_bytes)) * 100.0;
  const auto modified_save_percentage =
      _modified_previous_bytes == 0
          ? 0.0
          : (static_cast<double>(_total_bytes_saved) / static_cast<double>(_modified_previous_bytes)) * 100.0;

  std::cout << "Merged " << _num_merged_dictionaries << " dictionaries to " << _num_shared_dictionaries
            << " shared dictionaries\n";
  std::cout << "Saved " << _total_bytes_saved << " bytes (" << std::ceil(modified_save_percentage) << "% of modified, "
            << std::ceil(total_save_percentage) << "% of total)" << std::endl;
}
void SharedDictionariesPlugin::reset() {
  _total_bytes_saved = 0ul;
  _total_previous_bytes = 0ul;
  _modified_previous_bytes = 0u;
  _num_merged_dictionaries = 0u;
  _num_shared_dictionaries = 0u;
}

EXPORT_PLUGIN(SharedDictionariesPlugin)

}  // namespace opossum

#include "dictionary_sharing_task.hpp"

#include <algorithm>
#include <fstream>
#include <iostream>
#include <iterator>
#include <sstream>
#include <string>
#include <vector>

#include "hyrise.hpp"
#include "lossless_cast.hpp"
#include "operators/table_wrapper.hpp"
#include "storage/dictionary_segment.hpp"
#include "types.hpp"
#include "utils/size_estimation_utils.hpp"

namespace opossum {

// IDEA: Not only compare with previous segment but also with already created dictionaries. This would also allow cross column comparisons.
// IDEA: set_union can be optimized if jaccard index threshold is not reachable by exiting earlier.

struct Memory_Usage_Stats {
  Memory_Usage_Stats() {}

  Memory_Usage_Stats(long init_previous, long init_current)
      : previous(static_cast<double>(init_previous)), current(static_cast<double>(init_current)) {}

  double previous = 0.0;
  double current = 0.0;
};

struct Segment_Memory_Usage_Stats {
  Memory_Usage_Stats attribute_vector_memory_usage;
  Memory_Usage_Stats dictionary_memory_usage;
};

DictionarySharingTask::DictionarySharingTask(double init_jaccard_index_threshold,
                                             bool init_check_for_attribute_vector_size_increase)
    : jaccard_index_threshold(init_jaccard_index_threshold),
      check_for_attribute_vector_size_increase(init_check_for_attribute_vector_size_increase) {}

/**
 * copied from DictionarySegment::memory_usage
 */
template <typename T>
size_t calc_dictionary_memory_usage(const std::shared_ptr<const pmr_vector<T>> dictionary) {
  // TODO(hig): It can happen that the new dictionary is bigger than the old one, although the dictionaries have the same values
  // Is there maybe a string compression?

  if constexpr (std::is_same_v<T, pmr_string>) {
    return string_vector_memory_usage(*dictionary, MemoryUsageCalculationMode::Sampled);
  }
  return dictionary->size() * sizeof(typename decltype(dictionary)::element_type::value_type);
}

bool _shared_dictionary_increases_attribute_vector_size(const size_t shared_dictionary_size,
                                                        const size_t current_dictionary_size) {
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

/**
 * Returns the difference of the estimated memory usage.
 */
template <typename T>
Segment_Memory_Usage_Stats apply_shared_dictionary_to_segments(
    const std::vector<SegmentChunkColumn<T>>& segment_chunk_columns,
    const std::shared_ptr<const pmr_vector<T>> shared_dictionary, const std::string& table_name,
    const PolymorphicAllocator<T>& allocator) {
  auto previous_attribute_vector_memory_usage = 0l;
  auto new_attribute_vector_memory_usage = 0l;
  auto previous_dictionary_memory_usage = 0l;
  auto new_dictionary_memory_usage = static_cast<int64_t>(calc_dictionary_memory_usage(shared_dictionary));

  const auto max_value_id = static_cast<uint32_t>(shared_dictionary->size());
  for (auto segment_chunk_column : segment_chunk_columns) {
    const auto segment = segment_chunk_column.segment;
    const auto chunk_size = segment->size();

    // Construct new attribute vector that maps to the shared dictionary
    auto uncompressed_attribute_vector = pmr_vector<uint32_t>{allocator};
    uncompressed_attribute_vector.reserve(chunk_size);

    for (auto chunk_index = ChunkOffset{0}; chunk_index < chunk_size; ++chunk_index) {
      const auto search_value_opt = segment->get_typed_value(chunk_index);
      if (search_value_opt) {
        // Find and add new value id using binary search
        const auto search_iter =
            std::lower_bound(shared_dictionary->cbegin(), shared_dictionary->cend(), search_value_opt.value());
        DebugAssert(search_iter != shared_dictionary->end(), "Merged dictionary does not contain value.");
        const auto found_index = std::distance(shared_dictionary->cbegin(), search_iter);
        uncompressed_attribute_vector.emplace_back(found_index);
      } else {
        // Assume that search value is NULL
        uncompressed_attribute_vector.emplace_back(max_value_id);
      }
    }

    const auto compressed_attribute_vector = std::shared_ptr<const BaseCompressedVector>(compress_vector(
        uncompressed_attribute_vector, VectorCompressionType::FixedWidthInteger, allocator, {max_value_id}));

    // Create new dictionary segment
    const auto new_dictionary_segment =
        std::make_shared<DictionarySegment<T>>(shared_dictionary, compressed_attribute_vector);

    // Replace dictionary segment with new one that has a shared dictionary
    previous_attribute_vector_memory_usage += segment->attribute_vector()->data_size();
    previous_dictionary_memory_usage += calc_dictionary_memory_usage(segment->dictionary());

    segment_chunk_column.chunk->replace_segment(segment_chunk_column.column_id, new_dictionary_segment);

    new_attribute_vector_memory_usage += new_dictionary_segment->attribute_vector()->data_size();
  }

  const auto attribute_vector_memory_usage_diff =
      new_attribute_vector_memory_usage - previous_attribute_vector_memory_usage;
  const auto dictionary_memory_usage_diff = new_dictionary_memory_usage - previous_dictionary_memory_usage;

  const auto attribute_vector_relative_memory_diff = static_cast<double>(new_attribute_vector_memory_usage) * 100.0 /
                                                     static_cast<double>(previous_attribute_vector_memory_usage);
  const auto dictionary_relative_memory_diff =
      static_cast<double>(new_dictionary_memory_usage) * 100.0 / static_cast<double>(previous_dictionary_memory_usage);

  Assert(dictionary_memory_usage_diff <= 0, "Dictionary grew in size, this should not happen.");

  // TODO: output table name and column name
  std::cout << "Table \"" << table_name << "\" - Merged " << segment_chunk_columns.size()
            << " dictionaries for column \"" << segment_chunk_columns[0].column_name << "\" ["
            << segment_chunk_columns[0].column_id << "]. (memory diff: "
            << "dict=" << dictionary_memory_usage_diff << " bytes (" << dictionary_relative_memory_diff << "%), "
            << "attr=" << attribute_vector_memory_usage_diff << " bytes (" << attribute_vector_relative_memory_diff
            << "%)"
            << ")" << std::endl;

  return Segment_Memory_Usage_Stats{
      Memory_Usage_Stats{previous_attribute_vector_memory_usage, new_attribute_vector_memory_usage},
      Memory_Usage_Stats{previous_dictionary_memory_usage, new_dictionary_memory_usage},
  };
}

double calc_jaccard_index(size_t union_size, size_t intersection_size) {
  return static_cast<double>(intersection_size) / static_cast<double>(union_size);
}

template <typename T>
void log_jaccard_index(const double jaccard_index, const std::string& table_name, const std::string& column_name,
                       const std::string& compare_type, const DataType& column_data_type, const ChunkID chunk_id,
                       const std::shared_ptr<DictionarySegment<T>> dictionary_segment,
                       std::ofstream& output_file_stream) {
  const auto data_type_name = data_type_to_string.left.at(column_data_type);
  const auto dictionary = dictionary_segment->dictionary();
  output_file_stream << table_name << ";" << column_name << ";" << data_type_name << ";" << chunk_id << ";"
                     << compare_type << ";" << dictionary->size() << ";" << jaccard_index << std::endl;
}

template <typename T>
bool DictionarySharingTask::should_merge(const double jaccard_index, const size_t current_dictionary_size,
                                         const size_t shared_dictionary_size,
                                         std::vector<SegmentChunkColumn<T>> shared_segments) {
  if (jaccard_index >= jaccard_index_threshold) {
    if (!check_for_attribute_vector_size_increase) return true;
    if (!_shared_dictionary_increases_attribute_vector_size(shared_dictionary_size, current_dictionary_size)) {
      return std::none_of(shared_segments.cbegin(), shared_segments.cend(),
                          [shared_dictionary_size](const SegmentChunkColumn<T> segment) {
                            return _shared_dictionary_increases_attribute_vector_size(
                                shared_dictionary_size, segment.segment->dictionary()->size());
                          });
    }
  }
  return false;
}

void DictionarySharingTask::do_segment_sharing(std::optional<std::ofstream> csv_output_stream_opt) {
  std::cout << std::setprecision(4) << std::fixed;
  auto total_merged_dictionaries = 0ul;
  auto total_new_shared_dictionaries = 0ul;
  auto memory_usage_difference = Segment_Memory_Usage_Stats{};

  // Write output csv header
  if (csv_output_stream_opt) {
    *csv_output_stream_opt << "Table;Column;DataType;Chunk;CompareType;DictionarySize;JaccardIndex\n";
  }

  // Get tables using storage manager
  auto& sm = Hyrise::get().storage_manager;

  auto table_names = sm.table_names();
  std::sort(table_names.begin(), table_names.end());

  // Calculate jaccard index for each column in each table
  // The jaccard index is calculated between a dictionary segment and its preceding dictionary segment
  for (const auto& table_name : table_names) {
    const auto table = sm.get_table(table_name);
    // BinaryWriter::write(*table, table_path + table_name + ".bin");
    const auto column_count = table->column_count();
    const auto chunk_count = table->chunk_count();

    for (auto column_id = ColumnID{0}; column_id < column_count; ++column_id) {
      const auto column_data_type = table->column_definitions()[column_id].data_type;
      const auto column_name = table->column_definitions()[column_id].name;
      // std::cout << "column " << column_name << " is id " << column_id << std::endl;
      resolve_data_type(column_data_type, [&](const auto type) {
        using ColumnDataType = typename decltype(type)::type;

        const auto allocator = PolymorphicAllocator<ColumnDataType>{};

        auto shared_dictionaries = std::vector<std::shared_ptr<pmr_vector<ColumnDataType>>>{};
        auto segments_to_merge_at = std::vector<std::vector<SegmentChunkColumn<ColumnDataType>>>{};
        auto last_merged_index = -1;

        std::optional<SegmentChunkColumn<ColumnDataType>> previous_segment_info_opt = std::nullopt;

        for (auto chunk_id = ChunkID{0}; chunk_id < chunk_count; ++chunk_id) {
          const auto chunk = table->get_chunk(chunk_id);
          const auto segment = chunk->get_segment(column_id);

          auto current_dictionary_segment = std::dynamic_pointer_cast<DictionarySegment<ColumnDataType>>(segment);
          if (!current_dictionary_segment) {
            std::cerr << "Not a dictionary segment! " << chunk_id << "/" << column_id << std::endl;
            continue;
          }
          const auto current_dictionary = current_dictionary_segment->dictionary();
          auto current_jaccard_index = 0.0;
          auto current_compare_type = std::string{};

          auto potential_new_shared_dictionary = std::make_shared<pmr_vector<ColumnDataType>>(allocator);
          auto potential_new_shared_dictionary_index = -1;

          // Try to merge with last shared dictionary
          if (last_merged_index >= 0) {
            //TODO(hig): Fix code duplication
            auto shared_dictionary = shared_dictionaries[last_merged_index];
            std::set_union(current_dictionary->cbegin(), current_dictionary->cend(), shared_dictionary->cbegin(),
                           shared_dictionary->cend(), std::back_inserter(*potential_new_shared_dictionary));
            const auto total_size = current_dictionary->size() + shared_dictionary->size();
            const auto union_size = potential_new_shared_dictionary->size();
            current_jaccard_index = calc_jaccard_index(union_size, total_size - union_size);
            current_compare_type = "ExistingShared @ " + std::to_string(last_merged_index);
            if (should_merge(current_jaccard_index, current_dictionary->size(), union_size,
                             segments_to_merge_at[last_merged_index])) {
              potential_new_shared_dictionary_index = last_merged_index;
            } else {
              potential_new_shared_dictionary->clear();
            }
          }

          // Try to merge with other existing shared dictionaries.
          // Traverse in reverse order to first check the most recent created shared dictioanries.
          if (potential_new_shared_dictionary_index < 0) {
            for (auto shared_dictionary_index = static_cast<int>(shared_dictionaries.size()) - 1;
                 shared_dictionary_index >= 0; --shared_dictionary_index) {
              if (shared_dictionary_index != last_merged_index) {
                auto shared_dictionary = shared_dictionaries[shared_dictionary_index];
                std::set_union(current_dictionary->cbegin(), current_dictionary->cend(), shared_dictionary->cbegin(),
                               shared_dictionary->cend(), std::back_inserter(*potential_new_shared_dictionary));
                const auto total_size = current_dictionary->size() + shared_dictionary->size();
                const auto union_size = potential_new_shared_dictionary->size();
                current_jaccard_index = calc_jaccard_index(union_size, total_size - union_size);
                current_compare_type = "ExistingShared @ " + std::to_string(shared_dictionary_index);
                if (should_merge(current_jaccard_index, current_dictionary->size(), union_size,
                                 segments_to_merge_at[shared_dictionary_index])) {
                  potential_new_shared_dictionary_index = shared_dictionary_index;
                  break;
                } else {
                  potential_new_shared_dictionary->clear();
                }
              }
            }
          }

          // Try to merge with neighboring segments if we didn't find a good existing shared dictionary
          auto merged_with_previous = false;
          if (potential_new_shared_dictionary_index < 0 && previous_segment_info_opt) {
            // If the merge with the last shared dictionary was not successful, try to merge the neighboring segments.
            const auto previous_dictionary = previous_segment_info_opt->segment->dictionary();
            std::set_union(current_dictionary->cbegin(), current_dictionary->cend(), previous_dictionary->cbegin(),
                           previous_dictionary->cend(), std::back_inserter(*potential_new_shared_dictionary));
            const auto total_size = current_dictionary->size() + previous_dictionary->size();
            const auto union_size = potential_new_shared_dictionary->size();
            current_jaccard_index = calc_jaccard_index(union_size, total_size - union_size);
            current_compare_type = "NeighboringSegments";
            if (should_merge(current_jaccard_index, current_dictionary->size(), union_size,
                             std::vector<SegmentChunkColumn<ColumnDataType>>{*previous_segment_info_opt})) {
              merged_with_previous = true;
            } else {
              potential_new_shared_dictionary->clear();
            }
          }

          potential_new_shared_dictionary->shrink_to_fit();

          const auto current_segment_info =
              SegmentChunkColumn<ColumnDataType>{current_dictionary_segment, chunk, column_id, column_name};
          if (current_jaccard_index >= jaccard_index_threshold) {
            // The jaccard index matches the threshold, so we add the segment to the collection

            if (potential_new_shared_dictionary_index < 0) {
              shared_dictionaries.emplace_back(potential_new_shared_dictionary);
              segments_to_merge_at.emplace_back(std::vector<SegmentChunkColumn<ColumnDataType>>{current_segment_info});
              if (merged_with_previous) {
                segments_to_merge_at.back().push_back(*previous_segment_info_opt);
              }
              last_merged_index = shared_dictionaries.size() - 1;

            } else {
              shared_dictionaries[potential_new_shared_dictionary_index] = potential_new_shared_dictionary;
              segments_to_merge_at[potential_new_shared_dictionary_index].push_back(current_segment_info);
              if (merged_with_previous) {
                segments_to_merge_at[potential_new_shared_dictionary_index].push_back(*previous_segment_info_opt);
              }
              last_merged_index = potential_new_shared_dictionary_index;
            }
          }

          if (csv_output_stream_opt) {
            log_jaccard_index(current_jaccard_index, table_name, column_name, current_compare_type, column_data_type,
                              chunk_id, current_dictionary_segment, *csv_output_stream_opt);
          }

          previous_segment_info_opt = current_segment_info;
        }

        // Merge the collected shared dictioaries with the segments
        Assert(shared_dictionaries.size() == segments_to_merge_at.size(),
               "The two vectors used for the merging must have the same size.");
        const auto merge_size = shared_dictionaries.size();
        for (auto merge_index = 0u; merge_index < merge_size; ++merge_index) {
          const auto shared_dictionary = shared_dictionaries[merge_index];
          const auto segments_to_merge = segments_to_merge_at[merge_index];

          total_merged_dictionaries += segments_to_merge.size();
          total_new_shared_dictionaries++;

          const auto new_segment_memory_usage = apply_shared_dictionary_to_segments<ColumnDataType>(
              segments_to_merge, shared_dictionary, table_name, allocator);
          memory_usage_difference.attribute_vector_memory_usage.previous +=
              new_segment_memory_usage.attribute_vector_memory_usage.previous;
          memory_usage_difference.dictionary_memory_usage.previous +=
              new_segment_memory_usage.dictionary_memory_usage.previous;
          memory_usage_difference.attribute_vector_memory_usage.current +=
              new_segment_memory_usage.attribute_vector_memory_usage.current;
          memory_usage_difference.dictionary_memory_usage.current +=
              new_segment_memory_usage.dictionary_memory_usage.current;
        }
      });
    }
  }

  std::cout << "Merged " << total_merged_dictionaries << " dictionaries to " << total_new_shared_dictionaries
            << " shared dictionaries.\n";
  std::cout << "The estimated memory change is:\n"
            << "- total: "
            << static_cast<long>((memory_usage_difference.attribute_vector_memory_usage.current +
                                  memory_usage_difference.dictionary_memory_usage.current) -
                                 (memory_usage_difference.attribute_vector_memory_usage.previous +
                                  memory_usage_difference.dictionary_memory_usage.previous))
            << " bytes / "
            << ((memory_usage_difference.attribute_vector_memory_usage.current +
                 memory_usage_difference.dictionary_memory_usage.current) *
                100.0 /
                (memory_usage_difference.attribute_vector_memory_usage.previous +
                 memory_usage_difference.dictionary_memory_usage.previous))
            << "%.\n"
            << "- attribute vectors: "
            << static_cast<long>(memory_usage_difference.attribute_vector_memory_usage.current -
                                 memory_usage_difference.attribute_vector_memory_usage.previous)
            << " bytes / "
            << (memory_usage_difference.attribute_vector_memory_usage.current * 100.0 /
                memory_usage_difference.attribute_vector_memory_usage.previous)
            << "%\n"
            << "- dictionaries: "
            << static_cast<long>(memory_usage_difference.dictionary_memory_usage.current -
                                 memory_usage_difference.dictionary_memory_usage.previous)
            << " bytes / "
            << (memory_usage_difference.dictionary_memory_usage.current * 100.0 /
                memory_usage_difference.dictionary_memory_usage.previous)
            << "%\n";
}

}  // namespace opossum
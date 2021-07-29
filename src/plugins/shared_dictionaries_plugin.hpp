#pragma once

#include "hyrise.hpp"
#include "storage/dictionary_segment.hpp"
#include "storage/table.hpp"
#include "types.hpp"
#include "utils/abstract_plugin.hpp"
#include "utils/singleton.hpp"

namespace opossum {

// Contains the dictionary encoded segment with its chunk and column information
template <typename T>
struct SegmentToMergeInfo {
  std::shared_ptr<DictionarySegment<T>> segment;
  std::shared_ptr<Chunk> chunk;
  ColumnID column_id;
  std::string column_name;
};

/*
 * Per default every dictionary encoded segment in Hyrise has its own dictionary.
 * This plugin compares the dictionaries within a column for their similarity
 * using the jaccard-index. If the jaccard-index is equal or higher than the
 * specified threshold and additionally if the merging does not increase the width
 * of the attribute vector, this plugin creates a shared dictionary. Then, the plugin
 * replaces the dictionary segment with a new dictionary segment that has a shared
 * dictionary. The intention of this plugin is to save memory while trying to not
 * decrease the performance of Hyrise.
 */
class SharedDictionariesPlugin : public AbstractPlugin {
 public:
  explicit SharedDictionariesPlugin(double init_jaccard_index_threshold = 0.1)
      : storage_manager(Hyrise::get().storage_manager), jaccard_index_threshold(init_jaccard_index_threshold) {}

  std::string description() const final;

  void start() final;

  void stop() final;

  StorageManager& storage_manager;

  // Threshold for the similarity metric between dictionaries
  double jaccard_index_threshold;

 private:
  const char* _env_variable_name = "JACCARD_INDEX_THRESHOLD";
  uint64_t _total_bytes_saved = 0ul;
  uint64_t _total_previous_bytes = 0ul;
  uint64_t _modified_previous_bytes = 0u;
  uint32_t _num_merged_dictionaries = 0u;
  uint32_t _num_shared_dictionaries = 0u;

  void _process_for_every_column();

  template <typename T>
  void _process_column(const std::shared_ptr<Table> table, const std::string& table_name, const ColumnID column_id,
                       const std::string& column_name);

  template <typename T>
  std::pair<int32_t, std::shared_ptr<pmr_vector<T>>> _compare_with_existing_shared_dictionaries(
      const std::shared_ptr<const pmr_vector<T>> current_dictionary,
      const std::vector<std::shared_ptr<pmr_vector<T>>>& shared_dictionaries,
      const std::vector<std::vector<SegmentToMergeInfo<T>>>& segments_to_merge_at,
      const PolymorphicAllocator<T>& allocator);

  template <typename T>
  std::shared_ptr<pmr_vector<T>> _compare_with_previous_dictionary(
      const std::shared_ptr<const pmr_vector<T>> current_dictionary, const SegmentToMergeInfo<T>& previous_segment_info,
      const PolymorphicAllocator<T>& allocator);

  template <typename T>
  void _apply_shared_dictionaries(const std::vector<std::shared_ptr<pmr_vector<T>>>& shared_dictionaries,
                                  const std::vector<std::vector<SegmentToMergeInfo<T>>>& segments_to_merge_at,
                                  const std::string& table_name, const PolymorphicAllocator<T>& allocator);

  template <typename T>
  std::shared_ptr<const BaseCompressedVector> _create_new_attribute_vector(
      const std::shared_ptr<DictionarySegment<T>> segment, const std::shared_ptr<const pmr_vector<T>> shared_dictionary,
      const PolymorphicAllocator<T>& allocator);

  template <typename T>
  size_t _calc_dictionary_memory_usage(const std::shared_ptr<const pmr_vector<T>> dictionary);

  double _calc_jaccard_index(size_t union_size, size_t intersection_size);

  template <typename T>
  bool _should_merge(const double jaccard_index, const size_t current_dictionary_size,
                     const size_t shared_dictionary_size, const std::vector<SegmentToMergeInfo<T>>& shared_segments);

  void _print_processing_result();
};

}  // namespace opossum

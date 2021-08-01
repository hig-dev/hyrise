#pragma once

#include <gtest/gtest_prod.h>
#include "hyrise.hpp"
#include "storage/dictionary_segment.hpp"
#include "storage/table.hpp"
#include "types.hpp"
#include "utils/abstract_plugin.hpp"
#include "utils/singleton.hpp"

namespace opossum {

template <typename T>
using SegmentChunkPair = std::pair<std::shared_ptr<DictionarySegment<T>>, std::shared_ptr<Chunk>>;

struct SharedDictionariesStats {
  uint64_t total_bytes_saved = 0ul;
  uint64_t total_previous_bytes = 0ul;
  uint64_t modified_previous_bytes = 0u;
  uint32_t num_merged_dictionaries = 0u;
  uint32_t num_shared_dictionaries = 0u;
  uint32_t num_existing_merged_dictionaries = 0u;
  uint32_t num_existing_shared_dictionaries = 0u;
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
      : storage_manager(Hyrise::get().storage_manager), jaccard_index_threshold(init_jaccard_index_threshold) {
    stats = std::make_shared<SharedDictionariesStats>();
  }

  std::string description() const final;

  void start() final;

  void stop() final;

  StorageManager& storage_manager;

  // Threshold for the similarity metric between dictionaries
  double jaccard_index_threshold;

  std::shared_ptr<SharedDictionariesStats> stats;

 private:
  void _process_for_every_column();

  void _print_processing_result();
};

// Merge plan for a shared dictionary containing the segments to merge and additional information
template <typename T>
struct MergePlan {
 public:
  std::shared_ptr<const pmr_vector<T>> shared_dictionary;
  std::vector<SegmentChunkPair<T>> segment_chunk_pairs_to_merge;
  bool contains_non_merged_segment;
  bool contains_already_merged_segment;
  uint64_t non_merged_total_bytes = 0ul;
  uint64_t non_merged_dictionary_bytes = 0ul;

  MergePlan(const std::shared_ptr<const pmr_vector<T>>& init_shared_dictionary)
      : shared_dictionary(init_shared_dictionary) {
    segment_chunk_pairs_to_merge = {};
    contains_non_merged_segment = false;
    contains_already_merged_segment = false;
  }
};

// Processes one column for the SharedDictionariesPlugin
template <typename T>
class SharedDictionariesColumnProcessor {
  friend class SharedDictionariesPluginTest;

 public:
  const std::shared_ptr<Table> table;
  const std::string& table_name;
  const ColumnID column_id;
  const std::string& column_name;
  const double jaccard_index_threshold;
  std::shared_ptr<SharedDictionariesStats> stats;
  SharedDictionariesColumnProcessor(const std::shared_ptr<Table>& init_table, const std::string& init_table_name,
                                    const ColumnID init_column_id, const std::string& init_column_name,
                                    const double init_jaccard_index_threshold,
                                    const std::shared_ptr<SharedDictionariesStats>& init_stats);

  void process();

 private:
  void _initialize_merge_plans(std::vector<std::shared_ptr<MergePlan<T>>>& merge_plans);

  std::pair<int32_t, std::shared_ptr<const pmr_vector<T>>> _compare_with_existing_merge_plans(
      const std::shared_ptr<const pmr_vector<T>> current_dictionary,
      const std::vector<std::shared_ptr<MergePlan<T>>>& merge_plans, const PolymorphicAllocator<T>& allocator);

  std::shared_ptr<const pmr_vector<T>> _compare_with_previous_dictionary(
      const std::shared_ptr<const pmr_vector<T>> current_dictionary,
      const SegmentChunkPair<T> previous_segment_chunk_pair, const PolymorphicAllocator<T>& allocator);

  void _process_merge_plans(const std::vector<std::shared_ptr<MergePlan<T>>>& merge_plans,
                            const PolymorphicAllocator<T>& allocator);

  std::shared_ptr<const BaseCompressedVector> _create_new_attribute_vector(
      const std::shared_ptr<DictionarySegment<T>> segment, const std::shared_ptr<const pmr_vector<T>> shared_dictionary,
      const PolymorphicAllocator<T>& allocator);

  static size_t _calc_dictionary_memory_usage(const std::shared_ptr<const pmr_vector<T>> dictionary);

  bool _should_merge(const double jaccard_index, const size_t current_dictionary_size,
                     const size_t shared_dictionary_size,
                     const std::vector<SegmentChunkPair<T>>& shared_segment_chunk_pairs);

  static void _add_segment_chunk_pair(MergePlan<T>& merge_plan, const SegmentChunkPair<T>& segment_chunk_pair,
                                      bool is_already_merged);

  static double _calc_jaccard_index(size_t union_size, size_t intersection_size);

  static bool _increases_attribute_vector_width(const size_t shared_dictionary_size,
                                                const size_t current_dictionary_size);
};

EXPLICITLY_DECLARE_DATA_TYPES(SharedDictionariesColumnProcessor);

}  // namespace opossum

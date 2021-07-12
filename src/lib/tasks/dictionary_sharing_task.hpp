#pragma once

#include <iostream>
#include <string>
#include <vector>

namespace opossum {

template <typename T>
struct SegmentChunkColumn {
  std::shared_ptr<DictionarySegment<T>> segment;
  std::shared_ptr<Chunk> chunk;
  ColumnID column_id;
  std::string column_name;
};

class DictionarySharingTask {
 public:
  DictionarySharingTask(double init_jaccard_index_threshold = 0.5,
                        bool init_check_for_attribute_vector_size_increase = true);
  void do_segment_sharing(std::optional<std::ofstream> csv_output_stream_opt);

  double jaccard_index_threshold;
  bool check_for_attribute_vector_size_increase;

 private:
  template <typename T>
  bool should_merge(const double jaccard_index, const size_t current_dictionary_size,
                    const size_t shared_dictionary_size, std::vector<SegmentChunkColumn<T>> shared_segments);
};
}  // namespace opossum

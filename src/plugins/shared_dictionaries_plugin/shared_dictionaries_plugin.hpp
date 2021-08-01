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

/*
 * The intention of this plugin is to save memory by using "dictionary sharing"
 * while trying to not decrease the performance of Hyrise.
 *
 * Per default every dictionary encoded segment in Hyrise has its own dictionary.
 * This plugin compares the dictionaries within a column for their similarity
 * using the jaccard-index. If the jaccard-index is equal or higher than the
 * specified threshold and additionally if the merging does not increase the width
 * of the attribute vector, this plugin creates a shared dictionary. Then, the plugin
 * replaces the dictionary segment with a new dictionary segment that has a shared
 * dictionary.
 *
 * With the start of the plugin, the dictionary sharing compressor is automatically
 * started for every table in the database.
 */
class SharedDictionariesPlugin : public AbstractPlugin {
 public:
  inline static const std::string LOG_NAME = "SharedDictionariesPlugin";

  struct SharedDictionariesStats {
    uint64_t total_bytes_saved = 0ul;
    uint64_t total_previous_bytes = 0ul;
    uint64_t modified_previous_bytes = 0u;
    uint32_t num_merged_dictionaries = 0u;
    uint32_t num_shared_dictionaries = 0u;
    uint32_t num_existing_merged_dictionaries = 0u;
    uint32_t num_existing_shared_dictionaries = 0u;
  };

  explicit SharedDictionariesPlugin(double init_jaccard_index_threshold = 0.1)
      : storage_manager(Hyrise::get().storage_manager),
        log_manager(Hyrise::get().log_manager),
        jaccard_index_threshold(init_jaccard_index_threshold) {
    stats = std::make_shared<SharedDictionariesStats>();
  }

  std::string description() const final;

  void start() final;

  void stop() final;

  StorageManager& storage_manager;
  LogManager& log_manager;

  // Threshold for the similarity metric between dictionaries
  double jaccard_index_threshold;

  std::shared_ptr<SharedDictionariesStats> stats;

 private:
  class MemoryBudgetSetting : public AbstractSetting {
   public:
    MemoryBudgetSetting() : AbstractSetting("Plugin::SharedDictionaries::MemoryBudget") {}
    //    const std::string& description() const final {
    //      static const auto description = std::string{"The memory budget (MB) to target for the CompressionPlugin."};
    //      return description;
    //    }
    //    const std::string& display_name() const final { return _display_name; }
    //    const std::string& get() final { return _value; }
    //    void set(const std::string& value) final { _value = value; }
    //
    //    std::string _value = "10000";
    //    std::string _display_name = "Memory Budget (MB)";
  };

  void _process_for_every_column();

  void _log_plugin_configuration();
  void _log_processing_result();
};

}  // namespace opossum

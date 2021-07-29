#include <fstream>
#include <iostream>
#include <optional>

#include "../benchmarklib/tpcds/tpcds_table_generator.hpp"
#include "../lib/import_export/binary/binary_parser.hpp"
#include "../lib/import_export/binary/binary_writer.hpp"
#include "benchmark_config.hpp"
#include "file_based_table_generator.hpp"
#include "hyrise.hpp"
#include "synthetic_table_generator.hpp"
#include "tasks/dictionary_sharing_task.hpp"

using namespace opossum;  // NOLINT

int main() {
  std::cout << "Playground: Jaccard-Index" << std::endl;

  // Generate benchmark data
  const auto table_path = std::string{"./imdb_data"};

  const auto table_generator = std::make_unique<FileBasedTableGenerator>(
      std::make_shared<BenchmarkConfig>(BenchmarkConfig::get_default_config()), table_path);
  table_generator->generate_and_store();

  auto output_file_stream = std::ofstream("./jaccard_index_log.csv", std::ofstream::out | std::ofstream::trunc);

  //auto dictionary_sharing_task = DictionarySharingTask{0.01, true};
  //dictionary_sharing_task.do_segment_sharing(std::make_optional<std::ofstream>(std::move(output_file_stream)));

  auto& pm = Hyrise::get().plugin_manager;
  pm.load_plugin("./build/Release/lib/libhyriseSharedDictionariesPlugin.so");
  pm.unload_plugin("hyriseSharedDictionariesPlugin");

  output_file_stream.close();
  return 0;
}

#include <fstream>
#include <iostream>
#include <optional>

#include "../benchmarklib/tpcds/tpcds_table_generator.hpp"
#include "../lib/import_export/binary/binary_writer.hpp"
#include "benchmark_config.hpp"
#include "file_based_table_generator.hpp"
#include "hyrise.hpp"

using namespace opossum;  // NOLINT

int main() {
  std::cout << "Playground: Jaccard-Index" << std::endl;

  // Generate benchmark data
  const auto table_path = std::string{"./imdb_data"};

  const auto table_generator = std::make_unique<FileBasedTableGenerator>(
      std::make_shared<BenchmarkConfig>(BenchmarkConfig::get_default_config()), table_path);
  table_generator->generate_and_store();

  auto& pm = Hyrise::get().plugin_manager;
  pm.load_plugin("lib/libhyriseSharedDictionariesPlugin.so");
  pm.unload_plugin("hyriseSharedDictionariesPlugin");

  output_file_stream.close();
  return 0;
}

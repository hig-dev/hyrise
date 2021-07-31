#include <memory>
#include <string>
#include <vector>

#include "base_test.hpp"
#include "hyrise.hpp"
#include "lib/utils/plugin_test_utils.hpp"

#include "../../plugins/shared_dictionaries_plugin.hpp"
#include "storage/encoding_type.hpp"
#include "storage/storage_manager.hpp"
#include "storage/table.hpp"
#include "utils/load_table.hpp"
#include "utils/plugin_manager.hpp"

namespace opossum {

class SharedDictionariesTest : public BaseTest {
 public:
  void SetUp() override {
    auto& sm = Hyrise::get().storage_manager;
    if (!sm.has_table(_table_names[0])) {
      _add_test_tables();
    }
  }

  void TearDown() override { Hyrise::reset(); }

 protected:
  const std::vector<std::string> _table_names{"part", "partsupp", "customer", "orders"};
  const std::string _tables_path{"resources/test_data/tbl/tpch/sf-0.001/"};
  const std::string _table_extension{".tbl"};
  const size_t _chunk_size = 32;

  void _add_test_tables() {
    auto& sm = Hyrise::get().storage_manager;
    for (const auto& table_name : _table_names) {
      const auto table = load_table(_get_table_path(table_name), _chunk_size);
      _encode_table(table);
      sm.add_table(table_name, table);
    }
  }

  void _encode_table(std::shared_ptr<Table> table) {
    auto chunk_encoding_spec = ChunkEncodingSpec{};
    for (const auto& column_definition : table->column_definitions()) {
      if (encoding_supports_data_type(EncodingType::Dictionary, column_definition.data_type)) {
        chunk_encoding_spec.emplace_back(EncodingType::Dictionary);
      } else {
        chunk_encoding_spec.emplace_back(EncodingType::Unencoded);
      }
    }

    ChunkEncoder::encode_all_chunks(table, chunk_encoding_spec);
  }

  void _validate() {
    auto& sm = Hyrise::get().storage_manager;
    for (const auto& table_name : _table_names) {
      EXPECT_TABLE_EQ_ORDERED(sm.get_table(table_name), load_table(_get_table_path(table_name), _chunk_size));
    }
  }

  std::string _get_table_path(const std::string& table_name) { return _tables_path + table_name + _table_extension; }
};

TEST_F(SharedDictionariesTest, LoadUnloadPlugin) {
  auto& pm = Hyrise::get().plugin_manager;
  pm.load_plugin(build_dylib_path("libhyriseSharedDictionariesPlugin"));
  _validate();
  pm.unload_plugin("hyriseSharedDictionariesPlugin");
}

TEST_F(SharedDictionariesTest, ReloadPlugin) {
  auto& pm = Hyrise::get().plugin_manager;

  pm.load_plugin(build_dylib_path("libhyriseSharedDictionariesPlugin"));
  _validate();
  pm.unload_plugin("hyriseSharedDictionariesPlugin");

  pm.load_plugin(build_dylib_path("libhyriseSharedDictionariesPlugin"));
  _validate();
  pm.unload_plugin("hyriseSharedDictionariesPlugin");
}

}  // namespace opossum

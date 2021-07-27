#include <memory>
#include <string>
#include <vector>

#include "base_test.hpp"
#include "hyrise.hpp"
#include "lib/utils/plugin_test_utils.hpp"

#include "../../plugins/shared_dictionaries_plugin.hpp"
#include "import_export/binary/binary_parser.hpp"
#include "storage/storage_manager.hpp"
#include "storage/table.hpp"
#include "utils/plugin_manager.hpp"

namespace opossum {

class SharedDictionariesTest : public BaseTest {
 public:
  void SetUp() override {
    auto& sm = Hyrise::get().storage_manager;
    if (!sm.has_table(_table_name)) {
      sm.add_table(_table_name, BinaryParser::parse(_table_path));
    }
  }

  void TearDown() override { Hyrise::reset(); }

 protected:
  const std::string _table_name{"sharedDictionariesTestTable"};
  const std::string _table_path{"resources/test_data/bin/imdb_company_name.bin"};

  void _validate() {
    auto& sm = Hyrise::get().storage_manager;
    EXPECT_TABLE_EQ_ORDERED(sm.get_table(_table_name), BinaryParser::parse(_table_path));
  }
};

TEST_F(SharedDictionariesTest, LoadUnloadPlugin) {
  auto& pm = Hyrise::get().plugin_manager;
  pm.load_plugin(build_dylib_path("libhyriseSharedDictionariesPlugin"));
  _validate();
  pm.unload_plugin("hyriseSharedDictionariesPlugin");
}

}  // namespace opossum

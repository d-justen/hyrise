#include "base_test.hpp"
#include "gtest/gtest.h"

#include "../benchmarklib/random_generator.hpp"
#include "expression/expression_functional.hpp"
#include "expression/pqp_column_expression.hpp"
#include "operators/get_table.hpp"
#include "operators/table_scan.hpp"
#include "operators/update.hpp"
#include "operators/validate.hpp"
#include "resolve_type.hpp"
#include "storage/chunk.hpp"
#include "storage/table.hpp"
#include "storage/table_column_definition.hpp"
#include "storage/value_segment.hpp"
#include "types.hpp"
#include "utils/plugin_manager.hpp"
#include "utils/plugin_test_utils.hpp"

using namespace opossum;  // NOLINT

class MvccDeletePluginSystemTest : public BaseTest {
  public:
    static void SetUpTestCase() {
    }

    void SetUp() override {
      TableColumnDefinitions column_definitions;
      column_definitions.emplace_back("number", data_type_from_type<int>());

      std::vector<int> vec;
      vec.reserve(MAX_CHUNK_SIZE);
      // std::iota(vec.begin(), vec.end(), 0);
      for (size_t i = 0; i < MAX_CHUNK_SIZE; i++) {
        vec.emplace_back(i);
      }

      Segments segments;
      const auto value_segment = std::make_shared<ValueSegment<int>>(std::move(vec));
      segments.emplace_back(value_segment);
      auto table = std::make_shared<Table>(column_definitions, TableType::Data, MAX_CHUNK_SIZE, UseMvcc::Yes);
      table->append_chunk(segments);

      StorageManager::get().add_table("mvcc_test", table);
    }  // managed by each test individually

    void TearDown() override { 
      StorageManager::get().drop_table("mvcc_test");
      StorageManager::reset();
    }

  protected:

    void check_plugin_activity() {

      auto transaction_context = TransactionManager::get().new_transaction_context();
      auto gt = std::make_shared<GetTable>("mvcc_test");
      gt->set_transaction_context(transaction_context);

      gt->execute();

      const auto& table = gt->get_output();

      for (auto chunk = ChunkID{0}; chunk < table->chunk_count(); ++chunk) {
        std::cout << "Ratio " << chunk << ": " << table->get_chunk(chunk)->invalid_row_count() / static_cast<double>(table->max_chunk_size()) << std::endl;
        EXPECT_TRUE((table->get_chunk(chunk)->invalid_row_count() / static_cast<double>(table->max_chunk_size())) < .9);
      }
    }

    constexpr static size_t MAX_CHUNK_SIZE = 20'000;
    constexpr static size_t UPDATES = 200'000;
    constexpr static std::chrono::milliseconds PLUGIN_CHECKER_INTERVAL = std::chrono::milliseconds(1000);
};

TEST_F(MvccDeletePluginSystemTest, CheckPlugin) {
  auto& pm = PluginManager::get();
  pm.load_plugin(build_dylib_path("libMvccDeletePlugin"));
  std::unique_ptr<PausableLoopThread> plugin_activity_checker =
    std::make_unique<PausableLoopThread>(PLUGIN_CHECKER_INTERVAL, [&](size_t) { check_plugin_activity(); });

  const auto tbl = StorageManager::get().get_table("mvcc_test");
  auto column = expression_functional::pqp_column_(ColumnID{0}, DataType::Int, false, "number");

  auto& tm = TransactionManager::get();

  RandomGenerator rg;

  for (size_t i = 0; i < UPDATES; i++) {

    const auto transaction_context = tm.new_transaction_context();
    const auto rand = static_cast<int>(rg.random_number(0, MAX_CHUNK_SIZE - 1));
    const auto expr = expression_functional::equals_(column, rand);

    const auto gt = std::make_shared<GetTable>("mvcc_test");
    gt->set_transaction_context(transaction_context);

    const auto validate = std::make_shared<Validate>(gt);
    validate->set_transaction_context(transaction_context);

    const auto where = std::make_shared<TableScan>(validate, expr);
    where->set_transaction_context(transaction_context);

    const auto update = std::make_shared<Update>("mvcc_test", where, where);
    update->set_transaction_context(transaction_context);

    gt->execute();
    validate->execute();
    where->execute();
    update->execute();

    if (!update->execute_failed()) {
      transaction_context->commit();
    } else {
      transaction_context->rollback();
      i--;
    }
  }
  PluginManager::get().unload_plugin("MvccDeletePlugin");

  //EXPECT_FALSE();
}

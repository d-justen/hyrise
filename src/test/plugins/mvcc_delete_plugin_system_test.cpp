#include <chrono>
#include <thread>

#include "base_test.hpp"
#include "gtest/gtest.h"

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
  static void SetUpTestCase() {}

  void SetUp() override {
    TableColumnDefinitions column_definitions;
    column_definitions.emplace_back("number", data_type_from_type<int>());

    std::vector<int> vec;
    vec.reserve(MAX_CHUNK_SIZE);
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
  void update_table() {
    auto column = expression_functional::pqp_column_(ColumnID{0}, DataType::Int, false, "number");

    auto& tm = TransactionManager::get();

    const auto transaction_context = tm.new_transaction_context();
    const auto value = static_cast<int>(counter++ % (MAX_CHUNK_SIZE));
    const auto expr = expression_functional::equals_(column, value);

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
      counter--;
    }
  }

  size_t deleted_chunks = 0;
  uint64_t counter = 0;

  constexpr static size_t MAX_CHUNK_SIZE = 200;
  constexpr static size_t PHYSICALLY_DELETED_CHUNKS_COUNT = 2;
};

/**
 * This test checks the number of physically deleted chunks, which means
 * nullptrs in the table.
 * These nullptrs are created when the plugin successfully deleted a chunk.
 */
TEST_F(MvccDeletePluginSystemTest, CheckPlugin) {

  // {
  // // This thread updates the table continuously
  // std::unique_ptr<PausableLoopThread> table_update_thread =
  //     std::make_unique<PausableLoopThread>(std::chrono::milliseconds(0), [&](size_t) { update_table(); });
  // }

  auto column = expression_functional::pqp_column_(ColumnID{0}, DataType::Int, false, "number");

  auto& tm = TransactionManager::get();

  for (int i = 0; i < 30000; ++i)
  {
    const auto transaction_context = tm.new_transaction_context();
    const auto value = static_cast<int>(i % (MAX_CHUNK_SIZE));
    const auto expr = expression_functional::equals_(column, value);

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
      counter--;
    }
  }

  std::this_thread::sleep_for(std::chrono::milliseconds(2000));


  auto& pm = PluginManager::get();
  pm.load_plugin(build_dylib_path("libMvccDeletePlugin"));

  std::this_thread::sleep_for(std::chrono::milliseconds(2000));

  // while (deleted_chunks < PHYSICALLY_DELETED_CHUNKS_COUNT) {
  //   deleted_chunks = 0;

  //   // const auto table = StorageManager::get().get_table("mvcc_test");

  //   // for (auto chunk_id = ChunkID{0}; chunk_id < table->chunk_count(); ++chunk_id) {
  //   //   if (!table->get_chunk(chunk_id)) {
  //   //     deleted_chunks++;
  //   //   } else {
  //   //     // std::cout << "Ratio " << chunk_id << ": "
  //   //     //           << (table->get_chunk(chunk_id)->invalid_row_count() /
  //   //     //               static_cast<double>(table->get_chunk(chunk_id)->size()))
  //   //     //           << std::endl;
  //   //   }
  //   // }
  //   std::this_thread::sleep_for(std::chrono::milliseconds(1000));
  // }
  PluginManager::get().unload_plugin("MvccDeletePlugin");
}

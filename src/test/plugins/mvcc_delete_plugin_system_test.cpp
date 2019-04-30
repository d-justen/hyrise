#include <chrono>
#include <numeric>
#include <thread>

#include "base_test.hpp"
#include "gtest/gtest.h"

#include "../../plugins/mvcc_delete_plugin.hpp"
#include "expression/expression_functional.hpp"
#include "expression/pqp_column_expression.hpp"
#include "operators/aggregate_hash.hpp"
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
#include "utils/pausable_loop_thread.hpp"
#include "utils/plugin_manager.hpp"
#include "utils/plugin_test_utils.hpp"

using namespace opossum;  // NOLINT

class MvccDeletePluginSystemTest : public BaseTest {
 public:
  // Create table with three (INITIAL_CHUNK_COUNT) chunks of CHUNK_SIZE rows each and increasing values.
  void SetUp() override {
    TableColumnDefinitions column_definitions;
    column_definitions.emplace_back("number", DataType::Int);

    _table = std::make_shared<Table>(column_definitions, TableType::Data, CHUNK_SIZE, UseMvcc::Yes);

    auto begin_value = 0;
    for (auto chunk_id = size_t{0}; chunk_id < INITIAL_CHUNK_COUNT; ++chunk_id) {
      std::vector<int> values(CHUNK_SIZE);
      std::iota(values.begin(), values.end(), begin_value);

      const auto value_segment = std::make_shared<ValueSegment<int>>(std::move(values));
      Segments segments;
      segments.emplace_back(value_segment);
      _table->append_chunk(segments);

      begin_value += CHUNK_SIZE;
    }

    StorageManager::get().add_table("mvcc_test", _table);
  }

 protected:
  // Update a single row starting at 220 (INITIAL_UPDATE_OFFSET) in the table to create invalidated rows. This should
  // leave the first chunk untouched. Also, stop just before finishing the third chunk so that the third chunk is
  // "fresh" and not cleaned up.
  void update_next_row() {
    if (_counter == INITIAL_CHUNK_COUNT * CHUNK_SIZE - 2) return;

    auto column = expression_functional::pqp_column_(ColumnID{0}, DataType::Int, false, "number");

    const auto transaction_context = TransactionManager::get().new_transaction_context();

    const auto gt = std::make_shared<GetTable>("mvcc_test");
    gt->set_transaction_context(transaction_context);

    const auto validate = std::make_shared<Validate>(gt);
    validate->set_transaction_context(transaction_context);

    _counter++;
    const auto value = static_cast<int>(_counter);
    const auto expr = expression_functional::equals_(column, value);
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
      // Collided with the plugin rewriting a chunk
      transaction_context->rollback();
      _counter--;
    }
  }

  void validate_table() {
    const auto transaction_context = TransactionManager::get().new_transaction_context();

    const auto gt = std::make_shared<GetTable>("mvcc_test");
    gt->set_transaction_context(transaction_context);

    const auto validate = std::make_shared<Validate>(gt);
    validate->set_transaction_context(transaction_context);

    const auto aggregate_definition = std::vector<AggregateColumnDefinition>{{ColumnID{0}, AggregateFunction::Sum}};
    const auto group_by = std::vector<ColumnID>{};
    const auto aggregate = std::make_shared<AggregateHash>(validate, aggregate_definition, group_by);

    gt->execute();
    validate->execute();
    aggregate->execute();

    EXPECT_EQ(aggregate->get_output()->get_value<int64_t>(ColumnID{0}, 0), 179'700);
  }

  constexpr static size_t INITIAL_CHUNK_COUNT = 3;
  constexpr static size_t CHUNK_SIZE = 200;
  constexpr static size_t INITIAL_UPDATE_OFFSET = 220;
  constexpr static double DELETE_THRESHOLD = MvccDeletePlugin::DELETE_THRESHOLD_PERCENTAGE_INVALIDATED_ROWS;

  std::atomic<size_t> _counter = INITIAL_UPDATE_OFFSET;
  std::shared_ptr<Table> _table;
};

/**
 * TODO
 */
TEST_F(MvccDeletePluginSystemTest, CheckPlugin) {
  auto& pm = PluginManager::get();
  pm.load_plugin(build_dylib_path("libMvccDeletePlugin"));

  validate_table();

  // This context is older than all invalidations, so it might access the old rows. While it exists, no physical
  // cleanup should be performed.
  auto some_other_transaction_context = TransactionManager::get().new_transaction_context();

  // This thread calls update_next_row() continuously. As a PausableLoopThread, it gets terminated together with the
  // test.
  auto table_update_thread =
      std::make_unique<PausableLoopThread>(std::chrono::milliseconds(10), [&](size_t) { update_next_row(); });

  while (_counter < CHUNK_SIZE * 2) {
    // Wait until the second chunk has been updated
  }

  {
    auto attempts_remaining = 5;
    while (attempts_remaining--) {
      // The second chunk should have been logically deleted by now
      if (_table->get_chunk(ChunkID{1})->get_cleanup_commit_id()) break;

      // Not yet. Give the plugin some more time.
      std::this_thread::sleep_for(MvccDeletePlugin::IDLE_DELAY_LOGICAL_DELETE);
    }

    // Check that we have not given up
    EXPECT_GT(attempts_remaining, -1);
  }

  validate_table();

  // Now test the physical delete
  some_other_transaction_context = nullptr;

  {
    auto attempts_remaining = 5;
    while (attempts_remaining--) {
      // The second chunk should have been physically deleted by now
      if (_table->get_chunk(ChunkID{1}) == nullptr) break;

      // Not yet. Give the plugin some more time.
      std::this_thread::sleep_for(MvccDeletePlugin::IDLE_DELAY_PHYSICAL_DELETE);
    }

    // Check that we have not given up
    EXPECT_GT(attempts_remaining, -1);
  }

  validate_table();

  // The first chunk was never modified, so it should not have been cleaned up
  EXPECT_FALSE(_table->get_chunk(ChunkID{0})->get_cleanup_commit_id());

  // The third chunk was the last to be modified, so it should not have been cleaned up either
  EXPECT_FALSE(_table->get_chunk(ChunkID{2})->get_cleanup_commit_id());

  // Kill a couple of commit IDs so that the third chunk is eligible for clean-up, too
  for (auto transaction_idx = CommitID{0}; transaction_idx < MvccDeletePlugin::DELETE_THRESHOLD_LAST_COMMIT;
       ++transaction_idx) {
    TransactionManager::get().new_transaction_context()->commit();
  }

  {
    auto attempts_remaining = 5;
    while (attempts_remaining--) {
      // The third chunk should have been logically deleted by now
      if (_table->get_chunk(ChunkID{2}) == nullptr) break;

      // Not yet. Give the plugin some more time.
      std::this_thread::sleep_for(MvccDeletePlugin::IDLE_DELAY_LOGICAL_DELETE);
    }

    // Check that we have not given up
    EXPECT_GT(attempts_remaining, -1);
  }

  validate_table();

  PluginManager::get().unload_plugin("MvccDeletePlugin");
}

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
  /**
   * Create a table with three (INITIAL_CHUNK_COUNT) chunks of CHUNK_SIZE rows each.
   * The column number contains increasing integer values, starting from zero.
   */
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
      const auto mvcc_data = std::make_shared<MvccData>(segments.front()->size(), CommitID{0});
      _table->append_chunk(segments, mvcc_data);

      begin_value += CHUNK_SIZE;
    }

    StorageManager::get().add_table("mvcc_test", _table);
  }

 protected:
  /**
   * Updates a single row to make it invalid in its chunk. Data modification is not involved, so the row gets reinserted
   * at the end of the table.
   * - Updates start at position 220 (INITIAL_UPDATE_OFFSET), so the first chunk stays untouched.
   * - Updates stop just before the end of Chunk three, so that it is "fresh" and not cleaned up. // TODO unclear!
   */
  void update_next_row() {
    if (_counter == INITIAL_CHUNK_COUNT * CHUNK_SIZE - 2) return;  // -> if (_counter == 598)...

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

  /**
   * Checks the table configuration by summing up all integer values
   */
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

    // sum(0, 1, ... , 599) = 179'700
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
 * Tests the logical and physical delete operations of the MvccDeletePlugin in practise.
 */
TEST_F(MvccDeletePluginSystemTest, CheckPlugin) {
  // Load the MvccDeletePlugin
  auto& pm = PluginManager::get();
  pm.load_plugin(build_dylib_path("libMvccDeletePlugin"));

  // (1) Validate start conditions
  validate_table();

  // (2) Prepare clean-up of chunk two
  // (2.1) Create and run a thread which invalidates and reinserts rows of chunk two and three

  // This context is older than all invalidations, so it might access the old rows. While it exists, no physical
  // cleanup should be performed.
  auto some_other_transaction_context = TransactionManager::get().new_transaction_context();

  // This thread calls update_next_row() continuously. As a PausableLoopThread, it gets terminated together with the
  // test.
  auto table_update_thread =
      std::make_unique<PausableLoopThread>(std::chrono::milliseconds(10), [&](size_t) { update_next_row(); });

  // (2.2) Wait until the thread has finished invalidating rows in chunk two
  while (_counter < CHUNK_SIZE * 2) {  // -> if(_counter < 400)...
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
  }

  // (3) Wait for the MvccDeletePlugin to delete chunk two physically

  some_other_transaction_context = nullptr;
  {
    auto attempts_remaining = 10;
    while (attempts_remaining--) {
      // The second chunk should have been physically deleted by now
      if (_table->get_chunk(ChunkID{1}) == nullptr) break;

      // Not yet. Give the plugin some more time.
      std::this_thread::sleep_for(MvccDeletePlugin::IDLE_DELAY_PHYSICAL_DELETE);
    }

    // Check that we have not given up
    EXPECT_GT(attempts_remaining, -1);
  }

  // (4) Check after conditions
  {
    // Updates started from row 220 on. So chunk two contained 20 rows still valid before its logical deletion.
    // These rows must have been invalidated and reinserted to the table during the logical delete operation
    // by the MvccDeletePlugin.
    validate_table();

    // The first chunk was never modified, so it should not have been cleaned up
    const auto chunk0 = _table->get_chunk(ChunkID{0});
    EXPECT_TRUE(chunk0);
    EXPECT_FALSE(chunk0->get_cleanup_commit_id());

    // The third chunk was the last to be modified, so it should not have been cleaned up either. (compare criterion 2)
    const auto chunk2 = _table->get_chunk(ChunkID{2});
    EXPECT_TRUE(chunk2);
    EXPECT_FALSE(chunk2->get_cleanup_commit_id());
  }

  // (5) Prepare clean-up of chunk three
  {
    // Kill a couple of commit IDs so that the third chunk is eligible for clean-up too. (so criterion 2 is fulfilled)
    for (auto transaction_idx = CommitID{0}; transaction_idx < MvccDeletePlugin::DELETE_THRESHOLD_LAST_COMMIT;
         ++transaction_idx) {
      TransactionManager::get().new_transaction_context()->commit();
    }
  }

  // (6) Wait for the MvccDeletePlugin to delete chunk three physically
  {
    auto attempts_remaining = 10;
    while (attempts_remaining--) {
      // The third chunk should have been phyiscally deleted by now
      if (_table->get_chunk(ChunkID{2}) == nullptr) break;

      // Not yet. Give the plugin some more time.
      std::this_thread::sleep_for(MvccDeletePlugin::IDLE_DELAY_PHYSICAL_DELETE);
    }

    // Check that we have not given up
    EXPECT_GT(attempts_remaining, -1);
  }

  // (7) Check after conditions
  validate_table();

  // Unload the MvccDeletePlugin
  PluginManager::get().unload_plugin("MvccDeletePlugin");
}

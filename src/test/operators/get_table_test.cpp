#include <memory>

#include "base_test.hpp"
#include "gtest/gtest.h"

#include "concurrency/transaction_context.hpp"
#include "operators/get_table.hpp"
#include "storage/chunk.hpp"
#include "storage/storage_manager.hpp"
#include "storage/table.hpp"

namespace opossum {
// The fixture for testing class GetTable.
class OperatorsGetTableTest : public BaseTest {
 protected:
  void SetUp() override {
    _test_table = std::make_shared<Table>(TableColumnDefinitions{}, TableType::Data, 2);
    auto& manager = StorageManager::get();
    manager.add_table("tableWithValues", load_table("resources/test_data/tbl/int_float2.tbl", 1u));
  }

  std::shared_ptr<Table> _test_table;
};

TEST_F(OperatorsGetTableTest, GetOutput) {
  auto gt = std::make_shared<GetTable>("tableWithValues");
  gt->execute();

  EXPECT_TABLE_EQ_UNORDERED(gt->get_output(), load_table("resources/test_data/tbl/int_float2.tbl", 1u));
}

TEST_F(OperatorsGetTableTest, ThrowsUnknownTableName) {
  auto gt = std::make_shared<GetTable>("anUglyTestTable");

  EXPECT_THROW(gt->execute(), std::exception) << "Should throw unknown table name exception";
}

TEST_F(OperatorsGetTableTest, OperatorName) {
  auto gt = std::make_shared<opossum::GetTable>("tableWithValues");

  EXPECT_EQ(gt->name(), "GetTable");
}

TEST_F(OperatorsGetTableTest, ExcludedChunks) {
  auto gt = std::make_shared<opossum::GetTable>("tableWithValues");

  gt->set_excluded_chunk_ids({ChunkID(0), ChunkID(2)});
  gt->execute();

  auto original_table = StorageManager::get().get_table("tableWithValues");
  auto table = gt->get_output();
  EXPECT_EQ(table->chunk_count(), ChunkID(2));
  EXPECT_EQ(table->get_value<int>(ColumnID(0), 0u), original_table->get_value<int>(ColumnID(0), 1u));
  EXPECT_EQ(table->get_value<int>(ColumnID(0), 1u), original_table->get_value<int>(ColumnID(0), 3u));
}

TEST_F(OperatorsGetTableTest, ExcludeCleanedUpChunk) {
  auto gt = std::make_shared<opossum::GetTable>("tableWithValues");
  auto context = std::make_shared<TransactionContext>(1u, 3u);

  auto original_table = StorageManager::get().get_table("tableWithValues");
  auto chunk = original_table->get_chunk(ChunkID{0});

  chunk->set_cleanup_commit_id(CommitID{2u});

  gt->set_transaction_context(context);
  gt->execute();

  auto table = gt->get_output();
  EXPECT_EQ(original_table->chunk_count(), 4);
  EXPECT_EQ(table->chunk_count(), 3);
}
}  // namespace opossum

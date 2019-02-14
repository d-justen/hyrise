

#include "base_test.hpp"
#include "gtest/gtest.h"

#include "concurrency/transaction_manager.hpp"
#include "concurrency/transaction_context.hpp"

namespace opossum {

class TransactionManagerTest : public BaseTest {

 protected:
  void SetUp() override {}

  TransactionManager& manager() { return TransactionManager::get(); }

  static std::unordered_multiset<CommitID>& get_active_snapshot_commit_ids() {
    return TransactionManager::get()._active_snapshot_commit_ids;
  }
};

// Check if all active snapshot commit ids of uncommitted
// transaction contexts are tracked correctly.
// Normally, deregister_transaction() is called in the
// destructor of the transaction context but is called
// manually for this test.
TEST_F(TransactionManagerTest, TrackActiveCommitIDs) {

  EXPECT_EQ(get_active_snapshot_commit_ids().size(), 0);

  const auto t1_context = manager().new_transaction_context();
  const auto t2_context = manager().new_transaction_context();
  const auto t3_context = manager().new_transaction_context();

  EXPECT_EQ(get_active_snapshot_commit_ids().size(), 3);
  EXPECT_TRUE(std::find(get_active_snapshot_commit_ids().cbegin(), get_active_snapshot_commit_ids().cend(), t1_context->snapshot_commit_id()) != get_active_snapshot_commit_ids().cend());
  EXPECT_TRUE(std::find(get_active_snapshot_commit_ids().cbegin(), get_active_snapshot_commit_ids().cend(), t2_context->snapshot_commit_id()) != get_active_snapshot_commit_ids().cend());
  EXPECT_TRUE(std::find(get_active_snapshot_commit_ids().cbegin(), get_active_snapshot_commit_ids().cend(), t3_context->snapshot_commit_id()) != get_active_snapshot_commit_ids().cend());
  EXPECT_EQ(manager().get_lowest_active_snapshot_commit_id(), t1_context->snapshot_commit_id());

  t1_context->commit();
  manager().deregister_transaction(t1_context->snapshot_commit_id());
  
  EXPECT_EQ(get_active_snapshot_commit_ids().size(), 2);
  EXPECT_TRUE(std::find(get_active_snapshot_commit_ids().cbegin(), get_active_snapshot_commit_ids().cend(), t1_context->snapshot_commit_id()) != get_active_snapshot_commit_ids().cend());
  EXPECT_TRUE(std::find(get_active_snapshot_commit_ids().cbegin(), get_active_snapshot_commit_ids().cend(), t3_context->snapshot_commit_id()) != get_active_snapshot_commit_ids().cend());
  EXPECT_EQ(manager().get_lowest_active_snapshot_commit_id(), t2_context->snapshot_commit_id());

  t3_context->commit();
  manager().deregister_transaction(t3_context->snapshot_commit_id());

  EXPECT_EQ(get_active_snapshot_commit_ids().size(), 1);
  EXPECT_TRUE(std::find(get_active_snapshot_commit_ids().cbegin(), get_active_snapshot_commit_ids().cend(), t2_context->snapshot_commit_id()) != get_active_snapshot_commit_ids().cend());
  EXPECT_EQ(manager().get_lowest_active_snapshot_commit_id(), t2_context->snapshot_commit_id());
    
  t2_context->commit();
  manager().deregister_transaction(t2_context->snapshot_commit_id());

  EXPECT_EQ(get_active_snapshot_commit_ids().size(), 0);
  EXPECT_EQ(manager().get_lowest_active_snapshot_commit_id(), MvccData::MAX_COMMIT_ID);
}

}  // namespace opossum

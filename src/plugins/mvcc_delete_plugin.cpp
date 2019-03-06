#include "mvcc_delete_plugin.hpp"

#include "numeric"
#include "operators/get_table.hpp"
#include "operators/table_wrapper.hpp"
#include "operators/update.hpp"
#include "operators/validate.hpp"
#include "storage/pos_list.hpp"
#include "storage/reference_segment.hpp"
#include "storage/table.hpp"

namespace opossum {

const std::string MvccDeletePlugin::description() const { return "Physical MVCC delete plugin"; }

void MvccDeletePlugin::start() {
  _loop_thread_logical_delete =
      std::make_unique<PausableLoopThread>(_IDLE_DELAY_LOGICAL_DELETE, [&](size_t) { _logical_delete_loop(); });

  _loop_thread_physical_delete =
      std::make_unique<PausableLoopThread>(_IDLE_DELAY_PHYSICAL_DELETE, [&](size_t) { _physical_delete_loop(); });
}

void MvccDeletePlugin::stop() {
  // Call destructor of PausableLoopThread to terminate its thread
  _loop_thread_logical_delete.reset();
  _loop_thread_physical_delete.reset();
}

/**
 * This function analyzes each chunk of every table and triggers a chunk-cleanup-procedure if a certain threshold of
 * invalidated rows is exceeded.
 */
void MvccDeletePlugin::_logical_delete_loop() {
  for (auto& [table_name, table] : StorageManager::get().tables()) {
    // Check all chunks, except for the last one, which is currently used for insertions
    const auto max_chunk_id = static_cast<ChunkID>(table->chunk_count() - 1);

    for (auto chunk_id = ChunkID{0}; chunk_id < max_chunk_id; chunk_id++) {
      const auto& chunk = table->get_chunk(chunk_id);
      if (chunk && !chunk->get_cleanup_commit_id()) {
        // Calculate metric 1 – Chunk invalidation level
        const double invalidated_rows_ratio = static_cast<double>(chunk->invalid_row_count()) / chunk->size();

        // Calculate metric 2 – Chunk Hotness
        const CommitID lowest_end_commit_id =
            *std::min_element(std::begin(chunk->mvcc_data()->end_cids), std::end(chunk->mvcc_data()->end_cids));
        const CommitID commit_id_diff = TransactionManager::get().last_commit_id() - lowest_end_commit_id;
        const CommitID max_commit_id_diff =
            static_cast<CommitID>(table->max_chunk_size() * _DELETE_THRESHOLD_COMMIT_DIFF_FACTOR);

        // Evaluate metrics
        const bool criterion1 = _DELETE_THRESHOLD_RATE_INVALIDATED_ROWS <= invalidated_rows_ratio;
        const bool criterion2 = max_commit_id_diff <= commit_id_diff;

        if (criterion1 && criterion2) {
          const bool success = _delete_chunk_logically(table_name, chunk_id);

          if (success) {
            DebugAssert(StorageManager::get().get_table(table_name)->get_chunk(chunk_id)->get_cleanup_commit_id(),
                        "Chunk needs to be deleted logically before deleting it physically.");

            std::unique_lock<std::mutex> lock(_mutex_physical_delete_queue);
            _physical_delete_queue.emplace(table_name, chunk_id);
          } else {
            std::cout << "Logical delete of chunk " << chunk_id << " failed because of MVCC conflict. Retrying..."
                      << std::endl;
          }
        }
      }
    }  // for each chunk
  }    // for each table
}

/**
 * This function processes the physical-delete-queue until its empty.
 */
void MvccDeletePlugin::_physical_delete_loop() {
  std::unique_lock<std::mutex> lock(_mutex_physical_delete_queue);

  ChunkSpecifier chunk_spec = _physical_delete_queue.front();
  const auto& table = StorageManager::get().get_table(chunk_spec.table_name);
  const auto& chunk = table->get_chunk(chunk_spec.chunk_id);

  DebugAssert(chunk != nullptr, "Chunk does not exist. Physical Delete can not be applied.");

  // Check whether there are still active transactions that might use the chunk
  if (chunk->get_cleanup_commit_id()) {
    CommitID cleanup_commit_id = chunk->get_cleanup_commit_id().value();
    CommitID lowest_snapshot_commit_id = TransactionManager::get().get_lowest_active_snapshot_commit_id();

    if (cleanup_commit_id < lowest_snapshot_commit_id) {
      _delete_chunk_physically(chunk_spec.table_name, chunk_spec.chunk_id);
      _physical_delete_queue.pop();
    }
  }
}

bool MvccDeletePlugin::_delete_chunk_logically(const std::string& table_name, const ChunkID chunk_id) {
  const auto& table = StorageManager::get().get_table(table_name);
  const auto& chunk = table->get_chunk(chunk_id);

  DebugAssert(chunk != nullptr, "Chunk does not exist. Physical Delete can not be applied.");
  DebugAssert(chunk_id < (table->chunk_count() - 1),
              "MVCC Logical Delete should not be applied on the last/current mutable chunk.");

  // Create temporary referencing table that contains the given chunk only
  auto transaction_context = TransactionManager::get().new_transaction_context();
  auto gt = std::make_shared<GetTable>(table_name);
  gt->set_transaction_context(transaction_context);

  std::vector<ChunkID> excluded_chunk_ids(table->chunk_count());
  std::iota(std::begin(excluded_chunk_ids), std::end(excluded_chunk_ids), 0);
  excluded_chunk_ids.erase(std::remove(std::begin(excluded_chunk_ids), std::end(excluded_chunk_ids), chunk_id));

  gt->set_excluded_chunk_ids(excluded_chunk_ids);
  gt->execute();

  // Validate temporary table
  auto validate_table = std::make_shared<Validate>(gt);
  validate_table->set_transaction_context(transaction_context);
  validate_table->execute();

  // Use Update operator to delete and re-insert valid records in chunk
  // Pass validate_table into Update operator twice since data will not be changed.
  auto update_table = std::make_shared<Update>(table_name, validate_table, validate_table);
  update_table->set_transaction_context(transaction_context);
  update_table->execute();

  // Check for success
  if (update_table->execute_failed()) {
    transaction_context->rollback();
    return false;
  } else {
    // TODO(all): Check for success of commit, currently (2019-01-11) not yet possible.
    transaction_context->commit();

    // Mark chunk as logically deleted
    chunk->set_cleanup_commit_id(transaction_context->commit_id());
    std::cout << "Deleted chunk " << chunk_id << " logically." << std::endl;
    return true;
  }
}

void MvccDeletePlugin::_delete_chunk_physically(const std::string& table_name, const ChunkID chunk_id) {
  const auto& table = StorageManager::get().get_table(table_name);
  const auto& chunk = table->get_chunk(chunk_id);

  DebugAssert(chunk.use_count() == 2,
              "At this point, the chunk should be referenced by the plugin and the "
              "Table-chunk-vector only.");
  DebugAssert(chunk->get_cleanup_commit_id(),
              "The cleanup commit id of the chunk is not set. "
              "This should have been done by the logical delete.");

  // Usage checks have been passed. Apply physical delete now.
  table->remove_chunk(chunk_id);
  std::cout << "Deleted chunk " << chunk_id << " of table " << table_name << " physically." << std::endl;
}

EXPORT_PLUGIN(MvccDeletePlugin)

}  // namespace opossum

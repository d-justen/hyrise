#include <operators/get_table.hpp>
#include <operators/validate.hpp>
#include <operators/table_wrapper.hpp>
#include <operators/update.hpp>
#include "mvcc_delete_manager.hpp"
#include "storage/storage_manager.hpp"
#include "concurrency/transaction_manager.hpp"


namespace opossum {

void MvccDeleteManager::run_logical_delete(const std::string &tableName, ChunkID chunkID) {
  _delete_logically(tableName, chunkID);
}

bool MvccDeleteManager::_delete_logically(const std::string &tableName, const ChunkID chunkID) {
  auto& sm = StorageManager::get();
  const auto table = sm.get_table(tableName);
  auto chunk = table->get_chunk(chunkID);

  // ToDo: Maybe handle this as an edge case: -> Create a new chunk before Re-Insert
  DebugAssert(chunkID < (table->chunk_count() - 1), "MVCC Logical Delete should not be applied on the last/current mutable chunk.")

  // Create temporary table from chunk
  const auto table_extract = std::make_shared<Table>(table->column_definitions(), TableType::Data,
                                                          table->max_chunk_size(), table->has_mvcc());
  table_extract->append_chunk(chunk);
  auto table_wrapper = std::make_shared<TableWrapper>(table_extract);
  table_wrapper->execute();

  // Validate temporary table
  auto transaction_context = TransactionManager::get().new_transaction_context();
  auto validate_table = std::make_shared<Validate>(table_wrapper);
  validate_table->set_transaction_context(transaction_context);
  validate_table->execute();

  // Use UPDATE operator to DELETE and RE-INSERT valid records in chunk
    // Pass validate_table as input twice since data will not be changed.
  auto update_table = std::make_shared<Update>(tableName, validate_table, validate_table);
  update_table->set_transaction_context(transaction_context);
  update_table->execute();

  // Check for success
  if(update_table->execute_failed()) {
    return false;
  }

  // TODO: Check for success of commit, currently (2019-01-11) not possible.
  transaction_context->commit();

  // Mark chunk as logically deleted
  chunk->set_cleanup_commit_id(transaction_context->commit_id());
  return true;
}

void MvccDeleteManager::_delete_physically(const std::string &tableName, const opossum::ChunkID chunkID) {
  // Assert: Logical delete must have happened to this point.

  // TODO later
}


}  // namespace opossum
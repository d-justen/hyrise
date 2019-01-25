#include "mvcc_delete_plugin.hpp"

#include "operators/table_wrapper.hpp"
#include "operators/update.hpp"
#include "operators/validate.hpp"
#include "storage/pos_list.hpp"
#include "storage/reference_segment.hpp"
#include "storage/table.hpp"

namespace opossum {

const std::string MvccDeletePlugin::description() const { return "This is the Hyrise TestPlugin"; }

void MvccDeletePlugin::start() {

  // TODO(anyone) Put this into a separate function, e.g. _analyze_chunks()
  for (const auto& table : sm.tables()) {
    const auto& chunks = table.second->chunks();

    for (ChunkID chunk_id = ChunkID{0}; chunk_id < chunks.size(); chunk_id++) {
      const auto& chunk = chunks[chunk_id];

      if (chunk) {
        const double invalid_row_amount = static_cast<double>(chunk->invalid_row_count()) / chunk->size();
        if (invalid_row_amount >= DELETE_THRESHOLD) _clean_up_chunk(table.first, chunk_id);
      }
    }
  }
}

void MvccDeletePlugin::stop() {
  //TODO: Implement if necessary
}

void MvccDeletePlugin::_clean_up_chunk(const std::string &table_name, opossum::ChunkID chunk_id) {
  // Delete chunk logically
  bool success = _delete_chunk_logically(table_name, chunk_id);

  // Queue physical delete
  if (success) {
    DebugAssert(StorageManager::get().get_table(table_name)->get_chunk(chunk_id)->get_cleanup_commit_id()
                != MvccData::MAX_COMMIT_ID, "Chunk needs to be deleted logically before deleting it physically.")
    _physical_delete_queue.emplace(table_name, chunk_id);
  }
}


bool MvccDeletePlugin::_delete_chunk_logically(const std::string& table_name, ChunkID chunk_id) {
  const auto& table = StorageManager::get().get_table(table_name);
  const auto& chunk = table->get_chunk(chunk_id);

  // ToDo: Maybe handle this as an edge case: -> Create a new chunk before Re-Insert
  DebugAssert(chunk_id < (table->chunk_count() - 1),
              "MVCC Logical Delete should not be applied on the last/current mutable chunk.")

  // Create temporary referencing table that contains the given chunk only
  auto table_filtered = _get_referencing_table(table_name, chunk_id);
  auto table_wrapper = std::make_shared<TableWrapper>(table_filtered);
  table_wrapper->execute();

  // Validate temporary table
  auto transaction_context = TransactionManager::get().new_transaction_context();
  auto validate_table = std::make_shared<Validate>(table_wrapper);
  validate_table->set_transaction_context(transaction_context);
  validate_table->execute();

  // Use Update operator to delete and re-insert valid records in chunk
  // Pass validate_table into Update operator twice since data will not be changed.
  auto update_table = std::make_shared<Update>(table_name, validate_table, validate_table);
  update_table->set_transaction_context(transaction_context);
  update_table->execute();

  // Check for success
  if (update_table->execute_failed()) {
    return false;
  }

  // TODO(all): Check for success of commit, currently (2019-01-11) not yet possible.
  transaction_context->commit();

  // Mark chunk as logically deleted
  chunk->set_cleanup_commit_id(transaction_context->commit_id());

  return true;
}

bool MvccDeletePlugin::_delete_chunk_physically(const std::string& table_name, ChunkID chunk_id) {
  const auto& table = StorageManager::get().get_table(table_name);

  DebugAssert(table->get_chunk(chunk_id) != nullptr, "Chunk does not exist. Physical Delete can not be applied.")

  // Check whether there are still active transactions that might use the chunk
  CommitID cleanup_commit_id = table->get_chunk(chunk_id)->get_cleanup_commit_id();
  CommitID lowest_snapshot_commit_id = TransactionManager::get().get_lowest_active_snapshot_commit_id();
  if (cleanup_commit_id < lowest_snapshot_commit_id) {
    DebugAssert(table->chunks()[chunk_id].use_count() == 1,
                "At this point, the chunk should be referenced by the "
                "Table-chunk-vector only.")
    // Usage checks have been passed. Apply physical delete now.
    table->delete_chunk(chunk_id);
    return true;
  } else {
    // Chunk might still be in use. Wait with physical delete.
    return false;
  }
}

/**
 * Creates a new referencing table with only one chunk from a given table
 */
std::shared_ptr<const Table> MvccDeletePlugin::_get_referencing_table(const std::string& table_name, const ChunkID chunk_id) {
  auto& sm = StorageManager::get();
  const auto table_in = sm.get_table(table_name);
  const auto chunk_in = table_in->get_chunk(chunk_id);

  // Create new table
  auto table_out = std::make_shared<Table>(table_in->column_definitions(), TableType::References);

  DebugAssert(!std::dynamic_pointer_cast<const ReferenceSegment>(chunk_in->get_segment(ColumnID{0})),
              "Only Value- or DictionarySegments can be used.");

  // Generate pos_list_out.
  auto pos_list_out = std::make_shared<PosList>();
  auto chunk_size = chunk_in->size();  // The compiler fails to optimize this in the for clause :(
  for (auto i = 0u; i < chunk_size; i++) {
    pos_list_out->emplace_back(RowID{chunk_id, i});
  }

  // Create actual ReferenceSegment objects.
  Segments output_segments;
  for (ColumnID column_id{0}; column_id < chunk_in->column_count(); ++column_id) {
    auto ref_segment_out = std::make_shared<ReferenceSegment>(table_in, column_id, pos_list_out);
    output_segments.push_back(ref_segment_out);
  }

  if (!pos_list_out->empty() > 0) {
    table_out->append_chunk(output_segments);
  }

  return table_out;
}  // namespace opossum


void MvccDeletePlugin::_process_physical_delete_queue() {
  bool success = true;
  while(!_physical_delete_queue.empty() && success) {

    ChunkSpecifier chunk_spec = _physical_delete_queue.front();
    success = _delete_chunk_physically(chunk_spec.table_name, chunk_spec.chunk_id);

    if(success) {
      _physical_delete_queue.pop();
    }
  }
}

EXPORT_PLUGIN(MvccDeletePlugin)

}  // namespace opossum

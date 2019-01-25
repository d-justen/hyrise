#pragma once

#include <queue>
#include "storage/storage_manager.hpp"
#include "utils/abstract_plugin.hpp"
#include "utils/singleton.hpp"
#include "gtest/gtest_prod.h"

namespace opossum {

class MvccDeletePlugin : public AbstractPlugin, public Singleton<MvccDeletePlugin> {

  FRIEND_TEST(MvccDeleteTest, LogicalDelete);
    FRIEND_TEST(MvccDeleteTest, PhysicalDelete);
    FRIEND_TEST(MvccDeleteTest, PhysicalDelete_NegativePrecondition_cleanup_commit_id);

 public:
  MvccDeletePlugin() : sm(StorageManager::get()) {}

  const std::string description() const final;

  void start() final;

  void stop() final;

 private:
    struct ChunkSpecifier {
        std::string table_name;
        ChunkID chunk_id;
    };

    void _clean_up_chunk(const std::string &table_name, ChunkID chunk_id);

    static bool _delete_chunk_logically(const std::string& table_name, ChunkID chunk_id);
    static bool _delete_chunk_physically(const std::string& table_name, ChunkID chunk_id);
    void _process_physical_delete_queue();
    static std::shared_ptr<const Table> _get_referencing_table(const std::string& table_name, ChunkID chunk_id);


    const double DELETE_THRESHOLD = 0.9;
    StorageManager& sm;
    std::queue<ChunkSpecifier> _physical_delete_queue;
};

}  // namespace opossum

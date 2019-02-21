#pragma once

#include <algorithm>
#include <mutex>
#include <queue>
#include <thread>

#include "gtest/gtest_prod.h"
#include "storage/chunk.hpp"
#include "storage/storage_manager.hpp"
#include "utils/abstract_plugin.hpp"
#include "utils/pausable_loop_thread.hpp"
#include "utils/singleton.hpp"

namespace opossum {

class MvccDeletePlugin : public AbstractPlugin, public Singleton<MvccDeletePlugin> {
  friend class MvccDeletePluginCoreTest;

 public:
  const std::string description() const final;

  void start() final;

  void stop() final;

 private:
  struct ChunkSpecifier {
    std::string table_name;
    ChunkID chunk_id;
    ChunkSpecifier(std::string table_name, ChunkID chunk_id) : table_name(std::move(table_name)), chunk_id(chunk_id) {}
  };

  void _logical_delete_loop(StorageManager& sm);
  void _physical_delete_loop();

  void _delete_chunk(const std::string& table_name, ChunkID chunk_id);
  static bool _delete_chunk_logically(const std::string& table_name, ChunkID chunk_id);
  static bool _delete_chunk_physically(const std::string& table_name, ChunkID chunk_id);

  std::unique_ptr<PausableLoopThread> _loop_thread_logical_delete, _loop_thread_physical_delete;

  std::mutex _mutex_physical_delete_queue;
  std::queue<ChunkSpecifier> _physical_delete_queue;

  const double _DELETE_THRESHOLD_RATE_INVALIDATED_ROWS = 0.8;
  const double _DELETE_THRESHOLD_COMMIT_DIFF_FACTOR = 1.5;
  const std::chrono::milliseconds _IDLE_DELAY_LOGICAL_DELETE = std::chrono::milliseconds(1000);
  const std::chrono::milliseconds _IDLE_DELAY_PHYSICAL_DELETE = std::chrono::milliseconds(1000);
};

}  // namespace opossum

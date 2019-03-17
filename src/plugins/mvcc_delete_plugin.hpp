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
  friend class MvccDeletePluginTest;

 public:
  const std::string description() const final;

  void start() final;

  void stop() final;

 private:
  using TableAndChunkID = std::pair<const std::shared_ptr<Table>&, ChunkID>;

  void _logical_delete_loop();
  void _physical_delete_loop();

  static bool _try_logical_delete(const std::string& table_name, ChunkID chunk_id);
  static void _delete_chunk_physically(const std::shared_ptr<Table>& table, ChunkID chunk_id);

  std::unique_ptr<PausableLoopThread> _loop_thread_logical_delete, _loop_thread_physical_delete;

  std::mutex _mutex_physical_delete_queue;
  std::queue<TableAndChunkID> _physical_delete_queue;

  constexpr static double _DELETE_THRESHOLD_RATE_INVALIDATED_ROWS = 0.6;
  constexpr static double _DELETE_THRESHOLD_COMMIT_DIFF_FACTOR = 1.5;
  constexpr static std::chrono::milliseconds _IDLE_DELAY_LOGICAL_DELETE = std::chrono::milliseconds(900);
  constexpr static std::chrono::milliseconds _IDLE_DELAY_PHYSICAL_DELETE = std::chrono::milliseconds(900);
};

}  // namespace opossum

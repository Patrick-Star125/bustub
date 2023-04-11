//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lock_manager.h
//
// Identification: src/include/concurrency/lock_manager.h
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <algorithm>
#include <condition_variable>  // NOLINT
#include <list>
#include <memory>
#include <mutex>  // NOLINT
#include <set>
#include <unordered_map>
#include <utility>
#include <vector>

#include "common/config.h"
#include "common/rid.h"
#include "concurrency/transaction.h"

namespace bustub {

class TransactionManager;

/**
 * LockManager handles transactions asking for locks on records.
 */
class LockManager {
  enum class LockMode { SHARED, EXCLUSIVE };

  enum class RIDStatus { FREE, SHARED, EXCLUSIVE };

  enum class KillType { ALL_REQUEST, WRITE_REQUEST };

  static const txn_id_t MAX_ID = 0x0fffffff; //相当于32位地址最大值

  class LockRequest {
   public:
    LockRequest(Transaction *txn, txn_id_t txn_id, LockMode lock_mode)
        : txn_id_(txn_id), transation_(txn), lock_mode_(lock_mode), granted_{false} {}
    txn_id_t txn_id_;
    Transaction *transation_;
    LockMode lock_mode_;
    bool granted_;
  };

  class LockRequestQueue {
    struct SetpComparator {  // 重载map的key值排序方式
      bool operator()(const LockRequest &lhs, const LockRequest &rhs) const { return lhs.txn_id_ < rhs.txn_id_; }
    };

   public:
    // std::set<LockRequest, SetpComparator> request_queue_;
    // 改用list实现
    std::list<LockRequest> request_queue_;
    // for notifying blocked transactions on this rid
    std::condition_variable cv_;
    // txn_id of an upgrading transaction (if any)
    txn_id_t upgrading_ = INVALID_TXN_ID;

    int share_req_cnt_{0};  // 当前持有S锁的事务个数

    RIDStatus status_;  // 当前RID上的锁
  };

 public:
  /**
   * Creates a new lock manager configured for the deadlock prevention policy.
   */
  LockManager() = default;

  ~LockManager() = default;

  /*
   * [LOCK_NOTE]: For all locking functions, we:
   * 1. return false if the transaction is aborted; and
   * 2. block on wait, return true when the lock request is granted; and
   * 3. it is undefined behavior to try locking an already locked RID in the
   * same transaction, i.e. the transaction is responsible for keeping track of
   * its current locks.
   */

  /**
   * Acquire a lock on RID in shared mode. See [LOCK_NOTE] in header file.
   * @param txn the transaction requesting the shared lock
   * @param rid the RID to be locked in shared mode
   * @return true if the lock is granted, false otherwise
   */
  auto LockShared(Transaction *txn, const RID &rid) -> bool;

  /**
   * Acquire a lock on RID in exclusive mode. See [LOCK_NOTE] in header file.
   * @param txn the transaction requesting the exclusive lock
   * @param rid the RID to be locked in exclusive mode
   * @return true if the lock is granted, false otherwise
   */
  auto LockExclusive(Transaction *txn, const RID &rid) -> bool;

  /**
   * Upgrade a lock from a shared lock to an exclusive lock.
   * @param txn the transaction requesting the lock upgrade
   * @param rid the RID that should already be locked in shared mode by the
   * requesting transaction
   * @return true if the upgrade is successful, false otherwise
   */
  auto LockUpgrade(Transaction *txn, const RID &rid) -> bool;

  /**
   * Release the lock held by the transaction.
   * @param txn the transaction releasing the lock, it should actually hold the
   * lock
   * @param rid the RID that is locked by the transaction
   * @return true if the unlock is successful, false otherwise
   */
  auto Unlock(Transaction *txn, const RID &rid) -> bool;

  void KillRequest(txn_id_t id, const RID &rid, KillType type);

  void AwakeSharedRequest(const RID &rid);

  auto ProcessRequest(Transaction *txn, const RID &rid, const LockRequest &req) -> bool;

  auto UnlockImp(Transaction *txn, const RID &rid) -> bool;

 private:
  std::mutex latch_;

  /** Lock table for lock requests. */
  std::unordered_map<RID, LockRequestQueue> lock_table_;
};

}  // namespace bustub


//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// extendible_hash_table.cpp
//
// Identification: src/container/hash/extendible_hash_table.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <iostream>
#include <string>
#include <utility>
#include <vector>

#include "common/exception.h"
#include "common/logger.h"
#include "common/rid.h"
#include "container/hash/extendible_hash_table.h"

namespace bustub {

template <typename KeyType, typename ValueType, typename KeyComparator>
HASH_TABLE_TYPE::ExtendibleHashTable(const std::string &name, BufferPoolManager *buffer_pool_manager,
                                     const KeyComparator &comparator, HashFunction<KeyType> hash_fn)
    : buffer_pool_manager_(buffer_pool_manager), comparator_(comparator), hash_fn_(std::move(hash_fn)) {
  //  implement me!
  auto dir_page = CreateDirectoryPage(&directory_page_id_);  // 创建目录页

  page_id_t bucket_page_id;
  buffer_pool_manager_->NewPage(&bucket_page_id, nullptr);  // 申请第一个桶的页
  dir_page->SetBucketPageId(0, bucket_page_id);

  buffer_pool_manager_->UnpinPage(bucket_page_id, false, nullptr);     // 放回桶页
  buffer_pool_manager_->UnpinPage(directory_page_id_, true, nullptr);  // 放回目录页
}

/*****************************************************************************
 * HELPERS
 *****************************************************************************/
/**
 * Hash - simple helper to downcast MurmurHash's 64-bit hash to 32-bit
 * for extendible hashing.
 *
 * @param key the key to hash
 * @return the downcasted 32-bit hash
 */
template <typename KeyType, typename ValueType, typename KeyComparator>
uint32_t HASH_TABLE_TYPE::Hash(KeyType key) {
  return static_cast<uint32_t>(hash_fn_.GetHash(key));
}

template <typename KeyType, typename ValueType, typename KeyComparator>
inline uint32_t HASH_TABLE_TYPE::KeyToDirectoryIndex(KeyType key, HashTableDirectoryPage *dir_page) {
  uint32_t index = Hash(key) & dir_page->GetGlobalDepthMask();
  return index;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
inline page_id_t HASH_TABLE_TYPE::KeyToPageId(KeyType key, HashTableDirectoryPage *dir_page) {
  uint32_t index = KeyToDirectoryIndex(key, dir_page);
  page_id_t page_id = dir_page->GetBucketPageId(index);
  return page_id;
}
template <typename KeyType, typename ValueType, typename KeyComparator>
HashTableDirectoryPage *HASH_TABLE_TYPE::CreateDirectoryPage(page_id_t *bucket_page_id) {
  auto dir_page = reinterpret_cast<HashTableDirectoryPage *>(
      buffer_pool_manager_->NewPage(&directory_page_id_, nullptr)->GetData());  // 创建目录页
  return dir_page;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
HASH_TABLE_BUCKET_TYPE *HASH_TABLE_TYPE::CreateBucketPage(page_id_t *bucket_page_id) {
  auto *new_bucket_page =
      reinterpret_cast<HASH_TABLE_BUCKET_TYPE *>(buffer_pool_manager_->NewPage(bucket_page_id, nullptr)->GetData());
  return new_bucket_page;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
HashTableDirectoryPage *HASH_TABLE_TYPE::FetchDirectoryPage() {
  auto directory_page = reinterpret_cast<HashTableDirectoryPage *>(
      buffer_pool_manager_->FetchPage(directory_page_id_, nullptr)->GetData());
  return directory_page;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
HASH_TABLE_BUCKET_TYPE *HASH_TABLE_TYPE::FetchBucketPage(page_id_t bucket_page_id) {
  auto bucket_page =
      reinterpret_cast<HASH_TABLE_BUCKET_TYPE *>(buffer_pool_manager_->FetchPage(bucket_page_id, nullptr)->GetData());
  return bucket_page;
}

/*****************************************************************************
 * SEARCH
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_TYPE::GetValue(Transaction *transaction, const KeyType &key, std::vector<ValueType> *result) {
  table_latch_.RLock();
  auto dir_page = FetchDirectoryPage();
  page_id_t bucket_page_id = KeyToPageId(key, dir_page);
  auto *bucket_page = FetchBucketPage(bucket_page_id);

  reinterpret_cast<Page *>(bucket_page)->RLatch();
  bool ret = bucket_page->GetValue(key, comparator_, result);  // 读取桶页内容前加页的读锁
  reinterpret_cast<Page *>(bucket_page)->RUnlatch();

  buffer_pool_manager_->UnpinPage(bucket_page_id, false, nullptr);
  buffer_pool_manager_->UnpinPage(directory_page_id_, false, nullptr);
  table_latch_.RUnlock();
  return ret;
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_TYPE::Insert(Transaction *transaction, const KeyType &key, const ValueType &value) {
  table_latch_.RLock();
  auto dir_page = FetchDirectoryPage();
  page_id_t bucket_page_id = KeyToPageId(key, dir_page);
  auto *bucket_page = FetchBucketPage(bucket_page_id);

  reinterpret_cast<Page *>(bucket_page)->WLatch();
  bool ret = bucket_page->Insert(key, value, comparator_);
  reinterpret_cast<Page *>(bucket_page)->WUnlatch();

  buffer_pool_manager_->UnpinPage(bucket_page_id, true, nullptr);
  buffer_pool_manager_->UnpinPage(directory_page_id_, false, nullptr);
  table_latch_.RUnlock();
  if (!ret && bucket_page->IsFull()) {
    // printf("start split\n");
    ret = SplitInsert(transaction, key, value);
  }

  return ret;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_TYPE::SplitInsert(Transaction *transaction, const KeyType &key, const ValueType &value) {
  table_latch_.WLock();
  auto dir_page = FetchDirectoryPage();
  uint32_t old_bucket_page_index = KeyToDirectoryIndex(key, dir_page);
  page_id_t old_bucket_page_id = KeyToPageId(key, dir_page);  // 待分裂的桶
  uint32_t local_depth = dir_page->GetLocalDepth(old_bucket_page_index);
  auto *old_bucket_page = FetchBucketPage(old_bucket_page_id);

  if (!old_bucket_page->IsFull()) {  // 再次检查桶是否满了
    bool ret_tmp = old_bucket_page->Insert(key, value, comparator_);
    buffer_pool_manager_->UnpinPage(old_bucket_page_id, true, nullptr);
    buffer_pool_manager_->UnpinPage(directory_page_id_, false, nullptr);
    table_latch_.WUnlock();
    return ret_tmp;
  }

  page_id_t new_bucket_page_id;
  auto *new_bucket_page = CreateBucketPage(&new_bucket_page_id);

  uint32_t old_local_mask = dir_page->GetLocalDepthMask(old_bucket_page_index);
  uint32_t new_local_mask = old_local_mask + (old_local_mask + 1);  // 计算新的掩码
  uint32_t new_local_hash = old_bucket_page_index & new_local_mask;
  uint32_t dir_size = dir_page->Size();
  page_id_t page_id;

  for (uint32_t i = 0; i < dir_size; i++) {
    if ((i & new_local_mask) == new_local_hash) {  // 仍与旧桶保持一致，只需将深度加一
      dir_page->IncrLocalDepth(i);
    }
  }
  if (local_depth < dir_page->GetGlobalDepth()) {  // 不影响目录大小，只是将一半指向旧桶的指针改向新桶
    for (uint32_t i = 0; i < dir_size; i++) {
      page_id = dir_page->GetBucketPageId(i);
      if (page_id == old_bucket_page_id &&
          (i & new_local_mask) != new_local_hash) {  // 与旧桶不再一致，将目录指向新桶并将深度加一
        dir_page->SetBucketPageId(i, new_bucket_page_id);
        dir_page->IncrLocalDepth(i);
      }
    }
  } else {
    dir_page->IncrGlobalDepth();  // 目录长度变成原来的两倍
    uint32_t new_dir_size = dir_page->Size();
    page_id_t upper_page_id;
    uint32_t upper_local_depth;

    for (uint32_t i = dir_size; i < new_dir_size; i++) {
      // 下半部与上半部成镜像，只是分裂的桶需改id，其他的与上半部保持一致
      upper_page_id = dir_page->GetBucketPageId(i - dir_size);
      upper_local_depth = dir_page->GetLocalDepth(i - dir_size);
      if (upper_page_id == old_bucket_page_id) {  // 分裂桶对应的桶
        dir_page->SetBucketPageId(i, new_bucket_page_id);
      } else {  // 其余桶，page id和depth与上半部保持一致
        dir_page->SetBucketPageId(i, upper_page_id);
      }
      dir_page->SetLocalDepth(i, upper_local_depth);  // 统一设置成与上半部一样的深度
    }
  }

  reinterpret_cast<Page *>(old_bucket_page)->WLatch();
  reinterpret_cast<Page *>(new_bucket_page)->WLatch();

  uint32_t bucket_size = old_bucket_page->Size();
  KeyType bucket_key;
  ValueType bucket_value;
  page_id_t bucket_page_id;
  for (uint32_t i = 0; i < bucket_size; i++) {  // 遍历旧桶中的元素，插入部分元素至新桶
    bucket_key = old_bucket_page->KeyAt(i);
    bucket_value = old_bucket_page->ValueAt(i);
    bucket_page_id = KeyToPageId(bucket_key, dir_page);
    if (bucket_page_id == new_bucket_page_id) {
      old_bucket_page->RemoveAt(i);
      new_bucket_page->Insert(bucket_key, bucket_value, comparator_);
    }
  }

  bucket_page_id = KeyToPageId(key, dir_page);
  bool ret;
  if (bucket_page_id == old_bucket_page_id) {  // 进行正常插入操作
    ret = old_bucket_page->Insert(key, value, comparator_);
  } else {
    ret = new_bucket_page->Insert(key, value, comparator_);
  }

  reinterpret_cast<Page *>(old_bucket_page)->WUnlatch();
  reinterpret_cast<Page *>(new_bucket_page)->WUnlatch();

  buffer_pool_manager_->UnpinPage(old_bucket_page_id, true, nullptr);
  buffer_pool_manager_->UnpinPage(new_bucket_page_id, true, nullptr);
  buffer_pool_manager_->UnpinPage(directory_page_id_, true, nullptr);
  table_latch_.WUnlock();
  return ret;
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_TYPE::Remove(Transaction *transaction, const KeyType &key, const ValueType &value) {
  table_latch_.RLock();
  auto dir_page = FetchDirectoryPage();
  page_id_t bucket_page_id = KeyToPageId(key, dir_page);
  auto *bucket_page = FetchBucketPage(bucket_page_id);

  reinterpret_cast<Page *>(bucket_page)->WLatch();
  bool ret = bucket_page->Remove(key, value, comparator_);
  reinterpret_cast<Page *>(bucket_page)->WUnlatch();

  buffer_pool_manager_->UnpinPage(bucket_page_id, true, nullptr);  // 要提前unpin，有可能要删除该桶
  buffer_pool_manager_->UnpinPage(directory_page_id_, false, nullptr);
  table_latch_.RUnlock();
  if (ret && bucket_page->IsEmpty()) {
    // printf("start merge\n");
    Merge(transaction, key, value);
    while (ExtraMerge(transaction, key, value)) {
    }
  }

  return ret;
}

/*****************************************************************************
 * MERGE
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_TYPE::Merge(Transaction *transaction, const KeyType &key, const ValueType &value) {
  table_latch_.WLock();
  auto dir_page = FetchDirectoryPage();
  uint32_t index = KeyToDirectoryIndex(key, dir_page);
  page_id_t bucket_page_id = KeyToPageId(key, dir_page);
  uint32_t dir_size = dir_page->Size();
  uint32_t local_depth = dir_page->GetLocalDepth(index);
  uint32_t local_mask = dir_page->GetLocalDepthMask(index);
  uint32_t same_mask = local_mask ^ (1 << (local_depth - 1));  // 合并后的掩码，将最高位的1去掉

  auto *bucket_page = FetchBucketPage(bucket_page_id);
  bool merge_occur = false;  // 标志是否发生合并

  if (local_depth > 0 && bucket_page->IsEmpty()) {  // remove函数加的是读锁，有可能插入新值
    page_id_t another_bucket_page_id;
    uint32_t another_bucket_idx =
        dir_page->GetSplitImageIndex(index);  // 与其对应的桶，如果两者深度一致，则可以合并成一个桶
    uint32_t another_local_depth = dir_page->GetLocalDepth(another_bucket_idx);
    if (another_local_depth == local_depth) {  // 此时可以进行合并操作
      merge_occur = true;
      another_bucket_page_id = dir_page->GetBucketPageId(another_bucket_idx);
    }
    if (merge_occur) {
      for (uint32_t i = 0; i < dir_size; i++) {
        if ((i & local_mask) == (index & local_mask)) {  // 寻找指向空桶的指针,将其指向另一半another_bucket
          dir_page->SetBucketPageId(i, another_bucket_page_id);
        }
      }
      buffer_pool_manager_->UnpinPage(bucket_page_id, false, nullptr);  // 先unpin再删除
      buffer_pool_manager_->DeletePage(bucket_page_id, nullptr);
      for (uint32_t i = 0; i < dir_size; i++) {
        if ((i & same_mask) == (index & same_mask)) {  // 将所有指向another_bucket的local depth都减一
          dir_page->DecrLocalDepth(i);
        }
      }

      bool ret = dir_page->CanShrink();
      if (ret) {  // 降低全局深度
        dir_page->DecrGlobalDepth();
      }
    }
  }
  if (!merge_occur) {  // 合并未发生
    buffer_pool_manager_->UnpinPage(bucket_page_id, false, nullptr);
  }
  buffer_pool_manager_->UnpinPage(directory_page_id_, true, nullptr);
  table_latch_.WUnlock();
}

// 合并可能存在的另一半空桶  例如 00 10 指向空桶 01 指向非空 11变成空桶  再将11 和 01合并后再合并00 10对应的空桶
// 额外的合并操作
template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_TYPE::ExtraMerge(Transaction *transaction, const KeyType &key, const ValueType &value) {
  table_latch_.WLock();
  auto dir_page = FetchDirectoryPage();
  page_id_t bucket_page_id = KeyToPageId(key, dir_page);
  uint32_t index = KeyToDirectoryIndex(key, dir_page);
  uint32_t local_depth = dir_page->GetLocalDepth(index);
  uint32_t dir_size = dir_page->Size();
  bool extra_merge_occur = false;
  if (local_depth > 0) {
    auto extra_bucket_idx = dir_page->GetSplitImageIndex(index);  // 计算合并完后对应桶
    auto extra_local_depth = dir_page->GetLocalDepth(extra_bucket_idx);
    auto extra_bucket_page_id = dir_page->GetBucketPageId(extra_bucket_idx);
    auto *extra_bucket = FetchBucketPage(extra_bucket_page_id);
    if (extra_local_depth == local_depth && extra_bucket->IsEmpty()) {
      extra_merge_occur = true;
      page_id_t tmp_bucket_page_id;
      for (uint32_t i = 0; i < dir_size; i++) {
        tmp_bucket_page_id = dir_page->GetBucketPageId(i);
        if (tmp_bucket_page_id == extra_bucket_page_id) {  // 如果是空桶，更改指向并将深度减一
          dir_page->SetBucketPageId(i, bucket_page_id);
          dir_page->DecrLocalDepth(i);
        } else if (tmp_bucket_page_id == bucket_page_id) {  // 原先桶只需将深度减一
          dir_page->DecrLocalDepth(i);
        }
      }
      buffer_pool_manager_->UnpinPage(extra_bucket_page_id, false, nullptr);  // 先unpin再删除
      buffer_pool_manager_->DeletePage(extra_bucket_page_id, nullptr);
      bool ret = dir_page->CanShrink();
      if (ret) {  // 降低全局深度
        dir_page->DecrGlobalDepth();
      }
    }
    if (!extra_merge_occur) {  // 额外的合并未发生
      buffer_pool_manager_->UnpinPage(extra_bucket_page_id, false, nullptr);
    }
  }
  buffer_pool_manager_->UnpinPage(bucket_page_id, false, nullptr);
  buffer_pool_manager_->UnpinPage(directory_page_id_, true, nullptr);
  table_latch_.WUnlock();
  return extra_merge_occur;
}
/*****************************************************************************
 * GETGLOBALDEPTH - DO NOT TOUCH
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
uint32_t HASH_TABLE_TYPE::GetGlobalDepth() {
  table_latch_.RLock();
  HashTableDirectoryPage *dir_page = FetchDirectoryPage();
  uint32_t global_depth = dir_page->GetGlobalDepth();
  assert(buffer_pool_manager_->UnpinPage(directory_page_id_, false, nullptr));
  table_latch_.RUnlock();
  return global_depth;
}

/*****************************************************************************
 * VERIFY INTEGRITY - DO NOT TOUCH
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_TYPE::VerifyIntegrity() {
  table_latch_.RLock();
  HashTableDirectoryPage *dir_page = FetchDirectoryPage();
  dir_page->VerifyIntegrity();

  assert(buffer_pool_manager_->UnpinPage(directory_page_id_, false, nullptr));
  table_latch_.RUnlock();
}

// 测试方法
template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_TYPE::PrintDir() {
  table_latch_.RLock();
  HashTableDirectoryPage *dir_page = FetchDirectoryPage();
  uint32_t dir_size = dir_page->Size();

  dir_page->PrintDirectory();
  printf("dir size is: %d\n", dir_size);
  for (uint32_t idx = 0; idx < dir_size; idx++) {
    auto bucket_page_id = dir_page->GetBucketPageId(idx);
    HASH_TABLE_BUCKET_TYPE *bucket_page = FetchBucketPage(bucket_page_id);
    bucket_page->PrintBucket();
    buffer_pool_manager_->UnpinPage(bucket_page_id, false, nullptr);
  }

  assert(buffer_pool_manager_->UnpinPage(directory_page_id_, false, nullptr));
  table_latch_.RUnlock();
}

template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_TYPE::RemoveAllItem(Transaction *transaction, uint32_t bucket_idx) {
  table_latch_.RLock();
  HashTableDirectoryPage *dir_page = FetchDirectoryPage();
  auto bucket_page_id = dir_page->GetBucketPageId(bucket_idx);
  HASH_TABLE_BUCKET_TYPE *bucket_page = FetchBucketPage(bucket_page_id);
  auto items = bucket_page->GetAllItem();
  buffer_pool_manager_->UnpinPage(bucket_page_id, false, nullptr);
  table_latch_.RUnlock();
  for (auto &item : items) {
    Remove(nullptr, item.first, item.second);
  }

  buffer_pool_manager_->UnpinPage(directory_page_id_, false, nullptr);
}
/*****************************************************************************
 * TEMPLATE DEFINITIONS - DO NOT TOUCH
 *****************************************************************************/
template class ExtendibleHashTable<int, int, IntComparator>;

template class ExtendibleHashTable<GenericKey<4>, RID, GenericComparator<4>>;
template class ExtendibleHashTable<GenericKey<8>, RID, GenericComparator<8>>;
template class ExtendibleHashTable<GenericKey<16>, RID, GenericComparator<16>>;
template class ExtendibleHashTable<GenericKey<32>, RID, GenericComparator<32>>;
template class ExtendibleHashTable<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub

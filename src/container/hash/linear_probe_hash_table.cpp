//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// linear_probe_hash_table.cpp
//
// Identification: src/container/hash/linear_probe_hash_table.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <iostream>
#include <string>
#include <utility>
#include <vector>

#include "common/exception.h"
#include "common/logger.h"
#include "common/rid.h"
#include "container/hash/linear_probe_hash_table.h"

namespace bustub {

template <typename KeyType, typename ValueType, typename KeyComparator>
HASH_TABLE_TYPE::LinearProbeHashTable(const std::string &name, BufferPoolManager *buffer_pool_manager,
                                      const KeyComparator &comparator, size_t num_buckets,
                                      HashFunction<KeyType> hash_fn)
    : name_(name), buffer_pool_manager_(buffer_pool_manager), comparator_(comparator), hash_fn_(std::move(hash_fn)) {
  auto page = buffer_pool_manager->NewPage(&(this->header_page_id_));
  if (page == nullptr) {
    throw new Exception("Can't initialize header page");
  }
  auto header_page = reinterpret_cast<HashTableHeaderPage *>(page->GetData());
  header_page->SetSize(num_buckets);
  this->appendBuckets(header_page, num_buckets);
}

/*****************************************************************************
 * SEARCH
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_TYPE::GetValue(Transaction *transaction, const KeyType &key, std::vector<ValueType> *result) {
  this->table_latch_.RLock();
  auto header_page = this->HeaderPage();
  auto expected_index = this->GetSlotIndex(key);
  auto meet_starting_point = false;
  for (auto index = expected_index;; index = (index + 1) % header_page->GetSize()) {
    if (index == expected_index) {
      if (meet_starting_point) break;
      meet_starting_point = true;
    }
    // initialize block_page and block (casting)
    auto block_index = index / BLOCK_ARRAY_SIZE;
    auto page = this->buffer_pool_manager_->FetchPage(header_page->GetBlockPageId(block_index));
    auto block = reinterpret_cast<HashTableBlockPage<KeyType, ValueType, KeyComparator> *>(page->GetData());

    // expected offset of key-value pair in this block
    auto data_offset_in_block = index % BLOCK_ARRAY_SIZE;

    // start searching
    page->RLatch();
    if (!block->IsOccupied(data_offset_in_block)) {
      page->RUnlatch();
      break;
    }
    if (block->IsReadable(data_offset_in_block) && this->comparator_(key, block->KeyAt(data_offset_in_block)) == 0) {
      result->push_back(block->ValueAt(data_offset_in_block));
    }
    page->RUnlatch();
  }
  this->table_latch_.RUnlock();
  if (result->size() > 0) return true;
  return false;
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_TYPE::Insert(Transaction *transaction, const KeyType &key, const ValueType &value) {
  this->table_latch_.RLock();
  auto result = std::vector<ValueType>();
  this->GetValue(transaction, key, &result);
  if (std::find(result.begin(), result.end(), value) != result.end()) {
    return false;
  }
  auto header_page = this->HeaderPage();
  auto expected_index = this->GetSlotIndex(key);
  auto meet_starting_point = false;
  for (auto index = expected_index;; index = (index + 1) % header_page->GetSize()) {
    if (index == expected_index) {
      if (meet_starting_point) break;
      meet_starting_point = true;
    }
    // initialize block_page and block (casting)
    auto block_index = index / BLOCK_ARRAY_SIZE;
    auto page = this->buffer_pool_manager_->FetchPage(header_page->GetBlockPageId(block_index));
    auto block = reinterpret_cast<HashTableBlockPage<KeyType, ValueType, KeyComparator> *>(page->GetData());

    // inserting (and flushing the page if the insert operator is successfuly)
    page->WLatch();
    auto success = block->Insert(index % BLOCK_ARRAY_SIZE, key, value);
    if (success) {
      this->buffer_pool_manager_->FlushPage(page->GetPageId());
      page->WUnlatch();
      this->table_latch_.RUnlock();
      return true;
    }
    page->WUnlatch();
  }
  this->table_latch_.RUnlock();
  // come here mean the current hash table is full, we need to resize
  this->Resize(header_page->GetSize());
  return this->Insert(transaction, key, value);
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_TYPE::Remove(Transaction *transaction, const KeyType &key, const ValueType &value) {
  this->table_latch_.RLock();
  auto header_page = this->HeaderPage();
  auto expected_index = this->GetSlotIndex(key);
  auto meet_starting_point = false;
  for (auto index = expected_index;; index = (index + 1) % header_page->GetSize()) {
    if (index == expected_index) {
      if (meet_starting_point) break;
      meet_starting_point = true;
    }
    // initialize block_page and block (casting)
    auto block_index = index / BLOCK_ARRAY_SIZE;
    auto page = this->buffer_pool_manager_->FetchPage(header_page->GetBlockPageId(block_index));
    auto block = reinterpret_cast<HashTableBlockPage<KeyType, ValueType, KeyComparator> *>(page->GetData());
    // expected offset of key-value pair in this block
    auto data_offset_in_block = index % BLOCK_ARRAY_SIZE;

    // conditions
    page->WLatch();
    if (!block->IsOccupied(data_offset_in_block)) {
      page->WUnlatch();
      break;
    }
    if (block->IsReadable(data_offset_in_block) && this->comparator_(key, block->KeyAt(data_offset_in_block)) == 0 &&
        value == block->ValueAt(data_offset_in_block)) {
      // removing and unlock
      block->Remove(data_offset_in_block);
      page->WUnlatch();
      this->table_latch_.RUnlock();
      return true;
    }
    page->WUnlatch();
  }
  this->table_latch_.RUnlock();
  return false;
}

/*****************************************************************************
 * RESIZE
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_TYPE::Resize(size_t initial_size) {
  this->table_latch_.WLock();
  auto header_page = this->HeaderPage();
  auto expected_size = initial_size * 2;
  // only grow up in size
  if (header_page->GetSize() < expected_size) {
    auto old_size = header_page->GetSize();
    header_page->SetSize(expected_size);
    this->appendBuckets(header_page, expected_size - old_size);
    // re-organize all key-value pairs
    auto all_old_blocks = std::vector<HashTableBlockPage<KeyType, ValueType, KeyComparator> *>();
    auto all_block_page_ids = std::vector<page_id_t>();
    for (size_t idx = 0; idx < header_page->NumBlocks(); idx++) {
      all_old_blocks.push_back(this->BlockPage(header_page, idx));
      all_block_page_ids.push_back(header_page->GetBlockPageId(idx));
    }
    header_page->ResetBlockIndex();
    for (size_t idx = 0; idx < header_page->NumBlocks(); idx++) {
      const auto &block = all_old_blocks[idx];
      for (size_t pair_idx = 0; pair_idx < BLOCK_ARRAY_SIZE; pair_idx++) {
        this->Insert(nullptr, block->KeyAt(pair_idx), block->ValueAt(pair_idx));
      }
      this->buffer_pool_manager_->DeletePage(all_block_page_ids[idx]);
    }
  }
  this->table_latch_.WUnlock();
}

/*****************************************************************************
 * GETSIZE
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
size_t HASH_TABLE_TYPE::GetSize() {
  this->table_latch_.RLock();
  auto size = this->HeaderPage()->GetSize();
  this->table_latch_.RUnlock();
  return size;
}

/*****************************************************************************
 * UTILITIES (these functions should already be called in a lock context)
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
HashTableHeaderPage *HASH_TABLE_TYPE::HeaderPage() {
  return reinterpret_cast<HashTableHeaderPage *>(
      this->buffer_pool_manager_->FetchPage(this->header_page_id_)->GetData());
}

template <typename KeyType, typename ValueType, typename KeyComparator>
HashTableBlockPage<KeyType, ValueType, KeyComparator> *HASH_TABLE_TYPE::BlockPage(HashTableHeaderPage *header_page,
                                                                                  size_t bucket_ind) {
  return reinterpret_cast<HashTableBlockPage<KeyType, ValueType, KeyComparator> *>(
      this->buffer_pool_manager_->FetchPage(header_page->GetBlockPageId(bucket_ind))->GetData());
}

template <typename KeyType, typename ValueType, typename KeyComparator>
slot_offset_t HASH_TABLE_TYPE::GetSlotIndex(const KeyType &key) {
  return this->hash_fn_.GetHash(key) % this->HeaderPage()->GetSize();
}

template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_TYPE::appendBuckets(HashTableHeaderPage *header_page, size_t num_buckets) {
  size_t total_current_buckets = header_page->NumBlocks() * BLOCK_ARRAY_SIZE;
  for (; total_current_buckets < num_buckets; total_current_buckets += BLOCK_ARRAY_SIZE) {
    page_id_t next_block_id;
    assert(this->buffer_pool_manager_->NewPage(&next_block_id) != nullptr);
    this->buffer_pool_manager_->UnpinPage(next_block_id, true);
    this->buffer_pool_manager_->FlushPage(next_block_id);
    header_page->AddBlockPageId(next_block_id);
  }
}

template class LinearProbeHashTable<int, int, IntComparator>;

template class LinearProbeHashTable<GenericKey<4>, RID, GenericComparator<4>>;
template class LinearProbeHashTable<GenericKey<8>, RID, GenericComparator<8>>;
template class LinearProbeHashTable<GenericKey<16>, RID, GenericComparator<16>>;
template class LinearProbeHashTable<GenericKey<32>, RID, GenericComparator<32>>;
template class LinearProbeHashTable<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub

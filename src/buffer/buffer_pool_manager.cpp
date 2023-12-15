//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// buffer_pool_manager.cpp
//
// Identification: src/buffer/buffer_pool_manager.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/buffer_pool_manager.h"

#include "common/exception.h"
#include "common/macros.h"
#include "storage/page/page_guard.h"

namespace bustub {

BufferPoolManager::BufferPoolManager(size_t pool_size, DiskManager *disk_manager, size_t replacer_k,
                                     LogManager *log_manager)
    : pool_size_(pool_size), disk_scheduler_(std::make_unique<DiskScheduler>(disk_manager)), log_manager_(log_manager) {
  // we allocate a consecutive memory space for the buffer pool
  pages_ = new Page[pool_size_];
  replacer_ = std::make_unique<LRUKReplacer>(pool_size_, replacer_k);

  // Initially, every page is in the free list.
  for (size_t i = 0; i < pool_size_; ++i) {
    free_list_.emplace_back(static_cast<int>(i));
    pages_[i].ResetMemory();
    pages_[i].page_id_ = INVALID_PAGE_ID;
    pages_[i].is_dirty_ = false;
    pages_[i].pin_count_ = 0;
  }
}

BufferPoolManager::~BufferPoolManager() { delete[] pages_; }

auto BufferPoolManager::NewPage(page_id_t *page_id) -> Page * {
  std::lock_guard<std::shared_mutex> lock(latch_);
  // find a fresh frame id
  frame_id_t frame_id;
  if (free_list_.empty()) {
    if (!replacer_->Evict(&frame_id)) {
      *page_id = INVALID_PAGE_ID;
      return nullptr;
    }
    FlushPgInternal(pages_[frame_id].GetPageId());
  }
  frame_id = free_list_.front();
  free_list_.pop_front();

  // initialize the page
  pages_[frame_id].page_id_ = AllocatePage();
  pages_[frame_id].ResetMemory();
  pages_[frame_id].is_dirty_ = false;
  pages_[frame_id].pin_count_ = 1;

  *page_id = pages_[frame_id].page_id_;

  page_table_.insert({*page_id, frame_id});
  replacer_->RecordAccess(frame_id);
  replacer_->SetEvictable(frame_id, false);

  return &pages_[frame_id];
}

auto BufferPoolManager::FetchPage(page_id_t page_id, [[maybe_unused]] AccessType access_type) -> Page * {
  std::unique_lock<std::shared_mutex> lock(latch_);
  // return if found in buffer pool
  frame_id_t frame_id;
  if (page_table_.find(page_id) != page_table_.end()) {
    frame_id = page_table_.find(page_id)->second;
    pages_[frame_id].pin_count_++;
    replacer_->SetEvictable(frame_id, false);
    return &pages_[frame_id];
  }
  lock.unlock();

  auto promise = disk_scheduler_->CreatePromise();
  auto future = promise.get_future();

  // async fetch page from disk
  auto data = std::make_unique<char[]>(BUSTUB_PAGE_SIZE);
  disk_scheduler_->Schedule({/*is_write=*/false, data.get(), /*page_id=*/page_id, std::move(promise)});
  future.get();

  lock.lock();
  // check if this page is already in the pool
  if (page_table_.find(page_id) != page_table_.end()) {
    frame_id = page_table_.find(page_id)->second;
    pages_[frame_id].pin_count_++;
    replacer_->SetEvictable(frame_id, false);
    return &pages_[frame_id];
  }

  // try to vacate for the new disk fetched page
  if (free_list_.empty()) {
    if (!replacer_->Evict(&frame_id)) {
      return nullptr;
    }
    FlushPgInternal(pages_[frame_id].GetPageId());
  }
  frame_id = free_list_.front();
  free_list_.pop_front();

  // insert this page into the pool
  page_table_.insert({page_id, frame_id});

  pages_[frame_id].page_id_ = page_id;
  pages_[frame_id].is_dirty_ = false;
  pages_[frame_id].pin_count_ = 1;

  memcpy(pages_[frame_id].GetData(), data.get(), BUSTUB_PAGE_SIZE);
  lock.unlock();

  replacer_->RecordAccess(frame_id);
  replacer_->SetEvictable(frame_id, false);

  return &pages_[frame_id];
}

auto BufferPoolManager::UnpinPage(page_id_t page_id, bool is_dirty, [[maybe_unused]] AccessType access_type) -> bool {
  std::lock_guard<std::shared_mutex> lock(latch_);
  // find page from buffer pool
  frame_id_t frame_id;
  if (page_table_.find(page_id) == page_table_.end()) {
    return false;
  }
  frame_id = page_table_.find(page_id)->second;

  // return false if pin_count_ <= 0
  if (pages_[frame_id].pin_count_ <= 0) {
    return false;
  }

  if (is_dirty) {
    pages_[frame_id].is_dirty_ = true;
  }

  if (--pages_[frame_id].pin_count_ == 0) {
    replacer_->SetEvictable(frame_id, true);
  }

  return true;
}

auto BufferPoolManager::FlushPage(page_id_t page_id) -> bool {
  std::lock_guard<std::shared_mutex> lock(latch_);
  return FlushPgInternal(page_id);
}

auto BufferPoolManager::FlushPgInternal(page_id_t page_id) -> bool {
  // find page from buffer pool
  frame_id_t frame_id;
  if (page_table_.find(page_id) == page_table_.end()) {
    return false;
  }
  frame_id = page_table_.find(page_id)->second;

  if (pages_[frame_id].IsDirty()) {
    auto promise = disk_scheduler_->CreatePromise();
    auto future = promise.get_future();

    disk_scheduler_->Schedule({/*is_write=*/true, pages_[frame_id].GetData(), /*page_id=*/page_id, std::move(promise)});

    future.get();
  }

  if (pages_[frame_id].pin_count_ > 0) {
    pages_[frame_id].is_dirty_ = false;
    return true;
  }

  // clean up in-mem page if pin_count_ == 0
  pages_[frame_id].ResetMemory();
  pages_[frame_id].page_id_ = INVALID_PAGE_ID;
  pages_[frame_id].is_dirty_ = false;
  pages_[frame_id].pin_count_ = 0;

  free_list_.emplace_back(frame_id);
  replacer_->Remove(frame_id);
  page_table_.erase(page_id);
  return true;
}

void BufferPoolManager::FlushAllPages() {
  for (frame_id_t i = 0; i < static_cast<int>(pool_size_); i++) {
    FlushPage(pages_[i].GetPageId());
  }
}

auto BufferPoolManager::DeletePage(page_id_t page_id) -> bool {
  std::lock_guard<std::shared_mutex> lock(latch_);
  // find page from buffer pool
  frame_id_t frame_id;
  if (page_table_.find(page_id) == page_table_.end()) {
    return true;
  }
  frame_id = page_table_.find(page_id)->second;

  // return false if pin_count_ > 0
  if (pages_[frame_id].pin_count_ > 0) {
    return false;
  }

  free_list_.emplace_back(frame_id);
  replacer_->Remove(frame_id);
  page_table_.erase(page_id);

  pages_[frame_id].ResetMemory();
  pages_[frame_id].page_id_ = INVALID_PAGE_ID;
  pages_[frame_id].is_dirty_ = false;
  pages_[frame_id].pin_count_ = 0;

  DeallocatePage(page_id);
  return true;
}

auto BufferPoolManager::AllocatePage() -> page_id_t { return next_page_id_++; }

auto BufferPoolManager::FetchPageBasic(page_id_t page_id) -> BasicPageGuard { return {this, FetchPage(page_id)}; }

auto BufferPoolManager::FetchPageRead(page_id_t page_id) -> ReadPageGuard {
  auto page = FetchPage(page_id);
  if (page != nullptr) {
    page->RLatch();
  }
  return {this, page};
}

auto BufferPoolManager::FetchPageWrite(page_id_t page_id) -> WritePageGuard {
  auto page = FetchPage(page_id);
  if (page != nullptr) {
    page->WLatch();
  }
  return {this, page};
}

auto BufferPoolManager::NewPageGuarded(page_id_t *page_id) -> BasicPageGuard {
  auto page = NewPage(page_id);
  return {this, page};
}

}  // namespace bustub

//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// disk_scheduler.cpp
//
// Identification: src/storage/disk/disk_scheduler.cpp
//
// Copyright (c) 2015-2023, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "storage/disk/disk_scheduler.h"
#include "common/exception.h"
#include "storage/disk/disk_manager.h"

namespace bustub {

DiskScheduler::DiskScheduler(DiskManager *disk_manager, int num_workers)
    : disk_manager_(disk_manager), num_workers_(num_workers), request_queues_(num_workers_) {
  // Spawn the background threads
  for (size_t thread_id = 0; thread_id < num_workers_; thread_id++) {
    background_threads_.emplace_back([&, thread_id] { StartWorkerThread(thread_id); });
  }
}

DiskScheduler::~DiskScheduler() {
  // Put a `std::nullopt` in the queue to signal to exit the loop
  for (size_t thread_id = 0; thread_id < background_threads_.size(); thread_id++) {
    request_queues_[thread_id].Put(std::nullopt);
  }
  for (auto &thread : background_threads_) {
    if (thread.joinable()) {
      thread.join();
    }
  }
}

void DiskScheduler::Schedule(DiskRequest r) {
  page_id_t page_id = r.page_id_;
  request_queues_[page_id % num_workers_].Put(std::move(r));
}

void DiskScheduler::StartWorkerThread(size_t thread_id) {
  while (true) {
    auto request = request_queues_[thread_id].Get();

    // exit the loop
    if (!request.has_value()) {
      break;
    }

    auto page_data = request.value().data_;
    page_id_t page_id = request.value().page_id_;

    if (request.value().is_write_) {
      disk_manager_->WritePage(page_id, page_data);
    } else {
      disk_manager_->ReadPage(page_id, page_data);
    }

    request.value().callback_.set_value(true);
  }
}

}  // namespace bustub

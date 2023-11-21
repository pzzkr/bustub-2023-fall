//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lru_k_replacer.cpp
//
// Identification: src/buffer/lru_k_replacer.cpp
//
// Copyright (c) 2015-2022, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/lru_k_replacer.h"
#include <algorithm>
#include "common/exception.h"

namespace bustub {

LRUKReplacer::LRUKReplacer(size_t num_frames, size_t k) : replacer_size_(num_frames), k_(k) {}

auto LRUKReplacer::Evict(frame_id_t *frame_id) -> bool {
  std::lock_guard<std::mutex> lock(latch_);
  current_timestamp_++;

  if (node_store_.empty()) {
    return false;
  }

  *frame_id = -1;
  size_t max_k_distance = 0;
  std::vector<std::unique_ptr<LRUKNode>> inf_k_distance_nodes;
  for (auto &[fid, node] : node_store_) {
    if (evictable_.find(fid) == evictable_.end()) {
      continue;
    }
    size_t k_distance = current_timestamp_ - node.EarliestTimestamp();
    if (node.Size() < k_) {
      k_distance = std::numeric_limits<size_t>::max();
    }

    if (k_distance > max_k_distance) {
      *frame_id = fid;
      max_k_distance = k_distance;
    }

    if (k_distance == std::numeric_limits<size_t>::max()) {
      inf_k_distance_nodes.emplace_back(std::make_unique<LRUKNode>(node));
    }
  }

  if (!inf_k_distance_nodes.empty()) {
    *frame_id =
        std::min_element(inf_k_distance_nodes.begin(), inf_k_distance_nodes.end(),
                         [](const auto &a, const auto &b) { return a->EarliestTimestamp() < b->EarliestTimestamp(); })
            ->get()
            ->FrameId();
  }

  if (*frame_id == -1) {
    return false;
  }

  node_store_.erase(*frame_id);
  evictable_.erase(*frame_id);
  return true;
}

void LRUKReplacer::RecordAccess(frame_id_t frame_id, [[maybe_unused]] AccessType access_type) {
  std::lock_guard<std::mutex> lock(latch_);
  if (frame_id >= static_cast<int>(replacer_size_)) {
    throw std::invalid_argument{"invalid frame id"};
  }

  if (node_store_.find(frame_id) == node_store_.end()) {
    LRUKNode lru_node;
    lru_node.SetFid(frame_id);
    lru_node.SetK(k_);
    node_store_[frame_id] = std::move(lru_node);
  }

  node_store_[frame_id].RecordAccess(current_timestamp_++);
}

void LRUKReplacer::SetEvictable(frame_id_t frame_id, bool set_evictable) {
  std::lock_guard<std::mutex> lock(latch_);
  current_timestamp_++;

  if (set_evictable && node_store_.find(frame_id) == node_store_.end()) {
    return;
  }

  if (set_evictable) {
    evictable_[frame_id] = true;
  } else {
    evictable_.erase(frame_id);
  }
}

void LRUKReplacer::Remove(frame_id_t frame_id) {
  std::lock_guard<std::mutex> lock(latch_);
  current_timestamp_++;

  if (node_store_.find(frame_id) == node_store_.end()) {
    return;
  }

  if (evictable_.find(frame_id) == evictable_.end()) {
    throw std::invalid_argument{"invalid frame id"};
  }

  node_store_.erase(frame_id);
  evictable_.erase(frame_id);
}

auto LRUKReplacer::Size() -> size_t {
  std::lock_guard<std::mutex> lock(latch_);
  return evictable_.size();
}

}  // namespace bustub

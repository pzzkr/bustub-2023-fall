//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lru_k_replacer.h
//
// Identification: src/include/buffer/lru_k_replacer.h
//
// Copyright (c) 2015-2022, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <limits>
#include <list>
#include <mutex>  // NOLINT
#include <unordered_map>
#include <vector>

#include "common/config.h"
#include "common/macros.h"

namespace bustub {

enum class AccessType { Unknown = 0, Lookup, Scan, Index };

static auto AccessTypeScore(AccessType type) -> size_t {
  switch (type) {
    case (AccessType::Unknown):
    case (AccessType::Index):
      return 1;
    case (AccessType::Scan):
      return 2;
    case (AccessType::Lookup):
      return 3;
    default:
      return 1;
  }
}

class LRUKNode {
 public:
  auto FrameId() const -> size_t { return fid_; }

  void SetFid(frame_id_t fid) { fid_ = fid; }

  void SetK(size_t k) { k_ = k; }

  void RecordAccess(size_t timestamp, AccessType access_type = AccessType::Unknown) {
    size_t score = AccessTypeScore(access_type);
    history_.emplace_back(AccessRecord{timestamp, score});
    total_score_ += score;
    if (history_.size() > k_) {
      total_score_ -= history_.front().score_;
      history_.pop_front();
    }
  }

  auto EarliestTimestamp() const -> size_t { return history_.front().timestamp_; }

  auto TotalScore() const -> size_t { return total_score_; }

  auto Size() const -> size_t { return history_.size(); }

 private:
  struct AccessRecord {
    size_t timestamp_;
    size_t score_;
  };

  /** History of last seen K timestamps of this page. Least recent timestamp stored in front. */
  frame_id_t fid_;
  std::list<AccessRecord> history_;
  size_t k_;
  size_t total_score_{0};
};

/**
 * LRUKReplacer implements the LRU-k replacement policy.
 *
 * The LRU-k algorithm evicts a frame whose backward k-distance is maximum
 * of all frames. Backward k-distance is computed as the difference in time between
 * current timestamp and the timestamp of kth previous access.
 *
 * A frame with less than k historical references is given
 * +inf as its backward k-distance. When multiple frames have +inf backward k-distance,
 * classical LRU algorithm is used to choose victim.
 */
class LRUKReplacer {
 public:
  /**
   *
   * TODO(P1): Add implementation
   *
   * @brief a new LRUKReplacer.
   * @param num_frames the maximum number of frames the LRUReplacer will be required to store
   */
  explicit LRUKReplacer(size_t num_frames, size_t k);

  DISALLOW_COPY_AND_MOVE(LRUKReplacer);

  /**
   * TODO(P1): Add implementation
   *
   * @brief Destroys the LRUReplacer.
   */
  ~LRUKReplacer() = default;

  /**
   * TODO(P1): Add implementation
   *
   * @brief Find the frame with largest backward k-distance and evict that frame. Only frames
   * that are marked as 'evictable' are candidates for eviction.
   *
   * A frame with less than k historical references is given +inf as its backward k-distance.
   * If multiple frames have inf backward k-distance, then evict frame with earliest timestamp
   * based on LRU.
   *
   * Successful eviction of a frame should decrement the size of replacer and remove the frame's
   * access history.
   *
   * @param[out] frame_id id of frame that is evicted.
   * @return true if a frame is evicted successfully, false if no frames can be evicted.
   */
  auto Evict(frame_id_t *frame_id) -> bool;

  /**
   * TODO(P1): Add implementation
   *
   * @brief Record the event that the given frame id is accessed at current timestamp.
   * Create a new entry for access history if frame id has not been seen before.
   *
   * If frame id is invalid (ie. larger than replacer_size_), throw an exception. You can
   * also use BUSTUB_ASSERT to abort the process if frame id is invalid.
   *
   * @param frame_id id of frame that received a new access.
   * @param access_type type of access that was received. This parameter is only needed for
   * leaderboard tests.
   */
  void RecordAccess(frame_id_t frame_id, AccessType access_type = AccessType::Unknown);

  /**
   * TODO(P1): Add implementation
   *
   * @brief Toggle whether a frame is evictable or non-evictable. This function also
   * controls replacer's size. Note that size is equal to number of evictable entries.
   *
   * If a frame was previously evictable and is to be set to non-evictable, then size should
   * decrement. If a frame was previously non-evictable and is to be set to evictable,
   * then size should increment.
   *
   * If frame id is invalid, throw an exception or abort the process.
   *
   * For other scenarios, this function should terminate without modifying anything.
   *
   * @param frame_id id of frame whose 'evictable' status will be modified
   * @param set_evictable whether the given frame is evictable or not
   */
  void SetEvictable(frame_id_t frame_id, bool set_evictable);

  /**
   * TODO(P1): Add implementation
   *
   * @brief Remove an evictable frame from replacer, along with its access history.
   * This function should also decrement replacer's size if removal is successful.
   *
   * Note that this is different from evicting a frame, which always remove the frame
   * with largest backward k-distance. This function removes specified frame id,
   * no matter what its backward k-distance is.
   *
   * If Remove is called on a non-evictable frame, throw an exception or abort the
   * process.
   *
   * If specified frame is not found, directly return from this function.
   *
   * @param frame_id id of frame to be removed
   */
  void Remove(frame_id_t frame_id);

  /**
   * TODO(P1): Add implementation
   *
   * @brief Return replacer's size, which tracks the number of evictable frames.
   *
   * @return size_t
   */
  auto Size() -> size_t;

 private:
  auto GetKDistance(const LRUKNode &node) const -> size_t {
    size_t k_distance = current_timestamp_ - node.EarliestTimestamp();
    if (node.Size() < k_) {
      k_distance = INFINITE_K_DISTANCE;
    }
    return k_distance;
  }

  auto GetWeightedKDistance(const LRUKNode &node) const -> size_t {
    size_t k_distance = GetKDistance(node);
    return k_distance * node.TotalScore() / k_;
  }

  static constexpr size_t INFINITE_K_DISTANCE = std::numeric_limits<size_t>::max();
  std::unordered_map<frame_id_t, LRUKNode> node_store_;
  std::unordered_map<frame_id_t, bool> evictable_;
  size_t current_timestamp_{0};
  size_t replacer_size_;
  size_t k_;
  std::mutex latch_;
};

}  // namespace bustub

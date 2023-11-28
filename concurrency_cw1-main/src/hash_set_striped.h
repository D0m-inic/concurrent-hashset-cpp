#ifndef HASH_SET_STRIPED_H
#define HASH_SET_STRIPED_H

#include <atomic>
#include <cassert>
#include <functional>
#include <iostream>
#include <memory>
#include <mutex>
#include <vector>

#include "src/hash_set_base.h"

template <typename T>
class HashSetStriped : public HashSetBase<T> {
 public:
  explicit HashSetStriped(size_t initial_capacity)
      : capacity_(initial_capacity),
        num_locks(initial_capacity),
        table_(initial_capacity),
        locks_(initial_capacity) {}

  bool Add(T elem) final {
    // Get the mutex at the element's hash modded by the number of locks.
    std::mutex& bucket_mutex = locks_[std::hash<T>()(elem) % num_locks];
    {
      // The scoped lock is placed inside an inner scope so the mutex is no
      // longer locked when we enter resize
      std::scoped_lock<std::mutex> lock(bucket_mutex);
      if (Contains_(elem)) {
        return false;
      }
      size_t myBucket = std::hash<T>()(elem) % capacity_;
      auto& myVector = table_[myBucket];
      size_++;
      myVector.push_back(elem);
    }
    // Resizes the vector if the policy is met
    if (Policy()) {
      Resize();
    }
    return true;
  }

  bool Remove(T elem) final {
    // Get the mutex at the element's hash mod the number of locks.
    std::mutex& bucket_mutex = locks_[std::hash<T>()(elem) % num_locks];
    std::scoped_lock<std::mutex> lock(bucket_mutex);
    if (!Contains_(elem)) {
      return false;
    }
    size_t myBucket = std::hash<T>()(elem) % capacity_;
    auto& myVector = table_[myBucket];
    for (unsigned int i = 0; i < myVector.size(); i++) {
      if (myVector[i] == elem) {
        myVector.erase(std::next(myVector.begin(), i));
        size_--;
      }
    }
    return true;
  }

  [[nodiscard]] bool Contains(T elem) final {
    // Get the mutex at the element's hash mod the number of locks.
    std::mutex& bucket_mutex = locks_[std::hash<T>()(elem) % num_locks];
    std::scoped_lock<std::mutex> lock(bucket_mutex);
    return Contains_(elem);
  }

  [[nodiscard]] size_t Size() const final { return size_; }

 private:
 
  // Atomic because capacity is read by multiple threads concurrently
  std::atomic<size_t> capacity_;
  // Store initial capacity to avoid recomputing invariant length throughout
  size_t num_locks;
  // Atomic because changed by multple threads concurrently
  std::atomic<size_t> size_ = 0;
  std::vector<std::vector<T>> table_;
  // A vector of locks chosen since number of locks do not change and provides
  // easy access to locking/unlocking the relevant mutex
  std::vector<std::mutex> locks_;
  bool Contains_(T elem) {
    size_t myBucket = std::hash<T>()(elem) % capacity_;
    auto& myVector = table_[myBucket];
    for (size_t i = 0; i < myVector.size(); i++) {
      if (myVector[i] == elem) {
        return true;
      }
    }
    return false;
  }

  // Resize the vector if the average bucket size is greater than 4
  bool Policy() { return (size_ / capacity_) > 4; }

  void Resize() {
    size_t old_capacity = capacity_;

    // Lock all the mutexes that we have to ensure no other threads can access
    // the table whilst we are resizing. The scoped_locks are kept inside a
    // vector to keep them in scope until the resize is finished.
    std::vector<std::unique_ptr<std::scoped_lock<std::mutex>>> all_bucket_locks;
    for (auto& mutex : locks_) {
      all_bucket_locks.push_back(
          std::make_unique<std::scoped_lock<std::mutex>>(mutex));
    }
    if (old_capacity != table_.size()) {
      // Another thread has already resized, there is no need to resize again.
      return;
    }
    size_t newCapacity = capacity_ * 2;
    table_.resize(newCapacity);
    for (size_t i = 0; i < capacity_; i++) {
      auto& myVector = table_[i];
      size_t currSize = myVector.size();
      // For every element currently in a bucket, rehash it and push it to the
      // back of its new bucket.
      while (currSize-- > 0) {
        size_t newBucketHash = std::hash<T>()(myVector[0]) % newCapacity;
        table_[newBucketHash].push_back(myVector[0]);
        myVector.erase(std::next(myVector.begin(), 0));
      }
    }
    capacity_ = newCapacity;
  }
};

#endif  // HASH_SET_STRIPED_H

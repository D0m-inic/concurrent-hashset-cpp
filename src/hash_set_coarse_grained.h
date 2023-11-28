#ifndef HASH_SET_COARSE_GRAINED_H
#define HASH_SET_COARSE_GRAINED_H

#include <atomic>
#include <cassert>
#include <functional>
#include <iostream>
#include <mutex>
#include <vector>

#include "src/hash_set_base.h"

template <typename T>
class HashSetCoarseGrained : public HashSetBase<T> {
 public:
  explicit HashSetCoarseGrained(size_t initial_capacity)
      : capacity_(initial_capacity), table_(initial_capacity) {}

  bool Add(T elem) final {
    std::scoped_lock<std::mutex> lock(mutex_);
    if (Contains_(elem)) {
      return false;
    }
    size_t myBucket = std::hash<T>()(elem) % capacity_;
    auto& myVector = table_[myBucket];
    myVector.push_back(elem);
    size_++;
    if (Policy()) {
      Resize();
    }
    return true;
  }

  bool Remove(T elem) final {
    std::scoped_lock<std::mutex> lock(mutex_);
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
    std::scoped_lock<std::mutex> lock(mutex_);
    return Contains_(elem);
  }

  [[nodiscard]] size_t Size() const final { return size_; }

 private:
  // A global mutex is used by scoped_locks within Add, Contains, Remove
  // to ensure no other threads can access the table while a particular thread
  // is. We use scoped locks because there is no need for an explicit "unlock"
  // call.
  std::mutex mutex_;
  size_t capacity_;
  // Atomic to ensure safe reads from the "Size" function
  // Did not use the global mutex in "Size" as it is a const function.
  std::atomic<size_t> size_ = 0;
  std::vector<std::vector<T>> table_;

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

  // Called only from Add, where the scoped lock is held already and therefore
  // is held throughout the call to resize
  void Resize() {
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

#endif  // HASH_SET_COARSE_GRAINED_H

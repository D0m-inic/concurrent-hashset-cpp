#ifndef HASH_SET_REFINABLE_H
#define HASH_SET_REFINABLE_H

#include <atomic>
#include <cassert>
#include <deque>
#include <functional>
#include <iostream>
#include <memory>
#include <mutex>
#include <shared_mutex>
#include <vector>

#include "src/hash_set_base.h"

// Class which holds a bucket vector and a mutex. We group the mutexes with the
// bucket vector together in the LockableBucket class such that no list of locks
// are required for the implementation. This prevents the slowdown associated
// with the global lock which would be required to synchronise the list of
// locks.
template <typename T>
class LockableBucket {
 public:
  std::mutex& getMutex() { return mutex_; }

  std::vector<T>& getBucket() { return bucket_; }

 private:
  std::mutex mutex_;
  std::vector<T> bucket_;
};

template <typename T>
class HashSetRefinable : public HashSetBase<T> {
 public:
  explicit HashSetRefinable(size_t initial_capacity)
      : capacity_(initial_capacity), is_resizing_(false) {
    // Initialises the table_ field as a pointer to a vector containing pointers
    // to Lockable buckets. We used pointers to lockable buckets as opposed to
    // Buckets so that when the list is resized the bucket objects themselves
    // are not required to be copied over. We used a pointer to a vector instead
    // of a vector to allow the vector to be protected by an atomic variable,
    // and to allow the new table to be copied into the field variable cheaply
    // on resizing.
    table_ = new std::vector<std::unique_ptr<LockableBucket<T>>>();
    for (size_t i = 0; i < initial_capacity; i++) {
      table_.load()->push_back(std::make_unique<LockableBucket<T>>());
    }
  }

  bool Add(T elem) final {
    while (true) {
      // If a resize is currently happening, spin here until it is finished.
      while (is_resizing_.load()) {
      }
      size_t old_capacity = capacity_;
      // Accessing the reference of the lockable bucket in the table.
      auto& lockableBucket = *(*table_)[std::hash<T>()(elem) % capacity_];
      {
        // Acquires the lock on the given bucket before accessing it.
        std::unique_lock<std::mutex> lock(lockableBucket.getMutex());
        // Checks that the hashset has not been resized while acquiring the
        // lock.
        if (!is_resizing_.load() && old_capacity == capacity_.load()) {
          auto& bucket = lockableBucket.getBucket();
          for (size_t i = 0; i < bucket.size(); i++) {
            // If the element in question is already in the bucket, it cannot be
            // added.
            if (bucket[i] == elem) {
              return false;
            }
          }
          size_++;
          bucket.push_back(elem);

          // Lock released before a potential resize to avoid deadlock
          lock.unlock();

          // Checking if policy is true, then resizing.
          if (Policy()) {
            Resize();
          }
          return true;
        }
      }
    }
  }

  bool Remove(T elem) final {
    while (true) {
      while (is_resizing_.load()) {
      }
      size_t old_capacity = capacity_;
      auto& lockableBucket = *(*table_)[std::hash<T>()(elem) % capacity_];
      {
        std::scoped_lock<std::mutex> lock(lockableBucket.getMutex());
        if (!is_resizing_.load() && old_capacity == capacity_) {
          auto& bucket = lockableBucket.getBucket();
          for (unsigned int i = 0; i < bucket.size(); i++) {
            if (bucket[i] == elem) {
              // Removes element from bucket if found at the given index
              bucket.erase(std::next(bucket.begin(), i));
              size_--;
              return true;
            }
          }
          return false;
        }
      }
    }
  }

  [[nodiscard]] bool Contains(T elem) final {
    while (true) {
      while (is_resizing_.load()) {
      }
      size_t old_capacity = capacity_;
      auto& lockableBucket = *(*table_)[std::hash<T>()(elem) % capacity_];
      {
        std::scoped_lock<std::mutex> lock(lockableBucket.getMutex());
        if (!is_resizing_.load() && old_capacity == capacity_) {
          auto& bucket = lockableBucket.getBucket();
          for (unsigned int i = 0; i < bucket.size(); i++) {
            if (bucket[i] == elem) {
              // If element is in expected bucket, return true
              return true;
            }
          }
          return false;
        }
      }
    }
  }

  [[nodiscard]] size_t Size() const final { return size_; }

 private:
  // Atomic since accessed by multiple threads 
  std::atomic<size_t> capacity_;
  // Atomic since changed by multiple threads concurrently
  std::atomic<std::size_t> size_ = 0;
  std::atomic<std::vector<std::unique_ptr<LockableBucket<T>>>*> table_;
  // This bool is used to indicate when a resize operation is in place and prevent other threads from
  // accessing the table
  // The "owner" field from the Art of Multiprocessor Programming is not needed due to the non-reentrant
  // nature of the class (and lack of "initializeFrom") meaning resize can never call Add/Contains/Remove and
  // therefore a thread never needs to check if itself is resizing.
  std::atomic<bool> is_resizing_;
  
  bool Policy() { return (size_ / capacity_) > 4; }

  void Resize() {
    size_t old_capacity = capacity_;
    // If there was no resizing happening before, atomically set the value to
    // true stopping any other threads now entering the add, remove or contains
    // methods.
    if (!is_resizing_.exchange(true)) {
      if (old_capacity != capacity_.load()) {
        // Another thread got here first and resized already. No need to resize
        // again
        is_resizing_.store(false);
        return;
      }
      // Acquiring and releasing the locks for all the buckets before continuing the 
      // resize to prevent accesses to table_ 
      for (auto& lockableBucket : (*table_)) {
        lockableBucket.get()->getMutex().lock();
        lockableBucket.get()->getMutex().unlock();
      }

      size_t newCapacity = old_capacity * 2;
      std::atomic<std::vector<std::unique_ptr<LockableBucket<T>>>*> new_table;
      new_table = new std::vector<std::unique_ptr<LockableBucket<T>>>();

      // Creates new table and creates capacity*2 number of pointers in the new
      // table, doubling the size.
      for (size_t i = 0; i < newCapacity; i++) {
        (*new_table).push_back(std::make_unique<LockableBucket<T>>());
      }

      // Places the elements of the old buckets in the correct buckets by
      // rehashing with new capacity.
      for (size_t i = 0; i < capacity_; i++) {
        auto& lockableBucket = *(*table_)[i];
        auto& old_bucket = lockableBucket.getBucket();
        size_t currSize = old_bucket.size();
        while (currSize-- > 0) {
          size_t newBucketHash = std::hash<T>()(old_bucket[0]) % newCapacity;
          (*new_table)[newBucketHash].get()->getBucket().push_back(
              old_bucket[0]);
          old_bucket.erase(std::next(old_bucket.begin(), 0));
        }
      }
      // Replace the pointer to the old table with the pointer to the new
      // resized table 
      table_.store(new_table.load());
      capacity_ = newCapacity;
      // Sets atomic variable to false, indicating to waiting threads that other
      // methods (Add, Remove, Contains) can now be accessed again.
      is_resizing_.store(false);
    }
  }
};

#endif  // HASH_SET_REFINABLE_H

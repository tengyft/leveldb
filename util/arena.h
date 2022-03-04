// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifndef STORAGE_LEVELDB_UTIL_ARENA_H_
#define STORAGE_LEVELDB_UTIL_ARENA_H_

#include <atomic>
#include <cassert>
#include <cstddef>
#include <cstdint>
#include <vector>

namespace leveldb {

class Arena {
public:
    Arena();

    /// 禁止拷贝
    Arena(const Arena&) = delete;
    Arena& operator=(const Arena&) = delete;

    ~Arena();

    // Return a pointer to a newly allocated memory block of "bytes" bytes.
    char* Allocate(size_t bytes);

    // Allocate memory with the normal alignment guarantees provided by malloc.
    char* AllocateAligned(size_t bytes);

    // Returns an estimate of the total memory usage of data allocated
    // by the arena.
    size_t MemoryUsage() const { return memory_usage_.load(std::memory_order_relaxed); }

private:
    char* AllocateFallback(size_t bytes);
    char* AllocateNewBlock(size_t block_bytes);

private:
    // Allocation state
    char*  alloc_ptr_;             /// 指向的 block 是当前可以分配内存的 block
    size_t alloc_bytes_remaining_; /// 当前 block 还剩余多少个字节可以分配

    // Array of new[] allocated memory blocks
    std::vector<char*> blocks_; /// 当前 Area 对象已经申请分配的所有内存 block

    // Total memory usage of the arena.
    //
    // TODO(costan): This member is accessed via atomics, but the others are
    //               accessed without any locking. Is this OK?
    std::atomic<size_t> memory_usage_;
};

inline char* Arena::Allocate(size_t bytes) {
    // The semantics of what to return are a bit messy if we allow
    // 0-byte allocations, so we disallow them here (we don't need
    // them for our internal use).
    assert(bytes > 0);
    if (bytes <= alloc_bytes_remaining_) {
        /// 当前 block 剩余内存够用
        char* result = alloc_ptr_;
        alloc_ptr_ += bytes;
        alloc_bytes_remaining_ -= bytes;
        return result;
    }
    /// 当前 block 剩余内存不够用了
    return AllocateFallback(bytes);
}

} // namespace leveldb

#endif // STORAGE_LEVELDB_UTIL_ARENA_H_

// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "util/arena.h"

namespace leveldb {

static const int kBlockSize = 4096;

Arena::Arena() : alloc_ptr_(nullptr), alloc_bytes_remaining_(0), memory_usage_(0) {}

Arena::~Arena() {
    for (auto& block : blocks_) {
        delete[] block;
    }
}

/// 从系统内存中申请指定大小内存返回，或者从系统内存中申请一个 block，再从 block 中分出指定大小的内存返回。
char* Arena::AllocateFallback(size_t bytes) {
    if (bytes > kBlockSize / 4) {
        /// 新申请的内存大小如果比较大（大于 kBlockSize/4），就直接申请系统内存
        /// 默认大于 1k 字节的内存申请，直接从系统内存中申请。
        // Object is more than a quarter of our block size.  Allocate it separately
        // to avoid wasting too much space in leftover bytes.
        char* result = AllocateNewBlock(bytes);
        return result;
    }

    // We waste the remaining space in the current block.
    /// 当前供分配的 block 剩余空间不够 bytes(<= kBlockSize/4) 个字节，则直接抛弃剩余的空间，再申请一块新的内存。
    alloc_ptr_             = AllocateNewBlock(kBlockSize);
    alloc_bytes_remaining_ = kBlockSize;

    char* result = alloc_ptr_;
    alloc_ptr_ += bytes;
    alloc_bytes_remaining_ -= bytes;
    return result;
}

/// 按 8 字节对齐的方式申请内存并返回。
char* Arena::AllocateAligned(size_t bytes) {
    const int align = (sizeof(void*) > 8) ? sizeof(void*) : 8;
    static_assert((align & (align - 1)) == 0, "Pointer size should be a power of 2");
    size_t current_mod = reinterpret_cast<uintptr_t>(alloc_ptr_) & (align - 1);
    size_t slop        = (current_mod == 0 ? 0 : align - current_mod);
    size_t needed      = bytes + slop;
    char*  result;
    if (needed <= alloc_bytes_remaining_) {
        result = alloc_ptr_ + slop;
        alloc_ptr_ += needed;
        alloc_bytes_remaining_ -= needed;
    } else {
        // AllocateFallback always returned aligned memory
        result = AllocateFallback(bytes);
    }
    assert((reinterpret_cast<uintptr_t>(result) & (align - 1)) == 0);
    return result;
}

/// 申请一块新的内存
char* Arena::AllocateNewBlock(size_t block_bytes) {
    char* result = new char[block_bytes];
    blocks_.push_back(result);
    /// Arena 对象总的内存使用大小不仅包含新申请的内存，还包含指向这块新申请内存的指针。
    memory_usage_.fetch_add(block_bytes + sizeof(char*), std::memory_order_relaxed);
    return result;
}

} // namespace leveldb

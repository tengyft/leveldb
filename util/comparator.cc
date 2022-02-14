// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "leveldb/comparator.h"

#include <algorithm>
#include <cstdint>
#include <string>
#include <type_traits>

#include "leveldb/slice.h"
#include "util/logging.h"
#include "util/no_destructor.h"

namespace leveldb {

Comparator::~Comparator() = default;

namespace {
class BytewiseComparatorImpl : public Comparator {
public:
    BytewiseComparatorImpl() = default;

    const char* Name() const override { return "leveldb.BytewiseComparator"; }

    int Compare(const Slice& a, const Slice& b) const override { return a.compare(b); }

    void FindShortestSeparator(std::string* start, const Slice& limit) const override {
        // Find length of common prefix
        // 找到 *start 与 limit 的公共 prefix
        size_t min_length = std::min(start->size(), limit.size());
        size_t diff_index = 0;
        while ((diff_index < min_length) && ((*start)[diff_index] == limit[diff_index])) {
            diff_index++;
        }

        if (diff_index >= min_length) {
            // Do not shorten if one string is a prefix of the other
            // 如果 *start 是 limit 的 prefix，则直接返回 *start，即 *start 内容未做修改
        } else {
            // 找到第一个不是 prefix 的 byte
            auto diff_byte = static_cast<uint8_t>((*start)[diff_index]);
            // 只有当这个 diff_byte 范围在 [0, 0xff) 之间，并且 diff_byte + 1 < limit[diff_index] 时，才修改 *start，否则 *start 保持不变
            if (diff_byte < static_cast<uint8_t>(0xff) && diff_byte + 1 < static_cast<uint8_t>(limit[diff_index])) {
                (*start)[diff_index]++;
                start->resize(diff_index + 1);
                assert(Compare(*start, limit) < 0);
            }
        }
    }

    void FindShortSuccessor(std::string* key) const override {
        // Find first character that can be incremented
        size_t n = key->size();
        for (size_t i = 0; i < n; i++) {
            const uint8_t byte = (*key)[i];
            if (byte != static_cast<uint8_t>(0xff)) {
                (*key)[i] = byte + 1;
                key->resize(i + 1);
                return;
            }
        }
        // *key is a run of 0xffs.  Leave it alone.
    }
};
} // namespace

const Comparator* BytewiseComparator() {
    static NoDestructor<BytewiseComparatorImpl> singleton;
    return singleton.get();
}

} // namespace leveldb

// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "util/coding.h"

namespace leveldb {

/// 小端序，定长 32 位整数编码
void PutFixed32(std::string* dst, uint32_t value) {
    char buf[sizeof(value)];
    EncodeFixed32(buf, value);
    dst->append(buf, sizeof(buf));
}

/// 小端序，定长 64 位整数编码
void PutFixed64(std::string* dst, uint64_t value) {
    char buf[sizeof(value)];
    EncodeFixed64(buf, value);
    dst->append(buf, sizeof(buf));
}

/// 使用小端序对 32 位整数进行变长编码。将 v 对应的二进制按 7 位一组分组
char* EncodeVarint32(char* dst, uint32_t v) {
    // Operate on characters as unsigned
    auto*            ptr = reinterpret_cast<uint8_t*>(dst);
    static const int B   = 128;
    if (v < (1 << 7)) {
        /// v 取值范围为 [0, 2^7)，只有一组 7 位
        *(ptr++) = v; /// 第一组 7 位，第一个字节最高位为 0
    } else if (v < (1 << 14)) {
        /// v 取值范围为 [2^7, 2^14)，有两组 7 位
        *(ptr++) = v | B;  /// 第一组 7 位，第一个字节需要把最高位置 1，说明还有第二字节
        *(ptr++) = v >> 7; /// 第二组 7 位，第二字节最高位为 0
    } else if (v < (1 << 21)) {
        /// v 取值范围为 [2^14, 2^21)，有三组 7 位
        *(ptr++) = v | B;        /// 第一组 7 位，第一个字节需要把最高位置 1，说明还有第二字节
        *(ptr++) = (v >> 7) | B; /// 第二组 7 位，第二个字节需要把最高位置 1，说明还有第三字节
        *(ptr++) = v >> 14;      /// 第三组 7 位，第三字节最高位为 0
    } else if (v < (1 << 28)) {
        /// v 取值范围为 [2^21, 2^28)，有四组 7 位
        *(ptr++) = v | B;         /// 第一组 7 位，第一个字节需要把最高位置 1，说明还有第二字节
        *(ptr++) = (v >> 7) | B;  /// 第二组 7 位，第二个字节需要把最高位置 1，说明还有第三字节
        *(ptr++) = (v >> 14) | B; /// 第三组 7 位，第三个字节需要把最高位置 1，说明还有第四字节
        *(ptr++) = v >> 21;       /// 第四组 7 位，第四字节最高位为 0
    } else {
        /// v 取值范围为 [2^28, 2^32)，有五组 7 位
        *(ptr++) = v | B;         /// 第一组 7 位，第一个字节需要把最高位置 1，说明还有第二字节
        *(ptr++) = (v >> 7) | B;  /// 第二组 7 位，第二个字节需要把最高位置 1，说明还有第三字节
        *(ptr++) = (v >> 14) | B; /// 第三组 7 位，第三个字节需要把最高位置 1，说明还有第四字节
        *(ptr++) = (v >> 21) | B; /// 第四组 7 位，第四个字节需要把最高位置 1，说明还有第五字节
        *(ptr++) = v >> 28;       /// 第五组 7 位，第五字节最高位为 0
    }
    return reinterpret_cast<char*>(ptr);
}

/// 使用小端序将 v 变长编码到 dst 字符串中。
void PutVarint32(std::string* dst, uint32_t v) {
    char  buf[5]; /// 32 位整数变长编码时，最多需要 5 个字节。
    char* ptr = EncodeVarint32(buf, v);
    dst->append(buf, ptr - buf);
}

/// 使用小端序对 64 位整数进行变长编码。将 v 对应的二进制按 7 位一组分组
char* EncodeVarint64(char* dst, uint64_t v) {
    static const int B   = 128;
    auto*            ptr = reinterpret_cast<uint8_t*>(dst);
    while (v >= B) {
        *(ptr++) = v | B;
        v >>= 7;
    }
    *(ptr++) = static_cast<uint8_t>(v);
    return reinterpret_cast<char*>(ptr);
}

/// 使用小端序将 v 变长编码到 dst 字符串中。
void PutVarint64(std::string* dst, uint64_t v) {
    char  buf[10]; /// 64 位整数变长编码时，最多需要 10 个字节。
    char* ptr = EncodeVarint64(buf, v);
    dst->append(buf, ptr - buf);
}

/// 将 value 追加到 dst 字符串中。按规则：length(value)  data(value)
void PutLengthPrefixedSlice(std::string* dst, const Slice& value) {
    PutVarint32(dst, value.size());          /// length
    dst->append(value.data(), value.size()); /// data
}

int VarintLength(uint64_t v) {
    int len = 1;
    while (v >= 128) {
        v >>= 7;
        len++;
    }
    return len;
}

const char* GetVarint32PtrFallback(const char* p, const char* limit, uint32_t* value) {
    uint32_t result = 0;
    for (uint32_t shift = 0; shift <= 28 && p < limit; shift += 7) {
        uint32_t byte = *(reinterpret_cast<const uint8_t*>(p));
        p++;
        if (byte & 128) {
            // More bytes are present
            result |= ((byte & 127) << shift);
        } else {
            result |= (byte << shift);
            *value = result;
            return reinterpret_cast<const char*>(p);
        }
    }
    return nullptr;
}

bool GetVarint32(Slice* input, uint32_t* value) {
    const char* p     = input->data();
    const char* limit = p + input->size();
    const char* q     = GetVarint32Ptr(p, limit, value);
    if (q == nullptr) {
        return false;
    } else {
        *input = Slice(q, limit - q);
        return true;
    }
}

const char* GetVarint64Ptr(const char* p, const char* limit, uint64_t* value) {
    uint64_t result = 0;
    for (uint32_t shift = 0; shift <= 63 && p < limit; shift += 7) {
        uint64_t byte = *(reinterpret_cast<const uint8_t*>(p));
        p++;
        if (byte & 128) {
            // More bytes are present
            result |= ((byte & 127) << shift);
        } else {
            result |= (byte << shift);
            *value = result;
            return reinterpret_cast<const char*>(p);
        }
    }
    return nullptr;
}

bool GetVarint64(Slice* input, uint64_t* value) {
    const char* p     = input->data();
    const char* limit = p + input->size();
    const char* q     = GetVarint64Ptr(p, limit, value);
    if (q == nullptr) {
        return false;
    } else {
        *input = Slice(q, limit - q);
        return true;
    }
}

const char* GetLengthPrefixedSlice(const char* p, const char* limit, Slice* result) {
    uint32_t len;
    p = GetVarint32Ptr(p, limit, &len);
    if (p == nullptr)
        return nullptr;
    if (p + len > limit)
        return nullptr;
    *result = Slice(p, len);
    return p + len;
}

bool GetLengthPrefixedSlice(Slice* input, Slice* result) {
    uint32_t len;
    if (GetVarint32(input, &len) && input->size() >= len) {
        *result = Slice(input->data(), len);
        input->remove_prefix(len);
        return true;
    } else {
        return false;
    }
}

} // namespace leveldb

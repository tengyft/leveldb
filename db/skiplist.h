// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifndef STORAGE_LEVELDB_DB_SKIPLIST_H_
#define STORAGE_LEVELDB_DB_SKIPLIST_H_

// Thread safety
// -------------
//
// Writes require external synchronization, most likely a mutex.
// Reads require a guarantee that the SkipList will not be destroyed
// while the read is in progress.  Apart from that, reads progress
// without any internal locking or synchronization.
//
// Invariants:
//
// (1) Allocated nodes are never deleted until the SkipList is
// destroyed.  This is trivially guaranteed by the code since we
// never delete any skip list nodes.
//
// (2) The contents of a Node except for the next/prev pointers are
// immutable after the Node has been linked into the SkipList.
// Only Insert() modifies the list, and it is careful to initialize
// a node and use release-stores to publish the nodes in one or
// more lists.
//
// ... prev vs. next pointer ordering ...

#include <atomic>
#include <cassert>
#include <cstdlib>

#include "util/arena.h"
#include "util/random.h"

namespace leveldb {

class Arena;

/// SkipList 底层节点是有序递增的。
template <typename Key, class Comparator>
class SkipList {
private:
    struct Node;

public:
    // Create a new SkipList object that will use "cmp" for comparing keys,
    // and will allocate memory using "*arena".  Objects allocated in the arena
    // must remain allocated for the lifetime of the skiplist object.
    /// 构造一个 SkipList 对象，这个对象使用 Comparator(需要有 operator(A, B) 实现) 对象作为比较器。同时 SkipList 对象新插入的节点会分配在 Arena
    /// 中。 这就要求 SkipList 对象的生存期需要比传入的 arena 对象短，即 SkipList 对象需要 arena 对象之前销毁。
    explicit SkipList(Comparator cmp, Arena* arena);

    /// 禁止拷贝
    SkipList(const SkipList&) = delete;
    SkipList& operator=(const SkipList&) = delete;

    // Insert key into the list.
    // REQUIRES: nothing that compares equal to key is currently in the list.
    /// 向 SkipList 对象中插入新的 key。要求新插入的 key 不能与 SkipList 对象已有 key 相等。
    void Insert(const Key& key);

    // Returns true iff an entry that compares equal to key is in the list.
    /// 返回 SkipList 对象中是否有指定的 key。
    bool Contains(const Key& key) const;

    // Iteration over the contents of a skip list
    /// 辅助类：用于遍历绑定的 SkipList。
    class Iterator {
    public:
        // Initialize an iterator over the specified list.
        // The returned iterator is not valid.
        explicit Iterator(const SkipList* list);

        // Returns true iff the iterator is positioned at a valid node.
        /// 验证当前 Iterator 所指节点是否有效。node_ 不为 nullptr 就是有效的。
        bool Valid() const;

        // Returns the key at the current position.
        // REQUIRES: Valid()
        /// 返回值指定 Iterator 当前所指节点的 key。
        const Key& key() const;

        // Advances to the next position.
        // REQUIRES: Valid()
        /// 向后迭代，Iterator 指向下一个元素。
        void Next();

        // Advances to the previous position.
        // REQUIRES: Valid()
        /// 向前迭代，Iterator 指向上一个元素。代价较 Next() 高，因为需要先查询。
        void Prev();

        // Advance to the first entry with a key >= target
        /// 迭代当前节点到第一个 >= target 的节点。
        void Seek(const Key& target);

        // Position at the first entry in list.
        // Final state of iterator is Valid() iff list is not empty.
        /// 迭代当前节点为 SkipList 的第一个节点。
        void SeekToFirst();

        // Position at the last entry in list.
        // Final state of iterator is Valid() iff list is not empty.
        /// 迭代当前节点为 SkipList 的最后一个节点。
        void SeekToLast();

    private:
        const SkipList* list_; /// 绑定的 SkipList
        Node*           node_; /// 当前指定 SkipList 的哪个节点
        // Intentionally copyable
    };

private:
    /// SkipList 中一个 node 最多
    static constexpr int kMaxHeight = 12;
    // enum{ kMaxHeight = 12 };

    /// 返回当前 SkipList 对象的层高，即其所有节点中的最大层高。
    inline int GetMaxHeight() const { return max_height_.load(std::memory_order_relaxed); }

    /// 新建一个节点，内容由 key 指定，层高由 height 指定。新建节点内存由 arena_ 管理。
    Node* NewNode(const Key& key, int height);
    int   RandomHeight();
    bool  Equal(const Key& a, const Key& b) const { return (compare_(a, b) == 0); }

    // Return true if key is greater than the data stored in "n"
    /// 当 n 非空并且 key > n 时，返回 true。
    bool KeyIsAfterNode(const Key& key, Node* n) const;

    // Return the earliest node that comes at or after key.
    // Return nullptr if there is no such node.
    //
    // If prev is non-null, fills prev[level] with pointer to previous
    // node at "level" for every level in [0..max_height_-1].
    /// 返回第一个 >= key 的节点，并且如果 prev != 0 时，*prev 所指节点的指针域会被填充好，都是返回节点在各层的前驱节点。
    /// 如果 SkipList 没有节点，或者所有节点都 < key，则返回 nullptr。
    Node* FindGreaterOrEqual(const Key& key, Node** prev) const;

    // Return the latest node with a key < key.
    // Return head_ if there is no such node.
    /// 返回小于 key 的最大节点。如果没有符合条件的节点，则返回 head_。
    Node* FindLessThan(const Key& key) const;

    // Return the last node in the list.
    // Return head_ if list is empty.
    /// 返回 SkipList 最后一个节点，如果 SkipList 为空，则返回 head_ 节点。
    Node* FindLast() const;

private:
    // Immutable after construction
    /// 构造函数初始化后就不再变更。
    Comparator const compare_; /// 比较器，是一个函数对象
    Arena* const     arena_;   /// Arena used for allocations of nodes

    Node* const head_; /// node 的头节点，里面没有有效值，但是保存了 kMaxHeight 个指针域。

    // Modified only by Insert().  Read racily by readers, but stale
    // values are ok.
    /// SkipList 对象当前的最大高度。
    std::atomic<int> max_height_; // Height of the entire list

    // Read/written only by Insert().
    Random rnd_;
};

// Implementation details follow
/// SkipList 对象中一个节点的定义如下：
/// 要求 Key 是可以拷贝的。
template <typename Key, class Comparator>
struct SkipList<Key, Comparator>::Node {
    explicit Node(const Key& k) : key(k) {}

    /// SkipList 中 node 的值。
    Key const key;

    // Accessors/mutators for links.  Wrapped in methods so we can
    // add the appropriate barriers as necessary.
    /// 返回值指向当前 Node 第 n 层的后继 Node。
    Node* Next(int n) {
        assert(n >= 0);
        // Use an 'acquire load' so that we observe a fully initialized
        // version of the returned Node.
        return next_[n].load(std::memory_order_acquire);
    }
    /// 设置当前 Node 的第 n 层后继 Node。
    void SetNext(int n, Node* x) {
        assert(n >= 0);
        // Use a 'release store' so that anybody who reads through this
        // pointer observes a fully initialized version of the inserted node.
        next_[n].store(x, std::memory_order_release);
    }

    // No-barrier variants that can be safely used in a few locations.
    Node* NoBarrier_Next(int n) {
        assert(n >= 0);
        return next_[n].load(std::memory_order_relaxed);
    }
    void NoBarrier_SetNext(int n, Node* x) {
        assert(n >= 0);
        next_[n].store(x, std::memory_order_relaxed);
    }

private:
    // Array of length equal to the node height.  next_[0] is lowest level link.
    /// next_ 是一个动态数组，保证至少有一个元素。next_ 元素个数就是本 node 的高度。
    std::atomic<Node*> next_[1];
};

template <typename Key, class Comparator>
typename SkipList<Key, Comparator>::Node* SkipList<Key, Comparator>::NewNode(const Key& key, int height) {
    /// sizeof(Node) 中已经包含了一个 std::atomic<Node*> 对象的大小。
    char* const node_memory = arena_->AllocateAligned(sizeof(Node) + sizeof(std::atomic<Node*>) * (height - 1));
    return new (node_memory) Node(key);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

template <typename Key, class Comparator>
inline SkipList<Key, Comparator>::Iterator::Iterator(const SkipList* list) {
    list_ = list;
    node_ = nullptr;
}

template <typename Key, class Comparator>
inline bool SkipList<Key, Comparator>::Iterator::Valid() const {
    return node_ != nullptr;
}

template <typename Key, class Comparator>
inline const Key& SkipList<Key, Comparator>::Iterator::key() const {
    assert(Valid());
    return node_->key;
}

template <typename Key, class Comparator>
inline void SkipList<Key, Comparator>::Iterator::Next() {
    assert(Valid());
    node_ = node_->Next(0);
}

template <typename Key, class Comparator>
inline void SkipList<Key, Comparator>::Iterator::Prev() {
    // Instead of using explicit "prev" links, we just search for the
    // last node that falls before key.
    assert(Valid());
    node_ = list_->FindLessThan(node_->key);
    if (node_ == list_->head_) {
        node_ = nullptr;
    }
}

template <typename Key, class Comparator>
inline void SkipList<Key, Comparator>::Iterator::Seek(const Key& target) {
    node_ = list_->FindGreaterOrEqual(target, nullptr);
}

template <typename Key, class Comparator>
inline void SkipList<Key, Comparator>::Iterator::SeekToFirst() {
    node_ = list_->head_->Next(0);
}

template <typename Key, class Comparator>
inline void SkipList<Key, Comparator>::Iterator::SeekToLast() {
    node_ = list_->FindLast();
    if (node_ == list_->head_) {
        node_ = nullptr;
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// 为新插入的节点，计算出层高。
template <typename Key, class Comparator>
int SkipList<Key, Comparator>::RandomHeight() {
    // Increase height with probability 1 in kBranching
    /// 节点有 25% 的概率增加层高。
    static const unsigned int kBranching = 4;
    int                       height     = 1;
    while (height < kMaxHeight && ((rnd_.Next() % kBranching) == 0)) {
        height++;
    }
    assert(height > 0);
    assert(height <= kMaxHeight);
    return height;
}

/// 检测节点 n 是否 < key。即节点 n 是否排在 key 后面。
template <typename Key, class Comparator>
bool SkipList<Key, Comparator>::KeyIsAfterNode(const Key& key, Node* n) const {
    // null n is considered infinite
    return (n != nullptr) && (compare_(n->key, key) < 0);
}

template <typename Key, class Comparator>
typename SkipList<Key, Comparator>::Node* SkipList<Key, Comparator>::FindGreaterOrEqual(const Key& key, Node** prev) const {
    Node* x     = head_;
    int   level = GetMaxHeight() - 1;
    while (true) {
        Node* next = x->Next(level);
        if (KeyIsAfterNode(key, next)) {
            // Keep searching in this list
            /// key > next && next != nullptr
            x = next;
        } else {
            /// next == nullptr || key <= next
            if (prev != nullptr)
                prev[level] = x;
            if (level == 0) {
                return next;
            } else {
                // Switch to next list
                level--;
            }
        }
    }
}

template <typename Key, class Comparator>
typename SkipList<Key, Comparator>::Node* SkipList<Key, Comparator>::FindLessThan(const Key& key) const {
    Node* x     = head_;
    int   level = GetMaxHeight() - 1;
    while (true) {
        assert(x == head_ || compare_(x->key, key) < 0);
        Node* next = x->Next(level);
        if (next == nullptr || compare_(next->key, key) >= 0) {
            if (level == 0) {
                return x;
            } else {
                // Switch to next list
                level--;
            }
        } else {
            x = next;
        }
    }
}

template <typename Key, class Comparator>
typename SkipList<Key, Comparator>::Node* SkipList<Key, Comparator>::FindLast() const {
    Node* x     = head_;
    int   level = GetMaxHeight() - 1;
    while (true) {
        Node* next = x->Next(level);
        if (next == nullptr) {
            if (level == 0) {
                return x;
            } else {
                // Switch to next list
                level--;
            }
        } else {
            x = next;
        }
    }
}

template <typename Key, class Comparator>
SkipList<Key, Comparator>::SkipList(Comparator cmp, Arena* arena)
    : compare_(cmp), arena_(arena), head_(NewNode(0 /* any key will do */, kMaxHeight)), max_height_(1), rnd_(0xdeadbeef) {
    /// 头节点的所有指针域需要置空
    for (int i = 0; i < kMaxHeight; i++) {
        head_->SetNext(i, nullptr);
    }
}

template <typename Key, class Comparator>
void SkipList<Key, Comparator>::Insert(const Key& key) {
    // TODO(opt): We can use a barrier-free variant of FindGreaterOrEqual()
    // here since Insert() is externally synchronized.
    Node* prev[kMaxHeight];
    Node* x = FindGreaterOrEqual(key, prev);

    // Our data structure does not allow duplicate insertion
    assert(x == nullptr || !Equal(key, x->key));

    int height = RandomHeight();
    if (height > GetMaxHeight()) {
        /// 新插入节点，会更新 SkipList 层高，那么新增加的层也需要更新对应的前驱节点
        for (int i = GetMaxHeight(); i < height; i++) {
            prev[i] = head_;
        }
        // It is ok to mutate max_height_ without any synchronization
        // with concurrent readers.  A concurrent reader that observes
        // the new value of max_height_ will see either the old value of
        // new level pointers from head_ (nullptr), or a new value set in
        // the loop below.  In the former case the reader will
        // immediately drop to the next level since nullptr sorts after all
        // keys.  In the latter case the reader will use the new node.
        max_height_.store(height, std::memory_order_relaxed);
    }

    x = NewNode(key, height);
    for (int i = 0; i < height; i++) {
        // NoBarrier_SetNext() suffices since we will add a barrier when
        // we publish a pointer to "x" in prev[i].
        x->NoBarrier_SetNext(i, prev[i]->NoBarrier_Next(i));
        prev[i]->SetNext(i, x);
    }
}

template <typename Key, class Comparator>
bool SkipList<Key, Comparator>::Contains(const Key& key) const {
    Node* x = FindGreaterOrEqual(key, nullptr);
    if (x != nullptr && Equal(key, x->key)) {
        return true;
    } else {
        return false;
    }
}

} // namespace leveldb

#endif // STORAGE_LEVELDB_DB_SKIPLIST_H_

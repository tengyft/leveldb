// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "helpers/memenv/memenv.h"

#include <cstring>
#include <limits>
#include <map>
#include <string>
#include <vector>

#include "leveldb/env.h"
#include "leveldb/status.h"
#include "port/port.h"
#include "port/thread_annotations.h"
#include "util/mutexlock.h"

namespace leveldb {

namespace {

/// FileState 用于管理某个文件在内存中的数据，提供引用计数和 block 管理。
class FileState {
public:
    // FileStates are reference counted. The initial reference count is zero
    // and the caller must call Ref() at least once.
    /// FileState 是引用计数的。默认构造的 FileState 引用计数为 0，故构造者至少需要调用一次 Ref()。
    FileState() : refs_(0), size_(0) {}

    // No copying allowed.
    /// 禁止拷贝
    FileState(const FileState&) = delete;
    FileState& operator=(const FileState&) = delete;

    // Increase the reference count.
    /// 递增引用读数
    void Ref() {
        MutexLock lock(&refs_mutex_);
        ++refs_;
    }

    // Decrease the reference count. Delete if this is the last reference.
    /// 递减引用计数，最后一个引用需要析构自己。
    void Unref() {
        bool do_delete = false;

        {
            MutexLock lock(&refs_mutex_);
            --refs_;
            assert(refs_ >= 0);
            if (refs_ <= 0) {
                do_delete = true;
            }
        }

        if (do_delete) {
            delete this;
        }
    }

    /// 内存中共有多少个字节的数据。
    uint64_t Size() const {
        MutexLock lock(&blocks_mutex_);
        return size_;
    }

    /// 清空内存中管理的所有数据，并释放 block 占用的内存空间。
    void Truncate() {
        MutexLock lock(&blocks_mutex_);
        for (char*& block : blocks_) {
            delete[] block;
        }
        blocks_.clear();
        size_ = 0;
    }

    /// 从 offset 开始读 n 个字节，保存在 scratch 指向的内存中，*result 由 scratch 和 n 组成
    Status Read(uint64_t offset, size_t n, Slice* result, char* scratch) const {
        MutexLock lock(&blocks_mutex_);
        if (offset > size_) {
            return Status::IOError("Offset greater than file size.");
        }
        const uint64_t available = size_ - offset;
        if (n > available) {
            n = static_cast<size_t>(available);
        }
        if (n == 0) {
            *result = Slice();
            return Status::OK();
        }

        assert(offset / kBlockSize <= std::numeric_limits<size_t>::max());
        auto   block         = static_cast<size_t>(offset / kBlockSize); /// block 块号
        size_t block_offset  = offset % kBlockSize;                      /// block 块内偏移
        size_t bytes_to_copy = n;                                        /// 还需要 copy 的字节数
        char*  dst           = scratch;                                  /// 保存在哪

        while (bytes_to_copy > 0) {
            size_t avail = kBlockSize - block_offset; /// block 内还可以读多少个字节
            if (avail > bytes_to_copy) {
                avail = bytes_to_copy;
            }
            std::memcpy(dst, blocks_[block] + block_offset, avail);

            bytes_to_copy -= avail; /// 剩余多少个字节待读
            dst += avail;           /// 已经读取多少个字节，需要移动指针
            block++;                /// 如果还需要读的话，接着读下一个 block
            block_offset = 0;       /// 新 block 从 0 开始读
        }

        *result = Slice(scratch, n);
        return Status::OK();
    }

    /// 向 FileState 对象(其管理的内存)中追加数据。
    Status Append(const Slice& data) {
        const char* src     = data.data();
        size_t      src_len = data.size();

        MutexLock lock(&blocks_mutex_);
        while (src_len > 0) {
            size_t avail;
            size_t offset = size_ % kBlockSize; /// 块内偏移

            if (offset != 0) {
                // There is some room in the last block.
                /// blocks_ 中最后一个块还有空间可以追加数据。
                /// avail 表示这最后的块还有多少字节的空间可以追加数据。
                avail = kBlockSize - offset;
            } else {
                // No room in the last block; push new one.
                /// blocks_ 中管理的块都已经存满数据了，不能向已有块追加数据了，需要新一个块。
                blocks_.push_back(new char[kBlockSize]);
                avail = kBlockSize;
            }

            if (avail > src_len) {
                /// 保证不多写数据
                avail = src_len;
            }
            std::memcpy(blocks_.back() + offset, src, avail);
            src_len -= avail; /// 已经追加了 avail 个字节的数据，更新剩余多少个字节的数据需要追加
            src += avail;     /// 更新源数据指针，下次从哪开始读数据，追加到块中
            size_ += avail;   /// FileState 块中已经保存了多少个字节的数据
        }

        return Status::OK();
    }

private:
    const int kBlockSize = 8 * 1024;

    // Private since only Unref() should be used to delete it.
    /// 私有析构函数，所以只能通过 Unref() 来释放 FileState 资源
    ~FileState() { Truncate(); }

    port::Mutex refs_mutex_;             /// 引用锁
    int refs_   GUARDED_BY(refs_mutex_); /// 引用计数

    mutable port::Mutex        blocks_mutex_;             /// 块锁，用于同步块的读写操作
    std::vector<char*> blocks_ GUARDED_BY(blocks_mutex_); /// 保存内存中所有的 block 地址
    uint64_t size_ GUARDED_BY(blocks_mutex_); /// 内存中已经缓存了多少个字节的数据，这些数据保存在 blocks_ 管理的内存中
};

class SequentialFileImpl : public SequentialFile {
public:
    explicit SequentialFileImpl(FileState* file) : file_(file), pos_(0) { file_->Ref(); }

    ~SequentialFileImpl() override { file_->Unref(); }

    Status Read(size_t n, Slice* result, char* scratch) override {
        Status s = file_->Read(pos_, n, result, scratch);
        if (s.ok()) {
            pos_ += result->size();
        }
        return s;
    }

    Status Skip(uint64_t n) override {
        if (pos_ > file_->Size()) {
            return Status::IOError("pos_ > file_->Size()");
        }
        const uint64_t available = file_->Size() - pos_;
        if (n > available) {
            n = available;
        }
        pos_ += n;
        return Status::OK();
    }

private:
    FileState* file_;
    uint64_t   pos_;
};

class RandomAccessFileImpl : public RandomAccessFile {
public:
    explicit RandomAccessFileImpl(FileState* file) : file_(file) { file_->Ref(); }

    ~RandomAccessFileImpl() override { file_->Unref(); }

    Status Read(uint64_t offset, size_t n, Slice* result, char* scratch) const override { return file_->Read(offset, n, result, scratch); }

private:
    FileState* file_;
};

class WritableFileImpl : public WritableFile {
public:
    WritableFileImpl(FileState* file) : file_(file) { file_->Ref(); }

    ~WritableFileImpl() override { file_->Unref(); }

    Status Append(const Slice& data) override { return file_->Append(data); }

    Status Close() override { return Status::OK(); }
    Status Flush() override { return Status::OK(); }
    Status Sync() override { return Status::OK(); }

private:
    FileState* file_;
};

class NoOpLogger : public Logger {
public:
    void Logv(const char* format, std::va_list ap) override {}
};

class InMemoryEnv : public EnvWrapper {
public:
    explicit InMemoryEnv(Env* base_env) : EnvWrapper(base_env) {}

    ~InMemoryEnv() override {
        for (const auto& kvp : file_map_) {
            kvp.second->Unref();
        }
    }

    // Partial implementation of the Env interface.
    Status NewSequentialFile(const std::string& fname, SequentialFile** result) override {
        MutexLock lock(&mutex_);
        if (file_map_.find(fname) == file_map_.end()) {
            *result = nullptr;
            return Status::IOError(fname, "File not found");
        }

        *result = new SequentialFileImpl(file_map_[fname]);
        return Status::OK();
    }

    Status NewRandomAccessFile(const std::string& fname, RandomAccessFile** result) override {
        MutexLock lock(&mutex_);
        if (file_map_.find(fname) == file_map_.end()) {
            *result = nullptr;
            return Status::IOError(fname, "File not found");
        }

        *result = new RandomAccessFileImpl(file_map_[fname]);
        return Status::OK();
    }

    Status NewWritableFile(const std::string& fname, WritableFile** result) override {
        MutexLock            lock(&mutex_);
        FileSystem::iterator it = file_map_.find(fname);

        FileState* file;
        if (it == file_map_.end()) {
            // File is not currently open.
            file = new FileState();
            file->Ref();
            file_map_[fname] = file;
        } else {
            file = it->second;
            file->Truncate();
        }

        *result = new WritableFileImpl(file);
        return Status::OK();
    }

    Status NewAppendableFile(const std::string& fname, WritableFile** result) override {
        MutexLock   lock(&mutex_);
        FileState** sptr = &file_map_[fname];
        FileState*  file = *sptr;
        if (file == nullptr) {
            file = new FileState();
            file->Ref();
        }
        *result = new WritableFileImpl(file);
        return Status::OK();
    }

    bool FileExists(const std::string& fname) override {
        MutexLock lock(&mutex_);
        return file_map_.find(fname) != file_map_.end();
    }

    Status GetChildren(const std::string& dir, std::vector<std::string>* result) override {
        MutexLock lock(&mutex_);
        result->clear();

        for (const auto& kvp : file_map_) {
            const std::string& filename = kvp.first;

            if (filename.size() >= dir.size() + 1 && filename[dir.size()] == '/' && Slice(filename).starts_with(Slice(dir))) {
                result->push_back(filename.substr(dir.size() + 1));
            }
        }

        return Status::OK();
    }

    void RemoveFileInternal(const std::string& fname) EXCLUSIVE_LOCKS_REQUIRED(mutex_) {
        if (file_map_.find(fname) == file_map_.end()) {
            return;
        }

        file_map_[fname]->Unref();
        file_map_.erase(fname);
    }

    Status RemoveFile(const std::string& fname) override {
        MutexLock lock(&mutex_);
        if (file_map_.find(fname) == file_map_.end()) {
            return Status::IOError(fname, "File not found");
        }

        RemoveFileInternal(fname);
        return Status::OK();
    }

    Status CreateDir(const std::string& dirname) override { return Status::OK(); }

    Status RemoveDir(const std::string& dirname) override { return Status::OK(); }

    Status GetFileSize(const std::string& fname, uint64_t* file_size) override {
        MutexLock lock(&mutex_);
        if (file_map_.find(fname) == file_map_.end()) {
            return Status::IOError(fname, "File not found");
        }

        *file_size = file_map_[fname]->Size();
        return Status::OK();
    }

    Status RenameFile(const std::string& src, const std::string& target) override {
        MutexLock lock(&mutex_);
        if (file_map_.find(src) == file_map_.end()) {
            return Status::IOError(src, "File not found");
        }

        RemoveFileInternal(target);
        file_map_[target] = file_map_[src];
        file_map_.erase(src);
        return Status::OK();
    }

    Status LockFile(const std::string& fname, FileLock** lock) override {
        *lock = new FileLock;
        return Status::OK();
    }

    Status UnlockFile(FileLock* lock) override {
        delete lock;
        return Status::OK();
    }

    Status GetTestDirectory(std::string* path) override {
        *path = "/test";
        return Status::OK();
    }

    Status NewLogger(const std::string& fname, Logger** result) override {
        *result = new NoOpLogger;
        return Status::OK();
    }

private:
    // Map from filenames to FileState objects, representing a simple file system.
    /// key: 文件名  value: 对应的 FileState
    using FileSystem = std::map<std::string, FileState*>;

    port::Mutex          mutex_;
    FileSystem file_map_ GUARDED_BY(mutex_);
};

} // namespace

Env* NewMemEnv(Env* base_env) {
    return new InMemoryEnv(base_env);
}

} // namespace leveldb

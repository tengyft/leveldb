// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include <dirent.h>
#include <fcntl.h>
#include <pthread.h>
#include <sys/mman.h>
#include <sys/resource.h>
#include <sys/stat.h>
#include <sys/time.h>
#include <sys/types.h>
#include <unistd.h>

#include <atomic>
#include <cerrno>
#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <limits>
#include <queue>
#include <set>
#include <string>
#include <thread>
#include <type_traits>
#include <utility>

#include "leveldb/env.h"
#include "leveldb/slice.h"
#include "leveldb/status.h"
#include "port/port.h"
#include "port/thread_annotations.h"
#include "util/env_posix_test_helper.h"
#include "util/posix_logger.h"

namespace leveldb {

namespace {

// Set by EnvPosixTestHelper::SetReadOnlyMMapLimit() and MaxOpenFiles().
int g_open_read_only_file_limit = -1;

// Up to 1000 mmap regions for 64-bit binaries; none for 32-bit.
/// 64 位操作系统中默认可以开启的 mmap 数据限制为 1000。
constexpr const int kDefaultMmapLimit = (sizeof(void*) >= 8) ? 1000 : 0;

// Can be set using EnvPosixTestHelper::SetReadOnlyMMapLimit().
int g_mmap_limit = kDefaultMmapLimit;

// Common flags defined for all posix open operations
#if defined(HAVE_O_CLOEXEC)
constexpr const int kOpenBaseFlags = O_CLOEXEC;
#else
constexpr const int kOpenBaseFlags = 0;
#endif // defined(HAVE_O_CLOEXEC)

constexpr const size_t kWritableFileBufferSize = 65536; /// 64k

Status PosixError(const std::string& context, int error_number) {
    if (error_number == ENOENT) {
        return Status::NotFound(context, std::strerror(error_number));
    } else {
        return Status::IOError(context, std::strerror(error_number));
    }
}

// Helper class to limit resource usage to avoid exhaustion.
// Currently, used to limit read-only file descriptors and mmap file usage
// so that we do not run out of file descriptors or virtual memory, or run into
// kernel performance problems for very large databases.
/// Limiter 是一个用于避免资源耗尽的辅助类。当前 Limiter 仅用于限制只读文件描述符和 mmap 文件的使用。
class Limiter {
public:
    // Limit maximum number of resources to |max_acquires|.
    explicit Limiter(int max_acquires) : acquires_allowed_(max_acquires) {}

    /// 禁止拷贝
    Limiter(const Limiter&) = delete;
    Limiter operator=(const Limiter&) = delete;

    // If another resource is available, acquire it and return true.
    // Else return false.
    bool Acquire() {
        /// fetch_sub 会将 atomic 对象减 1，并返回老值
        int old_acquires_allowed = acquires_allowed_.fetch_sub(1, std::memory_order_relaxed);

        if (old_acquires_allowed > 0)
            return true;

        acquires_allowed_.fetch_add(1, std::memory_order_relaxed);
        return false;
    }

    // Release a resource acquired by a previous call to Acquire() that returned
    // true.
    void Release() { acquires_allowed_.fetch_add(1, std::memory_order_relaxed); }

private:
    // The number of available resources.
    //
    // This is a counter and is not tied to the invariants of any other class, so
    // it can be operated on safely using std::memory_order_relaxed.
    std::atomic<int> acquires_allowed_;
};

// Implements sequential read access in a file using read().
//
// Instances of this class are thread-friendly but not thread-safe, as required
// by the SequentialFile API.
/// PosixSequentialFile 通过 read() 实现了顺序读接口。通过查看代码可知 fd 是阻塞读。
class PosixSequentialFile final : public SequentialFile {
public:
    PosixSequentialFile(std::string filename, int fd) : fd_(fd), filename_(std::move(filename)) {}
    ~PosixSequentialFile() override { close(fd_); }

    /// 从文件中读取至多 n 个字节到 scratch 中，result 底层指向的内存也是 scratch。
    Status Read(size_t n, Slice* result, char* scratch) override {
        Status status;
        while (true) {
            ::ssize_t read_size = ::read(fd_, scratch, n);
            if (read_size < 0) { // Read error.
                if (errno == EINTR) {
                    continue; // Retry
                }
                status = PosixError(filename_, errno);
                break;
            }
            *result = Slice(scratch, read_size);
            break;
        }
        return status;
    }

    /// 从当前位置跳过 n 个字节，即设置文件新的读指针。
    Status Skip(uint64_t n) override {
        if (::lseek(fd_, n, SEEK_CUR) == static_cast<off_t>(-1)) {
            return PosixError(filename_, errno);
        }
        return Status::OK();
    }

private:
    const int         fd_;       /// 对应的文件描述符
    const std::string filename_; /// 文件名，可以是相对路径名，也可以是绝对路径名
};

// Implements random read access in a file using pread().
//
// Instances of this class are thread-safe, as required by the RandomAccessFile
// API. Instances are immutable and Read() only calls thread-safe library
// functions.
class PosixRandomAccessFile final : public RandomAccessFile {
public:
    // The new instance takes ownership of |fd|. |fd_limiter| must outlive this
    // instance, and will be used to determine if .
    PosixRandomAccessFile(std::string filename, int fd, Limiter* fd_limiter)
        : has_permanent_fd_(fd_limiter->Acquire()), fd_(has_permanent_fd_ ? fd : -1), fd_limiter_(fd_limiter),
          filename_(std::move(filename)) {
        if (!has_permanent_fd_) {
            /// 由于打开的可读文件数量限制，没有申请权限，直接关闭已经打开的文件描述符。
            assert(fd_ == -1);
            ::close(fd); // The file will be opened on every read.
        }
    }

    ~PosixRandomAccessFile() override {
        if (has_permanent_fd_) {
            assert(fd_ != -1);
            ::close(fd_);
            fd_limiter_->Release();
        }
    }

    Status Read(uint64_t offset, size_t n, Slice* result, char* scratch) const override {
        int fd = fd_;
        if (!has_permanent_fd_) {
            fd = ::open(filename_.c_str(), O_RDONLY | kOpenBaseFlags);
            if (fd < 0) {
                return PosixError(filename_, errno);
            }
        }

        assert(fd != -1);

        Status  status;
        ssize_t read_size = ::pread(fd, scratch, n, static_cast<off_t>(offset));
        *result           = Slice(scratch, (read_size < 0) ? 0 : read_size);
        if (read_size < 0) {
            // An error: return a non-ok status.
            status = PosixError(filename_, errno);
        }
        if (!has_permanent_fd_) {
            // Close the temporary file descriptor opened earlier.
            assert(fd != fd_);
            ::close(fd);
        }
        return status;
    }

private:
    const bool        has_permanent_fd_; // If false, the file is opened on every read.
    const int         fd_;               // -1 if has_permanent_fd_ is false.
    Limiter* const    fd_limiter_;
    const std::string filename_;
};

// Implements random read access in a file using mmap().
//
// Instances of this class are thread-safe, as required by the RandomAccessFile
// API. Instances are immutable and Read() only calls thread-safe library
// functions.
/// 使用 mmap() 来实现对文件的随机读。
/// PosixMmapReadableFile 实例是线程安全的。
class PosixMmapReadableFile final : public RandomAccessFile {
public:
    // mmap_base[0, length-1] points to the memory-mapped contents of the file. It
    // must be the result of a successful call to mmap(). This instances takes
    // over the ownership of the region.
    //
    // |mmap_limiter| must outlive this instance. The caller must have already
    // acquired the right to use one mmap region, which will be released when this
    // instance is destroyed.
    /// mmap_base[0, length-1] 所代表的内存与文件一一映射。mmap_base 必须是 mmap() 调用成功后返回的内存地址。新建的 PosixMmapReadableFile
    /// 实例会接管这段映射的内存区域。PosixMmapReadableFile 中的 mmap_limiter_ 引用了外部的 Limiter，这个 Limiter 在这个 PosixMmapReadable
    /// 实例 destroy 之前不能销毁。并且在创建新的 PosixMmapReadableFile 实例之前，需要先从这个 Limiter 成功申请一个资源使用权限。
    PosixMmapReadableFile(std::string filename, char* mmap_base, size_t length, Limiter* mmap_limiter)
        : mmap_base_(mmap_base), length_(length), mmap_limiter_(mmap_limiter), filename_(std::move(filename)) {}

    ~PosixMmapReadableFile() override {
        /// PosixMmapReadableFile 实例销毁时，需要解除内存与文件的映射关系
        ::munmap(static_cast<void*>(mmap_base_), length_);
        /// 同时归还资源使用权。
        mmap_limiter_->Release();
    }

    Status Read(uint64_t offset, size_t n, Slice* result, char* scratch) const override {
        if (offset + n > length_) {
            /// 要求读取的数据超限了。
            *result = Slice();
            return PosixError(filename_, EINVAL);
        }

        *result = Slice(mmap_base_ + offset, n);
        return Status::OK();
    }

private:
    char* const       mmap_base_;    /// mmap 映射的起始地址
    const size_t      length_;       /// mmap 映射块大小
    Limiter* const    mmap_limiter_; /// 外部传入的 mmap 资源数据限制器，其生存期需要与本实例长
    const std::string filename_;     /// 打开了哪个文件
};

/// 管理 Posix 可写文件。
class PosixWritableFile final : public WritableFile {
public:
    PosixWritableFile(std::string filename, int fd)
        : pos_(0), fd_(fd), is_manifest_(IsManifest(filename)), filename_(std::move(filename)), dirname_(Dirname(filename_)) {}

    ~PosixWritableFile() override {
        if (fd_ >= 0) {
            // Ignoring any potential errors
            Close();
        }
    }

    /// 追加数据。
    Status Append(const Slice& data) override {
        size_t      write_size = data.size();
        const char* write_data = data.data();

        // Fit as much as possible into buffer.
        /// 尽量写在缓存中
        size_t copy_size = std::min(write_size, kWritableFileBufferSize - pos_);
        std::memcpy(buf_ + pos_, write_data, copy_size);
        write_data += copy_size;
        write_size -= copy_size;
        pos_ += copy_size;
        if (write_size == 0) {
            return Status::OK();
        }

        // Can't fit in buffer, so need to do at least one write.
        /// 缓存已经满了，但是还有额外的数据没有写完，所以至少需要一次缓存刷新操作。
        Status status = FlushBuffer();
        if (!status.ok()) {
            return status;
        }

        // Small writes go to buffer, large writes are written directly.
        /// 剩余的数据如果不足以填充满写缓存，就把数据写入缓存中；但是如果剩余的数据过多，就直接写入底层文件中。
        if (write_size < kWritableFileBufferSize) {
            std::memcpy(buf_, write_data, write_size);
            pos_ = write_size;
            return Status::OK();
        }
        return WriteUnbuffered(write_data, write_size);
    }

    /// 关闭文件。
    Status Close() override {
        /// 首先需要先把缓存刷进文件中。
        Status    status       = FlushBuffer();
        const int close_result = ::close(fd_);
        if (close_result < 0 && status.ok()) {
            status = PosixError(filename_, errno);
        }
        fd_ = -1;
        return status;
    }

    /// 把缓存内容刷进文件中。
    Status Flush() override { return FlushBuffer(); }

    /// 同步。等待写盘完成。
    Status Sync() override {
        // Ensure new files referred to by the manifest are in the filesystem.
        //
        // This needs to happen before the manifest file is flushed to disk, to
        // avoid crashing in a state where the manifest refers to files that are not
        // yet on disk.
        /// 确保清单中文件已经在文件系统中了。
        ///
        /// 这个操作需要在清单中文件被刷进硬盘之前调用，这是为了避免清单中文件还没有被刷进硬盘之前，系统崩溃。
        Status status = SyncDirIfManifest();
        if (!status.ok()) {
            return status;
        }

        /// 写缓存写入文件
        status = FlushBuffer();
        if (!status.ok()) {
            return status;
        }

        /// 文件的 cache (操作系统管理的) 也需要及时写入硬盘中。
        return SyncFd(fd_, filename_);
    }

private:
    /// 刷新缓存，将缓存的数据写入底层文件中。
    Status FlushBuffer() {
        Status status = WriteUnbuffered(buf_, pos_);
        pos_          = 0; /// 更新写指针
        return status;
    }

    /// 无缓存（用户空间中的缓存）写，本项目中 fd_ 都没有打开 O_NONBLOCK，即都是阻塞写。
    /// 将数据直接写入底层文件中。
    Status WriteUnbuffered(const char* data, size_t size) {
        while (size > 0) {
            ssize_t write_result = ::write(fd_, data, size);
            if (write_result < 0) {
                if (errno == EINTR) {
                    continue; // Retry
                }
                return PosixError(filename_, errno);
            }
            data += write_result;
            size -= write_result;
        }
        return Status::OK();
    }

    /// 如果文件是 MANIFEST 开头文件，则确保此文件所在的目录文件及时写到硬盘中。
    Status SyncDirIfManifest() {
        Status status;
        if (!is_manifest_) {
            return status;
        }

        int fd = ::open(dirname_.c_str(), O_RDONLY | kOpenBaseFlags);
        if (fd < 0) {
            status = PosixError(dirname_, errno);
        } else {
            status = SyncFd(fd, dirname_);
            ::close(fd);
        }
        return status;
    }

    // Ensures that all the caches associated with the given file descriptor's
    // data are flushed all the way to durable media, and can withstand power
    // failures.
    //
    // The path argument is only used to populate the description string in the
    // returned Status if an error occurs.
    /// 确定 fd 关联的文件（可能是普通文件，也可能是目录文件）的 cache 及时被刷进硬盘中。传入的 fd_path 仅是用做错误提示。
    static Status SyncFd(int fd, const std::string& fd_path) {
#if HAVE_FULLFSYNC
        // On macOS and iOS, fsync() doesn't guarantee durability past power
        // failures. fcntl(F_FULLFSYNC) is required for that purpose. Some
        // filesystems don't support fcntl(F_FULLFSYNC), and require a fallback to
        // fsync().
        if (::fcntl(fd, F_FULLFSYNC) == 0) {
            return Status::OK();
        }
#endif // HAVE_FULLFSYNC

#if HAVE_FDATASYNC
        bool sync_success = ::fdatasync(fd) == 0;
#else
        bool sync_success = ::fsync(fd) == 0;
#endif // HAVE_FDATASYNC

        if (sync_success) {
            return Status::OK();
        }
        return PosixError(fd_path, errno);
    }

    // Returns the directory name in a path pointing to a file.
    //
    // Returns "." if the path does not contain any directory separator.
    /// 返回文件所在的目录。
    static std::string Dirname(const std::string& filename) {
        std::string::size_type separator_pos = filename.rfind('/');
        if (separator_pos == std::string::npos) {
            return ".";
        }
        // The filename component should not contain a path separator. If it does,
        // the splitting was done incorrectly.
        assert(filename.find('/', separator_pos + 1) == std::string::npos);

        return filename.substr(0, separator_pos);
    }

    // Extracts the file name from a path pointing to a file.
    //
    // The returned Slice points to |filename|'s data buffer, so it is only valid
    // while |filename| is alive and unchanged.
    static Slice Basename(const std::string& filename) {
        std::string::size_type separator_pos = filename.rfind('/');
        if (separator_pos == std::string::npos) {
            return {filename};
        }
        // The filename component should not contain a path separator. If it does,
        // the splitting was done incorrectly.
        assert(filename.find('/', separator_pos + 1) == std::string::npos);

        return {filename.data() + separator_pos + 1, filename.length() - separator_pos - 1};
    }

    // True if the given file is a manifest file.
    /// 检测指定文件是否是一个 manifest 文件
    static bool IsManifest(const std::string& filename) { return Basename(filename).starts_with("MANIFEST"); }

private:
    // buf_[0, pos_ - 1] contains data to be written to fd_.
    char   buf_[kWritableFileBufferSize]; /// 写缓存，buf_[0, pos_ - 1] 是缓存的数据。
    size_t pos_ = 0;                      /// 写缓存指针
    int    fd_;                           /// 对应的底层文件

    const bool        is_manifest_; // True if the file's name starts with MANIFEST.
    const std::string filename_;    /// 要写的文件名(绝对路径或相对于当前路径的路径)
    const std::string dirname_;     // The directory of filename_.
};

int LockOrUnlock(int fd, bool lock) {
    errno = 0;
    struct ::flock file_lock_info;
    std::memset(&file_lock_info, 0, sizeof(file_lock_info));
    file_lock_info.l_type   = (lock ? F_WRLCK : F_UNLCK);
    file_lock_info.l_whence = SEEK_SET;
    file_lock_info.l_start  = 0;
    file_lock_info.l_len    = 0; // Lock/unlock entire file.
    return ::fcntl(fd, F_SETLK, &file_lock_info);
}

// Instances are thread-safe because they are immutable.
class PosixFileLock : public FileLock {
public:
    PosixFileLock(int fd, std::string filename) : fd_(fd), filename_(std::move(filename)) {}

    int                fd() const { return fd_; }
    const std::string& filename() const { return filename_; }

private:
    const int         fd_;       /// 文件描述符
    const std::string filename_; /// 文件名(绝对路径名或相对路径名)
};

// Tracks the files locked by PosixEnv::LockFile().
//
// We maintain a separate set instead of relying on fcntl(F_SETLK) because
// fcntl(F_SETLK) does not provide any protection against multiple uses from the
// same process.
//
// Instances are thread-safe because all member data is guarded by a mutex.
/// 通过 PosixEnv::LockFile() 锁定的文件，都会被保存在 PosixLockTable 中。
class PosixLockTable {
public:
    bool Insert(const std::string& fname) LOCKS_EXCLUDED(mu_) {
        mu_.Lock();
        bool succeeded = locked_files_.insert(fname).second;
        mu_.Unlock();
        return succeeded;
    }
    void Remove(const std::string& fname) LOCKS_EXCLUDED(mu_) {
        mu_.Lock();
        locked_files_.erase(fname);
        mu_.Unlock();
    }

private:
    port::Mutex                         mu_;
    std::set<std::string> locked_files_ GUARDED_BY(mu_);
};

/// PosixEnv 应该作为单例使用，即 PosixEnv 对象生存期为静态生存期。
class PosixEnv : public Env {
public:
    PosixEnv();
    ~PosixEnv() override {
        static const char msg[] = "PosixEnv singleton destroyed. Unsupported behavior!\n";
        std::fwrite(msg, 1, sizeof(msg), stderr);
        std::abort();
    }

    /// 新建顺序读文件
    Status NewSequentialFile(const std::string& filename, SequentialFile** result) override {
        int fd = ::open(filename.c_str(), O_RDONLY | kOpenBaseFlags);
        if (fd < 0) {
            *result = nullptr;
            return PosixError(filename, errno);
        }

        *result = new PosixSequentialFile(filename, fd);
        return Status::OK();
    }

    /// 新建随机读文件
    Status NewRandomAccessFile(const std::string& filename, RandomAccessFile** result) override {
        *result = nullptr;
        int fd  = ::open(filename.c_str(), O_RDONLY | kOpenBaseFlags);
        if (fd < 0) {
            return PosixError(filename, errno);
        }

        if (!mmap_limiter_.Acquire()) {
            /// 没有 mmap 使用资源了，则尝试创建 PosixRandomAccessFile 而不使用 PosixMmapReadableFile。
            /// 不过，如果尝试创建的 PosixRandomAccessFile 对象数量超限，会把前面打开的 fd 关闭，生成一个无效和 PosixRandomAccessFile 对象。
            *result = new PosixRandomAccessFile(filename, fd, &fd_limiter_);
            return Status::OK();
        }

        /// 成功申请一个 mmap 使用权限
        uint64_t file_size;
        Status   status = GetFileSize(filename, &file_size);
        if (status.ok()) {
            void* mmap_base = ::mmap(/*addr=*/nullptr, file_size, PROT_READ, MAP_SHARED, fd, 0);
            if (mmap_base != MAP_FAILED) {
                *result = new PosixMmapReadableFile(filename, reinterpret_cast<char*>(mmap_base), file_size, &mmap_limiter_);
            } else {
                status = PosixError(filename, errno);
            }
        }
        /// 不管 mmap 成功与否，fd 都可以关闭了。
        ::close(fd);
        if (!status.ok()) {
            /// mmap 不成功，或者 GetFileSize() 不成功都需要归还 mmap 使用权限。
            mmap_limiter_.Release();
        }
        return status;
    }

    /// 新建可写文件
    Status NewWritableFile(const std::string& filename, WritableFile** result) override {
        int fd = ::open(filename.c_str(), O_TRUNC | O_WRONLY | O_CREAT | kOpenBaseFlags, 0644);
        if (fd < 0) {
            *result = nullptr;
            return PosixError(filename, errno);
        }

        *result = new PosixWritableFile(filename, fd);
        return Status::OK();
    }

    /// 新建追加文件
    Status NewAppendableFile(const std::string& filename, WritableFile** result) override {
        int fd = ::open(filename.c_str(), O_APPEND | O_WRONLY | O_CREAT | kOpenBaseFlags, 0644);
        if (fd < 0) {
            *result = nullptr;
            return PosixError(filename, errno);
        }

        *result = new PosixWritableFile(filename, fd);
        return Status::OK();
    }

    /// 检测指定文件是否存在
    bool FileExists(const std::string& filename) override { return ::access(filename.c_str(), F_OK) == 0; }

    /// 获取指定目录下的所有文件名，并保存在 *result 中。
    Status GetChildren(const std::string& directory_path, std::vector<std::string>* result) override {
        result->clear();
        ::DIR* dir = ::opendir(directory_path.c_str());
        if (dir == nullptr) {
            return PosixError(directory_path, errno);
        }
        struct ::dirent* entry;
        while ((entry = ::readdir(dir)) != nullptr) {
            result->emplace_back(entry->d_name);
        }
        ::closedir(dir);
        return Status::OK();
    }

    /// 删除指定文件
    Status RemoveFile(const std::string& filename) override {
        if (::unlink(filename.c_str()) != 0) {
            return PosixError(filename, errno);
        }
        return Status::OK();
    }

    /// 创建目录
    Status CreateDir(const std::string& dirname) override {
        if (::mkdir(dirname.c_str(), 0755) != 0) {
            return PosixError(dirname, errno);
        }
        return Status::OK();
    }

    /// 删除指定目录，这个目录必须为空。
    Status RemoveDir(const std::string& dirname) override {
        if (::rmdir(dirname.c_str()) != 0) {
            return PosixError(dirname, errno);
        }
        return Status::OK();
    }

    /// 获取指定文件大小
    Status GetFileSize(const std::string& filename, uint64_t* size) override {
        struct ::stat file_stat;
        if (::stat(filename.c_str(), &file_stat) != 0) {
            *size = 0;
            return PosixError(filename, errno);
        }
        *size = file_stat.st_size;
        return Status::OK();
    }

    /// 文件重命名
    Status RenameFile(const std::string& from, const std::string& to) override {
        if (std::rename(from.c_str(), to.c_str()) != 0) {
            return PosixError(from, errno);
        }
        return Status::OK();
    }

    /// 文件锁
    Status LockFile(const std::string& filename, FileLock** lock) override {
        *lock = nullptr;

        int fd = ::open(filename.c_str(), O_RDWR | O_CREAT | kOpenBaseFlags, 0644);
        if (fd < 0) {
            return PosixError(filename, errno);
        }

        if (!locks_.Insert(filename)) {
            ::close(fd);
            return Status::IOError("lock " + filename, "already held by process");
        }

        if (LockOrUnlock(fd, true) == -1) {
            int lock_errno = errno;
            ::close(fd);
            locks_.Remove(filename);
            return PosixError("lock " + filename, lock_errno);
        }

        *lock = new PosixFileLock(fd, filename);
        return Status::OK();
    }

    /// 释放文件锁
    Status UnlockFile(FileLock* lock) override {
        auto* posix_file_lock = static_cast<PosixFileLock*>(lock);
        if (LockOrUnlock(posix_file_lock->fd(), false) == -1) {
            return PosixError("unlock " + posix_file_lock->filename(), errno);
        }
        locks_.Remove(posix_file_lock->filename());
        ::close(posix_file_lock->fd());
        delete posix_file_lock;
        return Status::OK();
    }

    /// 向后台线程发起一个任务，这个任务为 background_work_function(background_work_arg) 函数，由后台线程执行。
    void Schedule(void (*background_work_function)(void* background_work_arg), void* background_work_arg) override;

    /// 新建一个线程运行 thread_main(thread_main_arg)。
    void StartThread(void (*thread_main)(void* thread_main_arg), void* thread_main_arg) override {
        std::thread new_thread(thread_main, thread_main_arg);
        new_thread.detach();
    }

    /// 返回测试目录
    Status GetTestDirectory(std::string* result) override {
        const char* env = std::getenv("TEST_TMPDIR");
        if (env && env[0] != '\0') {
            *result = env;
        } else {
            char buf[100];
            std::snprintf(buf, sizeof(buf), "/tmp/leveldbtest-%d", static_cast<int>(::geteuid()));
            *result = buf;
        }

        // The CreateDir status is ignored because the directory may already exist.
        CreateDir(*result);

        return Status::OK();
    }

    /// 创建一个新的 logger，用于记录日志。
    Status NewLogger(const std::string& filename, Logger** result) override {
        int fd = ::open(filename.c_str(), O_APPEND | O_WRONLY | O_CREAT | kOpenBaseFlags, 0644);
        if (fd < 0) {
            *result = nullptr;
            return PosixError(filename, errno);
        }

        std::FILE* fp = ::fdopen(fd, "w");
        if (fp == nullptr) {
            ::close(fd);
            *result = nullptr;
            return PosixError(filename, errno);
        } else {
            *result = new PosixLogger(fp);
            return Status::OK();
        }
    }

    /// 返回当前时间，以微秒计。
    uint64_t NowMicros() override {
        static constexpr uint64_t kUsecondsPerSecond = 1000000;
        struct ::timeval          tv;
        ::gettimeofday(&tv, nullptr);
        return static_cast<uint64_t>(tv.tv_sec) * kUsecondsPerSecond + tv.tv_usec;
    }

    /// 调用线程睡眠 micros 微秒。
    void SleepForMicroseconds(int micros) override { std::this_thread::sleep_for(std::chrono::microseconds(micros)); }

private:
    /// 后台线程实际运行的逻辑：一直从任务队列中取出任务并执行，没有任务时，就在条件变量上 wait，并等待新任务唤醒。
    void BackgroundThreadMain();

    /// 后台线程的入口。
    static void BackgroundThreadEntryPoint(PosixEnv* env) { env->BackgroundThreadMain(); }

    // Stores the work item data in a Schedule() call.
    //
    // Instances are constructed on the thread calling Schedule() and used on the
    // background thread.
    //
    // This structure is thread-safe because it is immutable.
    /// BackgroundWorkItem 用于保存后台任务元数据({运行逻辑，输入参数})。
    /// BackgroundWorkItem 实例由 Schedule() 创建，并且这些实例会被传入 background_work_queue_ 中，供后台线程弹出执行。
    /// 所有的 BackgroundWorkItem 实例都是不可变的，即构建后，不应该再变化。
    struct BackgroundWorkItem {
        // clang-format off
        explicit BackgroundWorkItem(void (*function)(void* arg), void* arg)
            : function(function),
              arg(arg) {}
        // clang-format on

        void (*const function)(void*); /// 后台任务运行逻辑
        void* const arg;               /// 后台任务运行执行时，传入的参数
    };

    /// 互斥锁，用于保护三个变量：background_work_cv_、started_background_thread_、background_work_queue_
    port::Mutex                       background_work_mutex_;
    port::CondVar background_work_cv_ GUARDED_BY(background_work_mutex_); /// 条件变量，用于 background_work_queue_ 队列中
    bool started_background_thread_   GUARDED_BY(background_work_mutex_); /// 后台线程是否已经启动

    /// 后台管理线程使用的队列
    std::queue<BackgroundWorkItem> background_work_queue_ GUARDED_BY(background_work_mutex_);

    PosixLockTable locks_;        // Thread-safe. 用于记录已经创建使用的文件锁
    Limiter        mmap_limiter_; // Thread-safe. 用于限制 mmap 文件的数量
    Limiter        fd_limiter_;   // Thread-safe. 用于限制随机读文件的打开数量
};

// Return the maximum number of concurrent mmaps.
/// 返回可以同时打开的 mmap 数量。
int MaxMmaps() {
    return g_mmap_limit;
}

// Return the maximum number of read-only files to keep open.
/// 返回可以打开的只读文件数量
int MaxOpenFiles() {
    if (g_open_read_only_file_limit >= 0) {
        return g_open_read_only_file_limit;
    }
    struct ::rlimit rlim;
    if (::getrlimit(RLIMIT_NOFILE, &rlim)) {
        // getrlimit failed, fallback to hard-coded default.
        g_open_read_only_file_limit = 50;
    } else if (rlim.rlim_cur == RLIM_INFINITY) {
        g_open_read_only_file_limit = std::numeric_limits<int>::max();
    } else {
        // Allow use of 20% of available file descriptors for read-only files.
        g_open_read_only_file_limit = rlim.rlim_cur / 5;
    }
    return g_open_read_only_file_limit;
}

} // namespace

// clang-format off
PosixEnv::PosixEnv()
    : background_work_cv_(&background_work_mutex_),
      started_background_thread_(false),
      mmap_limiter_(MaxMmaps()),
      fd_limiter_(MaxOpenFiles()) {}
// clang-format on

void PosixEnv::Schedule(void (*background_work_function)(void* background_work_arg), void* background_work_arg) {
    background_work_mutex_.Lock();

    // Start the background thread, if we haven't done so already.
    /// 如果还没有启动后台管理线程，则需要先启动后台管理线程。
    if (!started_background_thread_) {
        started_background_thread_ = true;
        std::thread background_thread(PosixEnv::BackgroundThreadEntryPoint, this);
        background_thread.detach();
    }

    // If the queue is empty, the background thread may be waiting for work.
    /// 如果队列为空，则后台管理线程可能在等待新的任务唤醒。
    if (background_work_queue_.empty()) {
        background_work_cv_.Signal();
    }

    background_work_queue_.emplace(background_work_function, background_work_arg);
    background_work_mutex_.Unlock();
}

void PosixEnv::BackgroundThreadMain() {
    /// 后台线程运行逻辑：从队列中弹出一个任务，并执行
    while (true) {
        background_work_mutex_.Lock();

        // Wait until there is work to be done.
        while (background_work_queue_.empty()) {
            background_work_cv_.Wait();
        }

        assert(!background_work_queue_.empty());
        auto  background_work_function = background_work_queue_.front().function;
        void* background_work_arg      = background_work_queue_.front().arg;
        background_work_queue_.pop();

        background_work_mutex_.Unlock();
        background_work_function(background_work_arg);
    }
}

namespace {

// Wraps an Env instance whose destructor is never created.
//
// Intended usage:
//   using PlatformSingletonEnv = SingletonEnv<PlatformEnv>;
//   void ConfigurePosixEnv(int param) {
//     PlatformSingletonEnv::AssertEnvNotInitialized();
//     // set global configuration flags.
//   }
//   Env* Env::Default() {
//     static PlatformSingletonEnv default_env;
//     return default_env.env();
//   }
template <typename EnvType>
class SingletonEnv {
public:
    SingletonEnv() {
#if !defined(NDEBUG)
        env_initialized_.store(true, std::memory_order::memory_order_relaxed);
#endif // !defined(NDEBUG)
        static_assert(sizeof(env_storage_) >= sizeof(EnvType), "env_storage_ will not fit the Env");
        static_assert(alignof(decltype(env_storage_)) >= alignof(EnvType), "env_storage_ does not meet the Env's alignment needs");
        new (&env_storage_) EnvType();
    }
    ~SingletonEnv() = default;

    /// 禁止拷贝
    SingletonEnv(const SingletonEnv&) = delete;
    SingletonEnv& operator=(const SingletonEnv&) = delete;

    Env* env() { return reinterpret_cast<Env*>(&env_storage_); }

    static void AssertEnvNotInitialized() {
#if !defined(NDEBUG)
        assert(!env_initialized_.load(std::memory_order::memory_order_relaxed));
#endif // !defined(NDEBUG)
    }

private:
    typename std::aligned_storage<sizeof(EnvType), alignof(EnvType)>::type env_storage_;
#if !defined(NDEBUG)
    static std::atomic<bool> env_initialized_;
#endif // !defined(NDEBUG)
};

#if !defined(NDEBUG)
template <typename EnvType>
std::atomic<bool> SingletonEnv<EnvType>::env_initialized_;
#endif // !defined(NDEBUG)

using PosixDefaultEnv = SingletonEnv<PosixEnv>;

} // namespace

void EnvPosixTestHelper::SetReadOnlyFDLimit(int limit) {
    PosixDefaultEnv::AssertEnvNotInitialized();
    g_open_read_only_file_limit = limit;
}

void EnvPosixTestHelper::SetReadOnlyMMapLimit(int limit) {
    PosixDefaultEnv::AssertEnvNotInitialized();
    g_mmap_limit = limit;
}

Env* Env::Default() {
    static PosixDefaultEnv env_container;
    return env_container.env();
}

} // namespace leveldb

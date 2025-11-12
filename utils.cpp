// utils.cpp
// Handlers for the network filesystem server.
// This file is designed to be #included by server.cpp

#include <string>
#include <vector>
#include <iostream>
#include <cstring> // for memcpy, strlen
#include <sys/socket.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <sys/sendfile.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <fcntl.h>
#include <unistd.h>
#include <dirent.h>
#include <errno.h>
#include <sys/statvfs.h>
#include <endian.h> // htobe64 / be64toh
#include "rwlocks.cpp" // For RWLockManager

// Note: std:: is used explicitly to avoid "using namespace std;" in an included file.
using std::string;
using std::vector;

// Max chunk size for a single read/write operation, matching client
#define CHUNK_SIZE 131072

// Operation codes, must match client.cpp
enum Op : uint32_t
{
    OP_GETATTR = 1,
    OP_READDIR = 2,
    OP_OPEN = 3,
    OP_READ = 4,
    OP_WRITE = 5,
    OP_CREATE = 6,
    OP_UNLINK = 7,
    OP_MKDIR = 8,
    OP_RMDIR = 9,
    OP_TRUNCATE = 10,
    OP_UTIMENS = 11,
    OP_STATFS = 12,
    OP_RELEASE = 13,
    OP_WRITE_BATCH = 14
};

// -------------------------- Utilities & helpers --------------------------

// Reads exactly n bytes from fd into buf
static int readn(int fd, void *buf, size_t n)
{
    size_t left = n;
    char *p = (char *)buf;
    while (left)
    {
        ssize_t r = ::read(fd, p, left);
        if (r < 0)
        {
            if (errno == EINTR) continue;
            return -1; // Error
        }
        if (r == 0) return 0; // EOF
        left -= r;
        p += r;
    }
    return (int)n;
}

// Writes exactly n bytes from buf to fd
static int writen(int fd, const void *buf, size_t n)
{
    size_t left = n;
    const char *p = (const char *)buf;
    while (left)
    {
        ssize_t w = ::write(fd, p, left);
        if (w < 0)
        {
            if (errno == EINTR) continue;
            return -1; // Error
        }
        if (w == 0) return 0; // Should not happen
        left -= w;
        p += w;
    }
    return (int)n;
}

// Helper to join the server root with a client-supplied path.
static string joinpath(const string &root, const string &path)
{
    if (path.empty() || path == "/")
        return root;
    if (path[0] == '/')
        return root + path; // Path is absolute, append to root
    return root + "/" + path;
}

// ----------------------- Protocol Response Helpers -----------------------
//
// Protocol:
//   Response: [ 4-byte status_be | 4-byte dlen_be | dlen bytes of data ]
//
// status == 0 -> success
// status != 0 -> errno
//

// Sends an error response (status = errno, dlen = 0)
static void send_errno(int client, int eno)
{
    uint32_t status = htonl((uint32_t)eno);
    uint32_t zlen = htonl(0); // zero payload length
    writen(client, &status, sizeof(status));
    writen(client, &zlen, sizeof(zlen));
}

// Sends a success response (status = 0, dlen = dlen)
static void send_ok_with_data(int client, const void *data, uint32_t dlen)
{
    uint32_t status = htonl(0);
    uint32_t dlen_be = htonl(dlen);
    writen(client, &status, sizeof(status));
    writen(client, &dlen_be, sizeof(dlen_be));
    if (dlen && data)
        writen(client, data, dlen);
};

// ---------------------- 1) OP_GETATTR ------------------------------------
// Request: [ 4b pathlen | path ]
// Response: [ struct stat ]
int getattr_handler(int client, const string &root, const char *p, RWLockManager &lock_manager)
{
    uint32_t pathlen;
    memcpy(&pathlen, p, 4); p += 4;
    pathlen = ntohl(pathlen);
    
    string path(p, p + pathlen);
    string full = joinpath(root, path);

    std::shared_lock<std::shared_mutex> lock = lock_manager.acquire_read_lock(full);

    struct stat st;
    if (lstat(full.c_str(), &st) == -1)
    {
        send_errno(client, errno);
        return 1;
    }

    send_ok_with_data(client, &st, sizeof(st));
    return 0;
}

// ---------------------- 2) OP_READDIR ------------------------------------
// Request: [ 4b pathlen | path ]
// Response: [ blob of \0-terminated names ]
int readdir_handler(int client, const string &root, const char *p, RWLockManager &lock_manager)
{
    uint32_t pathlen;
    memcpy(&pathlen, p, 4); p += 4;
    pathlen = ntohl(pathlen);

    string path(p, p + pathlen);
    string full = joinpath(root, path);

    std::shared_lock<std::shared_mutex> lock = lock_manager.acquire_read_lock(full);
    
    DIR *d = opendir(full.c_str());
    if (!d)
    {
        send_errno(client, errno);
        return 1;
    }

    // Build an output buffer with directory entries separated by '\0'.
    string out;
    struct dirent *e;
    while ((e = readdir(d)))
    {
        if (strcmp(e->d_name, ".") == 0 || strcmp(e->d_name, "..") == 0)
            continue;
        out.append(e->d_name, strlen(e->d_name));
        out.push_back('\0');
    }
    closedir(d);

    send_ok_with_data(client, out.data(), (uint32_t)out.size());
    return 0;
}

// ---------------------- 3) OP_OPEN and 6) OP_CREATE ----------------------
// Request (OPEN):   [ 4b pathlen | path | 4b flags ]
// Request (CREATE): [ 4b pathlen | path | 4b flags | 4b mode ]
// Response: [ 8b server_fd ]
int open_create_handler(int client, const string &root, const char *p, int op, RWLockManager &lock_manager)
{
    uint32_t pathlen;
    memcpy(&pathlen, p, 4); p += 4;
    pathlen = ntohl(pathlen);

    string path(p, p + pathlen); p += pathlen;

    uint32_t flags;
    memcpy(&flags, p, 4); p += 4;
    flags = ntohl(flags);

    string full = joinpath(root, path);
    std::unique_lock<std::shared_mutex> lock = lock_manager.acquire_write_lock(full);
    int fd;

    if (op == OP_CREATE)
    {
        uint32_t mode;
        memcpy(&mode, p, 4); p += 4;
        mode = ntohl(mode);
        fd = open(full.c_str(), (int)flags | O_CREAT, (mode_t)mode);
    }
    else
    {
        fd = open(full.c_str(), (int)flags);
    }

    if (fd == -1)
    {
        send_errno(client, errno);
        return 1;
    }

    // Send the file descriptor to client as uint64
    uint64_t fdbe = htobe64((uint64_t)fd);
    send_ok_with_data(client, &fdbe, sizeof(fdbe));
    return 0;
}

// ---------------------- 4) OP_READ ---------------------------------------
// Request: [ 8b sfd | 8b offset | 4b size ]
// Response: [ data (up to size bytes) ]
// Note: The client sends one request per chunk, so we do one pread.
int read_handler(int client, const string &root, const char *p, RWLockManager &lock_manager)
{
    (void)root; // unused

    uint64_t sfd;
    memcpy(&sfd, p, 8); p += 8;
    sfd = be64toh(sfd);

    uint64_t off;
    memcpy(&off, p, 8); p += 8;
    off = be64toh(off);

    uint32_t size;
    memcpy(&size, p, 4); p += 4;
    size = ntohl(size);
    
    if (size == 0) {
        send_ok_with_data(client, nullptr, 0);
        return 0;
    }

    // Client chunks requests, but good to double-check
    // if (size > CHUNK_SIZE) {
    //     size = CHUNK_SIZE; 
    // }
    std::shared_lock<std::shared_mutex> lock = lock_manager.acquire_read_lock(p);

    vector<char> buf(size);
    ssize_t r = pread((int)sfd, buf.data(), size, (off_t)off);
    
    if (r < 0) {
        send_errno(client, errno);
        return 1;
    }

    // Send back the number of bytes *actually* read (r)
    // r can be 0 for EOF, which is a successful read.
    send_ok_with_data(client, buf.data(), (uint32_t)r);
    return 0;
}

// ---------------------- 5) OP_WRITE & 14) OP_WRITE_BATCH -----------------
// Request: [ 8b sfd | 8b offset | 4b size | data ]
// Response: [ 4b bytes_written ]
// Note: The client sends one request per chunk, so we do one pwrite.
int write_handler(int client, const string &root, const char *p, RWLockManager &lock_manager)
{
    (void)root; // unused

    uint64_t sfd;
    memcpy(&sfd, p, 8); p += 8;
    sfd = be64toh(sfd);

    uint64_t off;
    memcpy(&off, p, 8); p += 8;
    off = be64toh(off);

    uint32_t wsize;
    memcpy(&wsize, p, 4); p += 4;
    wsize = ntohl(wsize);
    
    // Data immediately follows the header
    const char *data = p; 

    // Client should chunk, but we double-check
    if (wsize > CHUNK_SIZE) { 
        wsize = CHUNK_SIZE;
    }
    std::unique_lock<std::shared_mutex> lock = lock_manager.acquire_write_lock(p);
    ssize_t w = pwrite((int)sfd, data, wsize, (off_t)off);
    
    if (w == -1) {
        send_errno(client, errno);
        return 1;
    }

    // Return the number of bytes *actually* written
    uint32_t wrote_be = htonl((uint32_t)w);
    send_ok_with_data(client, &wrote_be, sizeof(wrote_be));
    return 0;
}

// ---------------------- 7) OP_UNLINK -------------------------------------
// Request: [ 4b pathlen | path ]
// Response: [ (empty) ]
int unlink_handler(int client, const string &root, const char *p, RWLockManager &lock_manager)
{
    uint32_t pathlen;
    memcpy(&pathlen, p, 4); p += 4;
    pathlen = ntohl(pathlen);
    string path(p, p + pathlen);
    string full = joinpath(root, path);
    std::unique_lock<std::shared_mutex> lock = lock_manager.acquire_write_lock(full);

    if (unlink(full.c_str()) == -1)
    {
        send_errno(client, errno);
        return 1;
    }
    send_ok_with_data(client, nullptr, 0);
    return 0;
}

// ---------------------- 8) OP_MKDIR --------------------------------------
// Request: [ 4b pathlen | path | 4b mode ]
// Response: [ (empty) ]
int mkdir_handler(int client, const string &root, const char *p, RWLockManager &lock_manager)
{
    uint32_t pathlen;
    memcpy(&pathlen, p, 4); p += 4;
    pathlen = ntohl(pathlen);
    string path(p, p + pathlen); p += pathlen;

    uint32_t mode;
    memcpy(&mode, p, 4); p += 4;
    mode = ntohl(mode);

    string full = joinpath(root, path);
    std::unique_lock<std::shared_mutex> lock = lock_manager.acquire_write_lock(full);
    if (mkdir(full.c_str(), (mode_t)mode) == -1)
    {
        send_errno(client, errno);
        return 1;
    }
    send_ok_with_data(client, nullptr, 0);
    return 0;
}

// ---------------------- 9) OP_RMDIR --------------------------------------
// Request: [ 4b pathlen | path ]
// Response: [ (empty) ]
int rmdir_handler(int client, const string &root, const char *p, RWLockManager &lock_manager)
{
    uint32_t pathlen;
    memcpy(&pathlen, p, 4); p += 4;
    pathlen = ntohl(pathlen);
    string path(p, p + pathlen);
    string full = joinpath(root, path);
    std::unique_lock<std::shared_mutex> lock = lock_manager.acquire_write_lock(full);

    if (rmdir(full.c_str()) == -1)
    {
        send_errno(client, errno);
        return 1;
    }
    send_ok_with_data(client, nullptr, 0);
    return 0;
}

// --------------------- 10) OP_TRUNCATE -----------------------------------
// Request: [ 4b pathlen | path | 8b size ]
// Response: [ (empty) ]
int truncate_handler(int client, const string &root, const char *p, RWLockManager &lock_manager)
{
    uint32_t pathlen;
    memcpy(&pathlen, p, 4); p += 4;
    pathlen = ntohl(pathlen);
    string path(p, p + pathlen); p += pathlen;

    uint64_t size;
    memcpy(&size, p, 8); p += 8;
    size = be64toh(size);

    string full = joinpath(root, path);
    std::unique_lock<std::shared_mutex> lock = lock_manager.acquire_write_lock(full);
    if (truncate(full.c_str(), (off_t)size) == -1)
    {
        send_errno(client, errno);
        return 1;
    }
    send_ok_with_data(client, nullptr, 0);
    return 0;
}

// --------------------- 11) OP_UTIMENS ------------------------------------
// Request: [ 4b pathlen | path | 8b at_sec | 8b at_nsec | 8b mt_sec | 8b mt_nsec ]
// Response: [ (empty) ]
int utimens_handler(int client, const string &root, const char *p, RWLockManager &lock_manager)
{
    uint32_t pathlen;
    memcpy(&pathlen, p, 4); p += 4;
    pathlen = ntohl(pathlen);
    string path(p, p + pathlen); p += pathlen;

    uint64_t at_sec, at_nsec, mt_sec, mt_nsec;
    memcpy(&at_sec, p, 8); p += 8; at_sec = be64toh(at_sec);
    memcpy(&at_nsec, p, 8); p += 8; at_nsec = be64toh(at_nsec);
    memcpy(&mt_sec, p, 8); p += 8; mt_sec = be64toh(mt_sec);
    memcpy(&mt_nsec, p, 8); p += 8; mt_nsec = be64toh(mt_nsec);

    struct timespec times[2];
    times[0].tv_sec = (time_t)at_sec;
    times[0].tv_nsec = (long)at_nsec;
    times[1].tv_sec = (time_t)mt_sec;
    times[1].tv_nsec = (long)mt_nsec;

    string full = joinpath(root, path);
    std::unique_lock<std::shared_mutex> lock = lock_manager.acquire_write_lock(full);
    if (utimensat(AT_FDCWD, full.c_str(), times, AT_SYMLINK_NOFOLLOW) == -1)
    {
        send_errno(client, errno);
        return 1;
    }
    send_ok_with_data(client, nullptr, 0);
    return 0;
}

// --------------------- 12) OP_STATFS -------------------------------------
// Request: [ 4b pathlen | path ]
// Response: [ struct statvfs ]
int statfs_handler(int client, const string &root, const char *p, RWLockManager &lock_manager)
{
    uint32_t pathlen;
    memcpy(&pathlen, p, 4); p += 4;
    pathlen = ntohl(pathlen);
    string path(p, p + pathlen);
    string full = joinpath(root, path);

    std::shared_lock<std::shared_mutex> lock = lock_manager.acquire_read_lock(full);

    struct statvfs st;
    if (statvfs(full.c_str(), &st) == -1)
    {
        send_errno(client, errno);
        return 1;
    }
    send_ok_with_data(client, &st, sizeof(st));
    return 0;
}

// --------------------- 13) OP_RELEASE ------------------------------------
// Request: [ 8b sfd ]
// Response: [ (empty) ]
int release_handler(int client, const string &root, const char *p, RWLockManager &lock_manager)
{
    (void)root; // unused
    uint64_t sfd;

    std::shared_lock<std::shared_mutex> lock = lock_manager.acquire_read_lock(p);

    memcpy(&sfd, p, 8); p += 8;
    sfd = be64toh(sfd);
    
    close((int)sfd); // Close the file descriptor

    send_ok_with_data(client, nullptr, 0);
    return 0;
}
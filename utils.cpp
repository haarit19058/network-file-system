#include <bits/stdc++.h>
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
using namespace std;



#define CHUNK_SIZE 131072 

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

// -------------------------- Utilities & helpers (kept together) --------------------------
//
// These helpers are shared by the handlers below. They were present in your original
// file and are repeated here so the handler block is self-contained and easy to drop in.
//
// Notes on design:
//  - Network/IPC wire format uses fixed-size integers in network byte order (big-endian).
//    We consistently convert incoming integers (ntohl / be64toh) and convert outgoing
//    integers back to network order (htonl / htobe64) before sending.
//  - Error reporting: send_errno sends a non-zero "status" followed by a zero-length payload.
//  - Success: send_ok_with_data sends status==0 followed by a uint32 length and then 'len' bytes.
//
// These helpers are small but critical: incorrect handling here will corrupt the protocol.

static int readn(int fd, void *buf, size_t n)
{
    size_t left = n;
    char *p = (char *)buf;
    while (left)
    {
        ssize_t r = ::read(fd, p, left);
        if (r <= 0)
            return r; // 0 => EOF, -1 => error (errno set)
        left -= r;
        p += r;
    }
    return (int)n;
}

static int writen(int fd, const void *buf, size_t n)
{
    size_t left = n;
    const char *p = (const char *)buf;
    while (left)
    {
        ssize_t w = ::write(fd, p, left);
        if (w <= 0)
            return (int)w; // error
        left -= w;
        p += w;
    }
    return (int)n;
}

// Helper to join the server root with a client-supplied path. It avoids double slashes
// and treats "/" or empty path as referring to the root itself.
static string joinpath(const string &root, const string &path)
{
    if (path.empty() || path == "/")
        return root;
    if (path[0] == '/')
        return root + path;
    return root + "/" + path;
}

// ----------------------- send data and errors --------------------------------------
//
// Protocol convention used by the server:
//   Response format for all RPCs:
//     uint32_t status        (network byte order)
//     uint32_t payload_len   (network byte order)
//     [payload bytes]        (present only if payload_len > 0)
//
// status == 0 -> success, non-zero -> an errno value.
//
// send_errno:
//   Sends an error response containing the errno in the status field and a zero-length
//   payload. The client should interpret status != 0 as failure and translate the value
//   back to its host byte order when needed.

void send_errno(int client, int eno)
{
    uint32_t status = htonl((uint32_t)eno); // send errno in big-endian
    uint32_t zlen = htonl(0);               // zero payload length
    writen(client, &status, sizeof(status));
    writen(client, &zlen, sizeof(zlen));
}

// send_ok_with_data:
//   Sends status==0 plus a length and the data bytes. If dlen==0 we still send the two
//   headers (status and length) but no data payload. The client will read the length to
//   know whether to expect a payload.

void send_ok_with_data(int client, const void *data, uint32_t dlen)
{
    uint32_t status = htonl(0);
    uint32_t dlen_be = htonl(dlen);
    writen(client, &status, sizeof(status));
    writen(client, &dlen_be, sizeof(dlen_be));
    if (dlen && data)
        writen(client, data, dlen);
};

// ---------------------- 1) OP_GETATTR ----------------------------------------------
// Request layout (client -> server):
//   uint32_t pathlen   (network order)
//   char[pathlen] path bytes (no terminating zero necessarily)
//
// Behavior:
//   Resolve path (relative to server root), call lstat(), and return the packed
//   struct stat on success. On failure, return errno via send_errno.
// Notes:
//   - We return the raw struct stat bytes. This is okay because both ends are
//     expected to run on compatible POSIX platforms. If you ever need cross-platform
//     portability, serialize individual fields explicitly.

int getattr_handler(int client, const string &root, const char *p)
{
    // Read path length
    uint32_t pathlen;
    memcpy(&pathlen, p, 4);
    p += 4;
    pathlen = ntohl(pathlen);
    
    // Read path name
    string path(p, p + pathlen);
    string full = joinpath(root, path);

    // Add attribute information using lstat
    struct stat st;
    if (lstat(full.c_str(), &st) == -1)
    {
        // lstat failed; send errno back to client
        send_errno(client, errno);
        return 1;
    }

    // On success send struct stat bytes
    send_ok_with_data(client, &st, sizeof(st));
    return 0;
}

// ---------------------- 2) OP_READDIR ----------------------------------------------
// Request layout:
//   uint32_t pathlen
//   char[pathlen] path
//
// Behavior:
//   Open the directory and collect the entries. This implementation concatenates
//   the entry names separated by a '\0' byte and sends the whole blob back.
// Notes:
//   - For each entry we skip "." and "..".
//   - We do not return type information in the current blob; if required, you can
//     prefix each name with a type byte or send a separate array of types.

int readdir_handler(int client, const string &root, const char *p)
{
    // Read path length
    uint32_t pathlen;
    memcpy(&pathlen, p, 4);
    p += 4;
    pathlen = ntohl(pathlen);

    // Read path name
    string path(p, p + pathlen);
    string full = joinpath(root, path);

    DIR *d = opendir(full.c_str());
    if (!d)
    {
        send_errno(client, errno);
        return 1;
    }

    // Build an output buffer with directory entries separated by '\0'.
    // This is a C-friendly format: the client can strtok / iterate by scanning for zeros.
    string out;
    struct dirent *e;
    while ((e = readdir(d)))
    {
        // skip self and parent entries
        if (strcmp(e->d_name, ".") == 0 || strcmp(e->d_name, "..") == 0)
            continue;
        // append name with a terminating zero to allow easy parsing
        out.append(e->d_name, strlen(e->d_name));
        out.push_back('\0');
    }
    closedir(d);

    // send the blob (can be empty if directory is empty)
    send_ok_with_data(client, out.data(), (uint32_t)out.size());
    return 0;
}

// ---------------------- 3) OP_OPEN and 6) OP_CREATE -------------------------------
// Request layout for OPEN:
//   uint32_t pathlen
//   char[pathlen] path
//   uint32_t flags
//
// Request layout for CREATE:
//   uint32_t pathlen
//   char[pathlen] path
//   uint32_t flags
//   uint32_t mode
//
// Behavior:
//   Open the requested file with given flags (and create if OP_CREATE with mode).
//   On success we return the file descriptor as a uint64 (host fd encoded in big-endian).
// Notes:
//   - We return the raw integer file descriptor value. This is an implementation detail
//     that only works when server and client run in an environment where sharing an FD
//     number makes sense (e.g., a single-process client talking to this server). If you
//     ever want to support remote clients, you would need a mapping (server-side) from
//     virtual file handles to real descriptors.

int open_create_handler(int client, const string &root, const char *p, int op)
{
    // Read path length
    uint32_t pathlen;
    memcpy(&pathlen, p, 4);
    p += 4;
    pathlen = ntohl(pathlen);

    // Read path name
    string path(p, p + pathlen);
    p += pathlen;

    // Read flags
    int flags;
    memcpy(&flags, p, 4);
    p += 4;
    flags = ntohl(flags);

    string full = joinpath(root, path);
    int fd;

    if (op == OP_CREATE)
    {
        // For CREATE an additional mode field follows.
        int mode;
        memcpy(&mode, p, 4);
        p += 4;
        mode = ntohl(mode);

        // Use O_CREAT combined with flags, respecting the provided mode.
        fd = open(full.c_str(), flags | O_CREAT, mode);
    }
    else
    {
        // OPEN path: no mode supplied.
        fd = open(full.c_str(), flags);
    }

    if (fd == -1)
    {
        send_errno(client, errno);
        return 1;
    }

    // Send the file descriptor to client as uint64 in big-endian so its size is stable.
    uint64_t fdbe = htobe64((uint64_t)fd);
    send_ok_with_data(client, &fdbe, sizeof(fdbe));
    return 0;
}

// ---------------------- 4) OP_READ -------------------------------------------------
// Request layout:
//   uint64_t sfd      (big-endian) [server-side fd returned earlier]
//   uint64_t offset   (big-endian)
//   uint32_t size     (network order)
//
// Behavior:
//   Use pread() on the supplied server fd to read 'size' bytes at 'offset' and send
//   the raw bytes back to the client. On error, send errno.
// Notes:
//   - Using a 64-bit sfd is defensive: it keeps the protocol stable across platforms.
//   - Caller must ensure the sfd is valid on the server; stale or malicious sfd will
//     cause appropriate errors.

int read_handler(int client, const string &root, const char *p)
{
    (void)root; // unused

    uint64_t sfd;
    memcpy(&sfd, p, 8);
    p += 8;
    sfd = be64toh(sfd);

    uint64_t off;
    memcpy(&off, p, 8);
    p += 8;
    off = be64toh(off);

    uint32_t size;
    memcpy(&size, p, 4);
    p += 4;
    size = ntohl(size);

    char buf[CHUNK_SIZE];
    size_t total_read = 0;

    while (total_read < size) {
        size_t this_chunk = std::min((size_t)CHUNK_SIZE, (size_t)(size - total_read));
        ssize_t r = pread((int)sfd, buf, this_chunk, (off_t)(off + total_read));
        if (r < 0) {
            send_errno(client, errno);
            return 1;
        }
        if (r == 0)
            break; // EOF

        // send_ok_with_data adds 4-byte status (0) automatically
        send_ok_with_data(client, buf, (uint32_t)r);

        total_read += r;
        if ((size_t)r < this_chunk)
            break; // hit EOF early
    }

    return 0;
}
// ---------------------- 5) OP_WRITE ------------------------------------------------
// Request layout:
//   uint64_t sfd      (big-endian) [server-side fd returned earlier]
//   uint64_t offset   (big-endian)
//   uint32_t size
//   char[size] data
//
// Behavior:
//   Open the file for writing (O_WRONLY), then pwrite() the 'size' bytes at offset.
//   Respond with the number of bytes actually written (uint32) on success.
// Notes:
//   - This handler opens by path (not by sfd). An alternative design is to use
//     an sfd sent by the client (like read), but your original code uses path.
//   - pwrite returns number of bytes written; we convert that to uint32 in the response.
int write_handler(int client, const string &root, const char *p)
{
    (void)root;

    uint64_t sfd;
    memcpy(&sfd, p, 8);
    p += 8;
    sfd = be64toh(sfd);

    uint64_t off;
    memcpy(&off, p, 8);
    p += 8;
    off = be64toh(off);

    uint32_t wsize;
    memcpy(&wsize, p, 4);
    p += 4;
    wsize = ntohl(wsize);

    const char *data = p;
    size_t total_written = 0;

    while (total_written < wsize) {
        size_t this_chunk = std::min((size_t)CHUNK_SIZE, (size_t)(wsize - total_written));
        ssize_t w = pwrite((int)sfd, data + total_written, this_chunk, (off_t)(off + total_written));
        if (w == -1) {
            send_errno(client, errno);
            return 1;
        }
        total_written += w;
        if ((size_t)w < this_chunk)
            break;
    }

    uint32_t wrote_be = htonl((uint32_t)total_written);
    send_ok_with_data(client, &wrote_be, sizeof(wrote_be));
    return 0;
}

// ---------------------- 7) OP_UNLINK ----------------------------------------------
// Request layout:
//   uint32_t pathlen
//   char[pathlen] path
//
// Behavior:
//   Calls unlink() on the resolved path. On success return an empty success response.

int unlink_handler(int client, const string &root, const char *p)
{
    uint32_t pathlen;
    memcpy(&pathlen, p, 4);
    p += 4;
    pathlen = ntohl(pathlen);
    string path(p, p + pathlen);
    string full = joinpath(root, path);

    if (unlink(full.c_str()) == -1)
    {
        send_errno(client, errno);
        return 1;
    }

    send_ok_with_data(client, nullptr, 0);
    return 0;
}

// ---------------------- 8) OP_MKDIR -----------------------------------------------
// Request layout:
//   uint32_t pathlen
//   char[pathlen] path
//   uint32_t mode
//
// Behavior:
//   Calls mkdir(path, mode). On success return empty success response.
// Notes:
//   - mode is sent as 32-bit in network order (we convert with ntohl).

int mkdir_handler(int client, const string &root, const char *p)
{
    uint32_t pathlen;
    memcpy(&pathlen, p, 4);
    p += 4;
    pathlen = ntohl(pathlen);
    string path(p, p + pathlen);
    p += pathlen;

    int mode;
    memcpy(&mode, p, 4);
    p += 4;
    mode = ntohl(mode);

    string full = joinpath(root, path);
    if (mkdir(full.c_str(), mode) == -1)
    {
        send_errno(client, errno);
        return 1;
    }

    send_ok_with_data(client, nullptr, 0);
    return 0;
}

// ---------------------- 9) OP_RMDIR -----------------------------------------------
// Request layout:
//   uint32_t pathlen
//   char[pathlen] path
//
// Behavior:
//   Calls rmdir(path). On success send empty success response.

int rmdir_handler(int client, const string &root, const char *p)
{
    uint32_t pathlen;
    memcpy(&pathlen, p, 4);
    p += 4;
    pathlen = ntohl(pathlen);
    string path(p, p + pathlen);
    string full = joinpath(root, path);

    if (rmdir(full.c_str()) == -1)
    {
        send_errno(client, errno);
        return 1;
    }

    send_ok_with_data(client, nullptr, 0);
    return 0;
}

// --------------------- 10) OP_TRUNCATE --------------------------------------------
// Request layout:
//   uint32_t pathlen
//   char[pathlen] path
//   uint64_t size   (big-endian)
//
// Behavior:
//   Calls truncate(path, size). On success return empty success response.

int truncate_handler(int client, const string &root, const char *p)
{
    uint32_t pathlen;
    memcpy(&pathlen, p, 4);
    p += 4;
    pathlen = ntohl(pathlen);
    string path(p, p + pathlen);
    p += pathlen;

    uint64_t size;
    memcpy(&size, p, 8);
    p += 8;
    size = be64toh(size);

    string full = joinpath(root, path);
    if (truncate(full.c_str(), (off_t)size) == -1)
    {
        send_errno(client, errno);
        return 1;
    }

    send_ok_with_data(client, nullptr, 0);
    return 0;
}

// --------------------- 11) OP_UTIMENS ---------------------------------------------
// Request layout:
//   uint32_t pathlen
//   char[pathlen] path
//   uint64_t at_sec  (big-endian)
//   uint64_t at_nsec (big-endian)
//   uint64_t mt_sec  (big-endian)
//   uint64_t mt_nsec (big-endian)
//
// Behavior:
//   Calls utimensat(AT_FDCWD, path, times, AT_SYMLINK_NOFOLLOW).
//   The times are interpreted as seconds + nanoseconds for atime and mtime.

int utimens_handler(int client, const string &root, const char *p)
{
    uint32_t pathlen;
    memcpy(&pathlen, p, 4);
    p += 4;
    pathlen = ntohl(pathlen);
    string path(p, p + pathlen);
    p += pathlen;

    uint64_t at_sec, at_nsec, mt_sec, mt_nsec;
    memcpy(&at_sec, p, 8);
    p += 8;
    at_sec = be64toh(at_sec);
    memcpy(&at_nsec, p, 8);
    p += 8;
    at_nsec = be64toh(at_nsec);
    memcpy(&mt_sec, p, 8);
    p += 8;
    mt_sec = be64toh(mt_sec);
    memcpy(&mt_nsec, p, 8);
    p += 8;
    mt_nsec = be64toh(mt_nsec);

    struct timespec times[2];
    times[0].tv_sec = (time_t)at_sec;
    times[0].tv_nsec = (long)at_nsec;
    times[1].tv_sec = (time_t)mt_sec;
    times[1].tv_nsec = (long)mt_nsec;

    string full = joinpath(root, path);
    if (utimensat(AT_FDCWD, full.c_str(), times, AT_SYMLINK_NOFOLLOW) == -1)
    {
        send_errno(client, errno);
        return 1;
    }

    send_ok_with_data(client, nullptr, 0);
    return 0;
}

// --------------------- 12) OP_STATFS ----------------------------------------------
// Request layout:
//   uint32_t pathlen
//   char[pathlen] path
//
// Behavior:
//   Calls statvfs on the path and returns the struct statvfs bytes on success.

int statfs_handler(int client, const string &root, const char *p)
{
    uint32_t pathlen;
    memcpy(&pathlen, p, 4);
    p += 4;
    pathlen = ntohl(pathlen);
    string path(p, p + pathlen);
    string full = joinpath(root, path);

    struct statvfs st;
    if (statvfs(full.c_str(), &st) == -1)
    {
        send_errno(client, errno);
        return 1;
    }

    send_ok_with_data(client, &st, sizeof(st));
    return 0;
}

// --------------------- 13) OP_RELEASE ----------------------------------------------
// Request layout:
//   uint64_t sfd
//
// Behavior:
//   Release is a no-op in this implementation (we don't track per-client FDs here).
//   We simply acknowledge the release and return success. If your system tracked
//   open handles per-client, you'd close or decrement reference counts here.

int release_handler(int client, const string &root, const char *p)
{
    uint64_t sfd;

    memcpy(&sfd, p, 8);
    p += 8;

    sfd = be64toh(sfd);
    close((int)sfd);

    send_ok_with_data(client, nullptr, 0);
    return 0;
}

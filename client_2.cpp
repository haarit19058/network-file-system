// nfuse.cpp
// FUSE client that forwards filesystem operations to a remote server over a
// persistent TCP socket.
//
// Usage:
//   ./nfuse <mountpoint> <server-host> <server-port> [fuse-args...]
//
// Protocol summary (wire format):
//  - Client -> Server:
//      [4-byte BE length N] [N bytes payload]
//
//    Payload structure (examples):
//      GETATTR:  op(4) | path_len(4) | path(bytes)
//      READ:     op(4) | serverfd(8 BE) | offset(8 BE) | size(4 BE)
//      WRITE:    op(4) | serverfd(8 BE) | offset(8 BE) | size(4 BE) | data...
//      OPEN/CREATE: op(4) | path_len(4) | path | flags(4 BE) [| mode(4 BE) if create]
//
//  - Server -> Client (response format):
//      status(4) | data_len(4) | data...
//    But this client reads status and then a length and then that many bytes of
//    data. For convenience, send_frame_and_recv returns a vector where the first
//    4 bytes are the status (in host order) and the following bytes are the
//    returned data (if any).
//
// Thread-safety: socket operations are protected by a mutex so multiple FUSE
// callbacks won't interleave frames and corrupt the protocol.
#define FUSE_USE_VERSION 35

#include <fuse3/fuse.h>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <string>
#include <vector>
#include <mutex>
#include <sys/socket.h>
#include <netdb.h>
#include <unistd.h>
#include <arpa/inet.h>
#include <endian.h> // htobe64 / be64toh
#include <sys/stat.h>
#include <sys/statvfs.h>
#include <fcntl.h>
#include <errno.h>
#include <list>
#include <unordered_map>
#include <chrono>
#include <fstream>
#include <sstream>
#include <iostream>
#include <algorithm>

using namespace std;
#define CHUNK_SIZE 131072

// ----------------------------- Operation enum -----------------------------
// Numeric operation codes used in the protocol. Stored on-wire as 32-bit BE.
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
    OP_RELEASE = 13
};

// --------------------------- Global socket state ---------------------------
// Single persistent TCP socket shared by all FUSE callbacks.
// Access to `sockfd` must be guarded by `sock_mtx` to avoid interleaving
// frames (we write length+payload and then read a response).
static int sockfd = -1;
static mutex sock_mtx;

// ---------------------------- Caching parameters ---------------------------
static const size_t CACHE_BLOCK_SIZE = 4096;                 // 4KB blocks
static const size_t CACHE_CAPACITY_BYTES = 16 * 1024 * 1024; // 16 MB total cache

// ------------------------------- Cache types ------------------------------
struct CacheValue
{
    string data; // actual bytes
    size_t size; // data.size()
};
using LRUList = list<pair<string, CacheValue>>;

static mutex cache_mtx;
static LRUList lru_list;
static unordered_map<string, LRUList::iterator> cache_map;
static size_t cache_size_bytes = 0;

// Map serverfd -> path so we can invalidate by fd on release
static mutex fdpath_mtx;
static unordered_map<uint64_t, string> fd_to_path;

static void cache_evict_if_needed()
{
    while (cache_size_bytes > CACHE_CAPACITY_BYTES && !lru_list.empty())
    {
        auto it = prev(lru_list.end());
        size_t entry_size = it->second.size;
        cache_map.erase(it->first);
        cache_size_bytes -= entry_size;
        lru_list.pop_back();
    }
}

static void cache_put_block(const string &key, const char *data, size_t len)
{
    if (len > CACHE_CAPACITY_BYTES)
        return;
    lock_guard<mutex> lk(cache_mtx);

    auto it = cache_map.find(key);
    if (it != cache_map.end())
    {
        auto lit = it->second;
        cache_size_bytes -= lit->second.size;
        lit->second.data.assign(data, data + len);
        lit->second.size = len;
        cache_size_bytes += len;
        lru_list.splice(lru_list.begin(), lru_list, lit);
    }
    else
    {
        CacheValue cv;
        cv.data.assign(data, data + len);
        cv.size = len;
        lru_list.emplace_front(key, std::move(cv));
        cache_map[key] = lru_list.begin();
        cache_size_bytes += len;
    }

    cache_evict_if_needed();
}

// Try read block from cache. Returns true if present and copies into out_buf, sets out_len.
static bool cache_get_block(const string &key, char *out_buf, size_t &out_len)
{
    lock_guard<mutex> lk(cache_mtx);
    auto it = cache_map.find(key);
    if (it == cache_map.end())
        return false;
    lru_list.splice(lru_list.begin(), lru_list, it->second);
    const CacheValue &cv = it->second->second;
    memcpy(out_buf, cv.data.data(), cv.size);
    out_len = cv.size;
    return true;
}

// Remove all blocks for a full path (prefix matching path:)
static void cache_invalidate_path(const string &path)
{
    lock_guard<mutex> lk(cache_mtx);
    string prefix = path + ":";
    for (auto it = lru_list.begin(); it != lru_list.end();)
    {
        if (it->first.rfind(prefix, 0) == 0)
        {
            cache_size_bytes -= it->second.size;
            cache_map.erase(it->first);
            it = lru_list.erase(it);
        }
        else
            ++it;
    }
}

// Remove exactly one key
static void cache_erase_key(const string &key)
{
    lock_guard<mutex> lk(cache_mtx);
    auto it = cache_map.find(key);
    if (it == cache_map.end())
        return;
    cache_size_bytes -= it->second->second.size;
    lru_list.erase(it->second);
    cache_map.erase(it);
}

static string make_block_key(const string &path, uint64_t block_idx)
{
    return path + ":" + to_string(block_idx);
}

// Helper that prints errno and exits (simple fail-fast for setup errors).
static void die(const char *m)
{
    perror(m);
    exit(1);
}

// --------------------------- Networking helpers ---------------------------
// Create a TCP connection to `host:port`. Returns a socket FD (or exits on
// unrecoverable error). Uses getaddrinfo so IPv4/IPv6 both work.
static int connect_to_server(const char *host, const char *port)
{
    struct addrinfo hints{};
    struct addrinfo *res = nullptr;

    hints.ai_family = AF_UNSPEC;     // IPv4 or IPv6
    hints.ai_socktype = SOCK_STREAM; // TCP

    if (getaddrinfo(host, port, &hints, &res) != 0)
    {
        die("getaddrinfo");
    }

    int s = socket(res->ai_family, res->ai_socktype, res->ai_protocol);
    if (s < 0)
    {
        freeaddrinfo(res);
        die("socket");
    }

    if (connect(s, res->ai_addr, res->ai_addrlen) < 0)
    {
        freeaddrinfo(res);
        close(s);
        die("connect");
    }

    freeaddrinfo(res);
    return s;
}

// readn / writen: reliable helpers that loop until the requested number of
// bytes are read or written (or an error/EOF occurs).
// They return the number of bytes actually transferred (n) or -1 on error,
// or 0 on EOF (for readn).
static int readn(int fd, void *buf, size_t n)
{
    size_t left = n;
    char *p = static_cast<char *>(buf);

    while (left)
    {
        ssize_t r = ::read(fd, p, left);
        if (r < 0)
        {
            if (errno == EINTR)
                continue; // retry on interrupt
            return -1;
        }
        if (r == 0)
            return 0; // remote closed connection (EOF)
        left -= r;
        p += r;
    }
    return (int)n;
}

static int writen(int fd, const void *buf, size_t n)
{
    size_t left = n;
    const char *p = static_cast<const char *>(buf);

    while (left)
    {
        ssize_t w = ::write(fd, p, left);
        if (w < 0)
        {
            if (errno == EINTR)
                continue; // retry on interrupt
            return -1;
        }
        if (w == 0)
            return 0; // shouldn't normally happen for sockets
        left -= w;
        p += w;
    }
    return (int)n;
}

// send_frame_and_recv:
//  - Takes an already-constructed payload (where the payload is the bytes
//    that follow the 4-byte length header).
//  - It performs: write(4-byte BE length) then write(payload).
//  - Then it reads the response header which is: status(4) | dlen(4) | data...
//  - Returns 0 on success and fills out_status_and_data with:
//       [status (host-order 4 bytes)] [data bytes...]
//    If any socket operation fails, it returns -EIO (or other negative errno)
//    so callers can return appropriate FUSE error codes.
static int send_frame_and_recv(const void *payload, uint32_t payload_len, vector<char> &out_status_and_data)
{
    cout << "Sent" << endl;
    // The socket is shared between multiple possible FUSE callback threads,
    // so lock while composing/sending and reading the server response.
    unique_lock<mutex> lk(sock_mtx);

    // Frame length is 4-byte big-endian
    uint32_t len_be = htonl(payload_len);
    if (writen(sockfd, &len_be, sizeof(len_be)) != (int)sizeof(len_be))
    {
        return -EIO;
    }

    if (payload_len > 0)
    {
        if (writen(sockfd, payload, payload_len) != (int)payload_len)
        {
            return -EIO;
        }
    }

    // Read response status (4 bytes) and data length (4 bytes)
    uint32_t status_be;
    if (readn(sockfd, &status_be, sizeof(status_be)) != (int)sizeof(status_be))
    {
        return -EIO;
    }
    uint32_t status = ntohl(status_be);

    uint32_t dlen_be;
    if (readn(sockfd, &dlen_be, sizeof(dlen_be)) != (int)sizeof(dlen_be))
    {
        return -EIO;
    }
    uint32_t dlen = ntohl(dlen_be);

    // Preallocate vector: we store status in host order as first 4 bytes,
    // then append the server data bytes (if any).
    out_status_and_data.resize(4 + dlen);
    memcpy(out_status_and_data.data(), &status, 4); // status in host order

    if (dlen)
    {
        if (readn(sockfd, out_status_and_data.data() + 4, dlen) != (int)dlen)
        {
            return -EIO;
        }
    }
    cout << "Recieved" << endl;
    return 0;
}

// ------------------------ High level operation helpers ------------------------
// Each helper constructs the on-wire request for an operation, calls
// send_frame_and_recv(), validates the response, and fills the caller's
// outputs (stat structs, read data, server file descriptor, etc).
//
// On error, helpers return a negative errno value suitable for returning to
// FUSE (e.g. -ENOENT, -EIO). The server returns a status int (0 on success,
// errno-style on failure) which we propagate back to the FUSE layer.

// do_getattr:
//  - Fills `stbuf` for path. Returns 0 on success or negative errno on failure.
static int do_getattr(const char *path, struct stat *stbuf)
{
    string p(path);
    uint32_t op_be = htonl(OP_GETATTR);
    uint32_t pathlen_be = htonl(static_cast<uint32_t>(p.size()));

    // build payload: op(4) | pathlen(4) | path bytes
    vector<char> payload;
    payload.insert(payload.end(), reinterpret_cast<char *>(&op_be), reinterpret_cast<char *>(&op_be) + 4);
    payload.insert(payload.end(), reinterpret_cast<char *>(&pathlen_be), reinterpret_cast<char *>(&pathlen_be) + 4);
    payload.insert(payload.end(), p.begin(), p.end());

    vector<char> resp;
    if (send_frame_and_recv(payload.data(), (uint32_t)payload.size(), resp) != 0)
    {
        return -EIO;
    }

    // resp layout: status(4) | possibly struct stat bytes...
    if (resp.size() < 4)
        return -EIO;
    uint32_t status;
    memcpy(&status, resp.data(), 4); // host-order status
    if (status != 0)
        return -(int)status;

    // ensure we received enough bytes for a stat struct
    if (resp.size() < 4 + sizeof(struct stat))
        return -EIO;
    memcpy(stbuf, resp.data() + 4, sizeof(struct stat));
    return 0;
}

// do_readdir:
//  - Server returns a sequence of null-terminated names concatenated.
//  - We iterate through them and call the provided `filler` function for each.
static int do_readdir(const char *path, void *buf, fuse_fill_dir_t filler,
                      off_t /*offset*/, fuse_readdir_flags /*flags*/)
{
    string p(path);
    uint32_t op_be = htonl(OP_READDIR);
    uint32_t pathlen_be = htonl(static_cast<uint32_t>(p.size()));

    vector<char> payload;
    payload.insert(payload.end(), reinterpret_cast<char *>(&op_be), reinterpret_cast<char *>(&op_be) + 4);
    payload.insert(payload.end(), reinterpret_cast<char *>(&pathlen_be), reinterpret_cast<char *>(&pathlen_be) + 4);
    payload.insert(payload.end(), p.begin(), p.end());

    vector<char> resp;
    if (send_frame_and_recv(payload.data(), (uint32_t)payload.size(), resp) != 0)
        return -EIO;
    if (resp.size() < 4)
        return -EIO;
    uint32_t status;
    memcpy(&status, resp.data(), 4);
    if (status != 0)
        return -(int)status;

    // The remainder (resp.data()+4, length resp.size()-4) is a sequence of
    // zero-terminated names. Iterate and call filler for each.
    const char *ptr = resp.data() + 4;
    size_t len = resp.size() - 4;
    size_t i = 0;
    while (i < len)
    {
        // If there are stray zero bytes, skip them.
        if (ptr[i] == '\0')
        {
            ++i;
            continue;
        }
        const char *name = ptr + i;
        size_t nlen = strlen(name);
        // FUSE v3 filler signature has flags parameter. We pass 0 for those.
        filler(buf, name, NULL, 0, static_cast<fuse_fill_dir_flags>(0));
        i += nlen + 1;
    }
    return 0;
}

// do_open_or_create:
//  - If create==false -> OP_OPEN else OP_CREATE.
//  - On success returns a server-side fd (64-bit) in out_serverfd.
static int do_open_or_create(const char *path, int flags, int mode, bool create, uint64_t &out_serverfd)
{
    string p(path);
    uint32_t op_be = htonl(create ? OP_CREATE : OP_OPEN);
    uint32_t pathlen_be = htonl(static_cast<uint32_t>(p.size()));
    uint32_t flags_be = htonl(static_cast<uint32_t>(flags));

    vector<char> payload;
    payload.insert(payload.end(), reinterpret_cast<char *>(&op_be), reinterpret_cast<char *>(&op_be) + 4);
    payload.insert(payload.end(), reinterpret_cast<char *>(&pathlen_be), reinterpret_cast<char *>(&pathlen_be) + 4);
    payload.insert(payload.end(), p.begin(), p.end());
    payload.insert(payload.end(), reinterpret_cast<char *>(&flags_be), reinterpret_cast<char *>(&flags_be) + 4);

    if (create)
    {
        uint32_t mode_be = htonl(static_cast<uint32_t>(mode));
        payload.insert(payload.end(), reinterpret_cast<char *>(&mode_be), reinterpret_cast<char *>(&mode_be) + 4);
    }

    vector<char> resp;
    if (send_frame_and_recv(payload.data(), (uint32_t)payload.size(), resp) != 0)
        return -EIO;
    if (resp.size() < 4)
        return -EIO;

    uint32_t status;
    memcpy(&status, resp.data(), 4);
    if (status != 0)
        return -(int)status;

    // server returns 8-byte BE file-descriptor handle after the status
    if (resp.size() < 4 + 8)
        return -EIO;
    uint64_t fdbe;
    memcpy(&fdbe, resp.data() + 4, 8);
    out_serverfd = be64toh(fdbe);
    return 0;
}

// do_read:
//  - Reads up to `size` bytes from `serverfd` at `offset`.
//  - On success sets *out_read to the number of bytes actually returned.
// do_read:
//  - Reads up to `size` bytes from `serverfd` at `offset`.
//  - Streams the data in CHUNK_SIZE blocks to avoid large allocations.
static int do_read(uint64_t serverfd, char *buf, size_t size, off_t offset, size_t *out_read)
{
    size_t total_read = 0;
    char chunkbuf[CHUNK_SIZE];

    while (total_read < size)
    {
        size_t this_chunk = std::min((size_t)CHUNK_SIZE, size - total_read);

        uint32_t op_be = htonl(OP_READ);
        uint64_t fd_be = htobe64(serverfd);
        uint64_t off_be = htobe64((uint64_t)(offset + total_read));
        uint32_t size_be = htonl(static_cast<uint32_t>(this_chunk));

        std::vector<char> payload;
        payload.insert(payload.end(), reinterpret_cast<char *>(&op_be), reinterpret_cast<char *>(&op_be) + 4);
        payload.insert(payload.end(), reinterpret_cast<char *>(&fd_be), reinterpret_cast<char *>(&fd_be) + 8);
        payload.insert(payload.end(), reinterpret_cast<char *>(&off_be), reinterpret_cast<char *>(&off_be) + 8);
        payload.insert(payload.end(), reinterpret_cast<char *>(&size_be), reinterpret_cast<char *>(&size_be) + 4);

        std::vector<char> resp;
        if (send_frame_and_recv(payload.data(), (uint32_t)payload.size(), resp) != 0)
            return -EIO;

        if (resp.size() < 4)
            return -EIO;

        uint32_t status;
        memcpy(&status, resp.data(), 4);
        if (status != 0)
            return -(int)status;

        size_t dlen = resp.size() - 4;
        if (dlen == 0)
            break; // EOF

        if (buf)
            memcpy(buf + total_read, resp.data() + 4, dlen);

        total_read += dlen;
        if (dlen < this_chunk)
            break; // short read, done
    }

    *out_read = total_read;
    return 0;
}

// do_write:
//  - Sends data to write in CHUNK_SIZE chunks.
//  - Expects server to respond with bytes written per chunk.
static int do_write(uint64_t serverfd, const char *buf, size_t size, off_t offset, size_t *out_written)
{
    size_t total_written = 0;

    while (total_written < size)
    {
        size_t this_chunk = std::min((size_t)CHUNK_SIZE, size - total_written);

        uint32_t op_be = htonl(OP_WRITE);
        uint64_t fd_be = htobe64(serverfd);
        uint64_t off_be = htobe64((uint64_t)(offset + total_written));
        uint32_t size_be = htonl(static_cast<uint32_t>(this_chunk));

        // payload: op | fd | offset | size | data...
        std::vector<char> payload;
        payload.insert(payload.end(), reinterpret_cast<char *>(&op_be), reinterpret_cast<char *>(&op_be) + 4);
        payload.insert(payload.end(), reinterpret_cast<char *>(&fd_be), reinterpret_cast<char *>(&fd_be) + 8);
        payload.insert(payload.end(), reinterpret_cast<char *>(&off_be), reinterpret_cast<char *>(&off_be) + 8);
        payload.insert(payload.end(), reinterpret_cast<char *>(&size_be), reinterpret_cast<char *>(&size_be) + 4);
        payload.insert(payload.end(), buf + total_written, buf + total_written + this_chunk);

        std::vector<char> resp;
        if (send_frame_and_recv(payload.data(), (uint32_t)payload.size(), resp) != 0)
            return -EIO;

        if (resp.size() < 8)
            return -EIO;

        uint32_t status;
        memcpy(&status, resp.data(), 4);
        if (status != 0)
            return -(int)status;

        uint32_t wrote_be;
        memcpy(&wrote_be, resp.data() + 4, 4);
        uint32_t wrote = ntohl(wrote_be);

        total_written += wrote;
        if (wrote < this_chunk)
            break; // probably full disk or partial write
    }

    *out_written = total_written;
    return 0;
}

// do_release: close server-side handle
static int do_release(uint64_t serverfd)
{
    uint32_t op_be = htonl(OP_RELEASE);
    uint64_t fd_be = htobe64(serverfd);

    vector<char> payload;
    payload.insert(payload.end(), reinterpret_cast<char *>(&op_be), reinterpret_cast<char *>(&op_be) + 4);
    payload.insert(payload.end(), reinterpret_cast<char *>(&fd_be), reinterpret_cast<char *>(&fd_be) + 8);

    vector<char> resp;
    if (send_frame_and_recv(payload.data(), (uint32_t)payload.size(), resp) != 0)
        return -EIO;
    if (resp.size() < 4)
        return -EIO;
    uint32_t status;
    memcpy(&status, resp.data(), 4);
    if (status != 0)
        return -(int)status;
    return 0;
}

// do_unlink, do_mkdir, do_rmdir, do_truncate, do_utimens, do_statfs
// All follow the same pattern: build payload (op | path_len | path | extra fields),
// send, check status, and propagate server errno on failure.

static int do_unlink(const char *path)
{
    string p(path);
    uint32_t op_be = htonl(OP_UNLINK);
    uint32_t pathlen_be = htonl(static_cast<uint32_t>(p.size()));

    vector<char> payload;
    payload.insert(payload.end(), reinterpret_cast<char *>(&op_be), reinterpret_cast<char *>(&op_be) + 4);
    payload.insert(payload.end(), reinterpret_cast<char *>(&pathlen_be), reinterpret_cast<char *>(&pathlen_be) + 4);
    payload.insert(payload.end(), p.begin(), p.end());

    vector<char> resp;
    if (send_frame_and_recv(payload.data(), (uint32_t)payload.size(), resp) != 0)
        return -EIO;
    if (resp.size() < 4)
        return -EIO;
    uint32_t status;
    memcpy(&status, resp.data(), 4);
    if (status != 0)
        return -(int)status;
    return 0;
}

static int do_mkdir(const char *path, mode_t mode)
{
    string p(path);
    uint32_t op_be = htonl(OP_MKDIR);
    uint32_t pathlen_be = htonl(static_cast<uint32_t>(p.size()));
    uint32_t mode_be = htonl(static_cast<uint32_t>(mode));

    vector<char> payload;
    payload.insert(payload.end(), reinterpret_cast<char *>(&op_be), reinterpret_cast<char *>(&op_be) + 4);
    payload.insert(payload.end(), reinterpret_cast<char *>(&pathlen_be), reinterpret_cast<char *>(&pathlen_be) + 4);
    payload.insert(payload.end(), p.begin(), p.end());
    payload.insert(payload.end(), reinterpret_cast<char *>(&mode_be), reinterpret_cast<char *>(&mode_be) + 4);

    vector<char> resp;
    if (send_frame_and_recv(payload.data(), (uint32_t)payload.size(), resp) != 0)
        return -EIO;
    if (resp.size() < 4)
        return -EIO;
    uint32_t status;
    memcpy(&status, resp.data(), 4);
    if (status != 0)
        return -(int)status;
    return 0;
}

static int do_rmdir(const char *path)
{
    string p(path);
    uint32_t op_be = htonl(OP_RMDIR);
    uint32_t pathlen_be = htonl(static_cast<uint32_t>(p.size()));

    vector<char> payload;
    payload.insert(payload.end(), reinterpret_cast<char *>(&op_be), reinterpret_cast<char *>(&op_be) + 4);
    payload.insert(payload.end(), reinterpret_cast<char *>(&pathlen_be), reinterpret_cast<char *>(&pathlen_be) + 4);
    payload.insert(payload.end(), p.begin(), p.end());

    vector<char> resp;
    if (send_frame_and_recv(payload.data(), (uint32_t)payload.size(), resp) != 0)
        return -EIO;
    if (resp.size() < 4)
        return -EIO;
    uint32_t status;
    memcpy(&status, resp.data(), 4);
    if (status != 0)
        return -(int)status;
    return 0;
}

static int do_truncate(const char *path, off_t size)
{
    string p(path);
    uint32_t op_be = htonl(OP_TRUNCATE);
    uint32_t pathlen_be = htonl(static_cast<uint32_t>(p.size()));
    uint64_t size_be = htobe64((uint64_t)size);

    vector<char> payload;
    payload.insert(payload.end(), reinterpret_cast<char *>(&op_be), reinterpret_cast<char *>(&op_be) + 4);
    payload.insert(payload.end(), reinterpret_cast<char *>(&pathlen_be), reinterpret_cast<char *>(&pathlen_be) + 4);
    payload.insert(payload.end(), p.begin(), p.end());
    payload.insert(payload.end(), reinterpret_cast<char *>(&size_be), reinterpret_cast<char *>(&size_be) + 8);

    vector<char> resp;
    if (send_frame_and_recv(payload.data(), (uint32_t)payload.size(), resp) != 0)
        return -EIO;
    if (resp.size() < 4)
        return -EIO;
    uint32_t status;
    memcpy(&status, resp.data(), 4);
    if (status != 0)
        return -(int)status;
    return 0;
}

static int do_utimens(const char *path, const struct timespec tv[2])
{
    string p(path);
    uint32_t op_be = htonl(OP_UTIMENS);
    uint32_t pathlen_be = htonl(static_cast<uint32_t>(p.size()));

    // Convert timespec components to big-endian 64-bit
    uint64_t at_sec = htobe64((uint64_t)tv[0].tv_sec);
    uint64_t at_nsec = htobe64((uint64_t)tv[0].tv_nsec);
    uint64_t mt_sec = htobe64((uint64_t)tv[1].tv_sec);
    uint64_t mt_nsec = htobe64((uint64_t)tv[1].tv_nsec);

    vector<char> payload;
    payload.insert(payload.end(), reinterpret_cast<char *>(&op_be), reinterpret_cast<char *>(&op_be) + 4);
    payload.insert(payload.end(), reinterpret_cast<char *>(&pathlen_be), reinterpret_cast<char *>(&pathlen_be) + 4);
    payload.insert(payload.end(), p.begin(), p.end());
    payload.insert(payload.end(), reinterpret_cast<char *>(&at_sec), reinterpret_cast<char *>(&at_sec) + 8);
    payload.insert(payload.end(), reinterpret_cast<char *>(&at_nsec), reinterpret_cast<char *>(&at_nsec) + 8);
    payload.insert(payload.end(), reinterpret_cast<char *>(&mt_sec), reinterpret_cast<char *>(&mt_sec) + 8);
    payload.insert(payload.end(), reinterpret_cast<char *>(&mt_nsec), reinterpret_cast<char *>(&mt_nsec) + 8);

    vector<char> resp;
    if (send_frame_and_recv(payload.data(), (uint32_t)payload.size(), resp) != 0)
        return -EIO;
    if (resp.size() < 4)
        return -EIO;
    uint32_t status;
    memcpy(&status, resp.data(), 4);
    if (status != 0)
        return -(int)status;
    return 0;
}

static int do_statfs(const char *path, struct statvfs *stbuf)
{
    string p(path);
    uint32_t op_be = htonl(OP_STATFS);
    uint32_t pathlen_be = htonl(static_cast<uint32_t>(p.size()));

    vector<char> payload;
    payload.insert(payload.end(), reinterpret_cast<char *>(&op_be), reinterpret_cast<char *>(&op_be) + 4);
    payload.insert(payload.end(), reinterpret_cast<char *>(&pathlen_be), reinterpret_cast<char *>(&pathlen_be) + 4);
    payload.insert(payload.end(), p.begin(), p.end());

    vector<char> resp;
    if (send_frame_and_recv(payload.data(), (uint32_t)payload.size(), resp) != 0)
        return -EIO;
    if (resp.size() < 4 + sizeof(struct statvfs))
        return -EIO;
    uint32_t status;
    memcpy(&status, resp.data(), 4);
    if (status != 0)
        return -(int)status;
    memcpy(stbuf, resp.data() + 4, sizeof(struct statvfs));
    return 0;
}

// ------------------------------ FUSE callbacks ------------------------------
// Each callback adapts FUSE's API to the corresponding do_* helper above.
// Callbacks return either 0 (success) or a negative errno (so FUSE can map it).

static int nf_getattr(const char *path, struct stat *stbuf, struct fuse_file_info * /*fi*/)
{
    cout << "Get Attribute Called with path: " << path << endl;
    return do_getattr(path, stbuf);
}

// FUSE readdir signature for FUSE >= 3.5 may include flags. We forward them
// to do_readdir which currently ignores offset and flags.
static int nf_readdir(const char *path, void *buf, fuse_fill_dir_t filler, off_t offset,
                      struct fuse_file_info * /*fi*/, fuse_readdir_flags flags)
{
    cout << "Readdir Called with path: " << path << endl;
    return do_readdir(path, buf, filler, offset, flags);
}

static int nf_open(const char *path, struct fuse_file_info *fi)
{
    cout << "Open Called with path: " << path << endl;
    uint64_t serverfd = 0;
    fi->direct_io = 1; // No Read/Write caching
    int flags = fi->flags; // POSIX flags from FUSE
    int r = do_open_or_create(path, flags, 0644, false, serverfd);
    if (r < 0)
        return r;
    // store remote handle in fi->fh so subsequent read/write/release get it
    fi->fh = serverfd;
    return 0;
}

static int nf_create(const char *path, mode_t mode, struct fuse_file_info *fi)
{
    cout << "Create Called with path: " << path << endl;
    uint64_t serverfd = 0;
    //    fi->direct_io = 1; // No Read/Write caching
    int r = do_open_or_create(path, fi->flags, mode, true, serverfd);
    if (r < 0)
        return r;
    fi->fh = serverfd;
    return 0;
}

static int nf_read(const char *path, char *buf, size_t size, off_t offset, struct fuse_file_info *fi)
{
    cout << "Read called with path: " << path << " and buf size: " << size << endl;
    size_t got = 0;
    int r = do_read((uint64_t)fi->fh, buf, size, offset, &got);
    if (r < 0)
        return r;
    return (int)got; // FUSE expects number of bytes read
}

static int nf_write(const char *path, const char *buf, size_t size, off_t offset, struct fuse_file_info *fi)
{
    cout << "Write called with path: " << path << " and buf size: " << size << endl;
    size_t wrote = 0;
    int r = do_write((uint64_t)fi->fh, buf, size, offset, &wrote);
    if (r < 0)
        return r;
    return (int)wrote; // number of bytes written
}

static int nf_release(const char *path, struct fuse_file_info *fi)
{
    cout << "Release called with path: " << path << endl;
    return do_release((uint64_t)fi->fh);
}

static int nf_unlink(const char *path) { return do_unlink(path); }
static int nf_mkdir(const char *path, mode_t mode) { return do_mkdir(path, mode); }
static int nf_rmdir(const char *path) { return do_rmdir(path); }
static int nf_truncate(const char *path, off_t size, struct fuse_file_info * /*fi*/)
{
    return do_truncate(path, size);
}
static int nf_utimens(const char *path, const struct timespec tv[2], struct fuse_file_info * /*fi*/)
{
    return do_utimens(path, tv);
}
static int nf_statfs(const char *path, struct statvfs *stbuf) { return do_statfs(path, stbuf); }

// zero-init and then set only the callbacks we implement.
static struct fuse_operations nf_ops;

// ----------------------------------- main -----------------------------------
// Parse arguments: <mountpoint> <server-host> <server-port> [fuse args...]
// Connect to server, build argc/argv for fuse_main, and hand off to FUSE.
int main(int argc, char **argv)
{
    if (argc < 4)
    {
        fprintf(stderr, "Usage: %s <mountpoint> <server-host> <server-port> [fuse-args...]\n", argv[0]);
        return 1;
    }

    const char *mountpoint = argv[1];
    const char *host = argv[2];
    const char *port = argv[3];

    // Establish persistent connection to the remote server before mounting.
    sockfd = connect_to_server(host, port);
    if (sockfd < 0)
        die("connect_to_server");

    // Build arguments to pass to fuse_main:
    //   program name, mountpoint, then any additional fuse args supplied by user.
    vector<char *> fargs;
    fargs.push_back(argv[0]);
    fargs.push_back(const_cast<char *>(mountpoint));
    for (int i = 4; i < argc; ++i)
    {
        fargs.push_back(argv[i]);
    }
    int fargc = (int)fargs.size();
    fargs.push_back(nullptr); // fuse_main expects a null-terminated argv

    cout << "Client started" << endl;

    // Zero-init operations structure and then assign callbacks so we
    // avoid potential designated-initializer/ordering issues on some compilers.
    memset(&nf_ops, 0, sizeof(nf_ops));
    nf_ops.getattr = nf_getattr;
    nf_ops.readdir = nf_readdir;
    nf_ops.open = nf_open;
    nf_ops.create = nf_create;
    nf_ops.read = nf_read;
    nf_ops.write = nf_write;
    nf_ops.release = nf_release;
    nf_ops.unlink = nf_unlink;
    nf_ops.mkdir = nf_mkdir;
    nf_ops.rmdir = nf_rmdir;
    nf_ops.truncate = nf_truncate;
    nf_ops.utimens = nf_utimens;
    nf_ops.statfs = nf_statfs;

    // Call into FUSE. This function will block until the filesystem is
    // unmounted. On return, fuse_main will have cleaned up FUSE state.
    int ret = fuse_main(fargc, fargs.data(), &nf_ops, nullptr);

    // Close the socket after unmount (best-effort).
    if (sockfd >= 0)
    {
        close(sockfd);
        sockfd = -1;
    }

    return ret;
}
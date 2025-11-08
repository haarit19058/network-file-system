// nfuse_client_with_cache_and_metrics.cpp
// Rewritten FUSE client with improved client-side LRU block cache and
// precise per-request metrics logging (timestamp, opcode, latency_us,
// bytes_sent, bytes_recv, cache_hit/miss).

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

static string opcode_to_string(uint32_t op)
{
    switch (op)
    {
    case OP_GETATTR: return "GETATTR";
    case OP_READDIR: return "READDIR";
    case OP_OPEN: return "OPEN";
    case OP_READ: return "READ";
    case OP_WRITE: return "WRITE";
    case OP_CREATE: return "CREATE";
    case OP_UNLINK: return "UNLINK";
    case OP_MKDIR: return "MKDIR";
    case OP_RMDIR: return "RMDIR";
    case OP_TRUNCATE: return "TRUNCATE";
    case OP_UTIMENS: return "UTIMENS";
    case OP_STATFS: return "STATFS";
    case OP_RELEASE: return "RELEASE";
    default: return "UNKNOWN";
    }
}

// ------------------------------ Metrics logger ----------------------------
// Writes CSV lines: timestamp_us,opcode,latency_us,bytes_sent,bytes_recv,cache
class MetricLogger
{
    mutex mtx;
    ofstream out;
    string filename;

public:
    MetricLogger(const string &fname = "metrics.csv") : filename(fname)
    {
        // Open in append mode; if file is new/empty write header
        out.open(filename, ios::app);
        if (!out.is_open())
        {
            cerr << "Warning: cannot open metrics file '" << filename << "' for appending\n";
            return;
        }
        // If file is empty, write header. Check size by seeking
        out.seekp(0, ios::end);
        if (out.tellp() == 0)
        {
            out << "timestamp_us,opcode,latency_us,bytes_sent,bytes_recv,cache\n";
            out.flush();
        }
    }

    void log(const string &opcode, long latency_us, size_t bytes_sent, size_t bytes_recv, const string &cache_status)
    {
        lock_guard<mutex> lk(mtx);
        if (!out.is_open())
            return;
        using namespace chrono;
        auto now = duration_cast<microseconds>(system_clock::now().time_since_epoch()).count();
        out << now << "," << opcode << "," << latency_us << "," << bytes_sent << "," << bytes_recv << "," << cache_status << "\n";
        out.flush();
    }
};

static MetricLogger metrics;

// --------------------------- Global socket state ---------------------------
static int sockfd = -1;
static mutex sock_mtx;

// ---------------------------- Caching parameters ---------------------------
static const size_t CACHE_BLOCK_SIZE = 4096;                       // 4KB blocks
static const size_t CACHE_CAPACITY_BYTES = 16 * 1024 * 1024;       // 16 MB total cache

// ------------------------------- Cache types ------------------------------
struct CacheValue
{
    string data;   // actual bytes
    size_t size;   // data.size()
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
    for (auto it = lru_list.begin(); it != lru_list.end(); )
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

// ----------------------------- Helper that prints errno and exits
static void die(const char *m)
{
    perror(m);
    exit(1);
}

// --------------------------- Networking helpers ---------------------------
static int connect_to_server(const char *host, const char *port)
{
    struct addrinfo hints{};
    struct addrinfo *res = nullptr;

    hints.ai_family = AF_UNSPEC;
    hints.ai_socktype = SOCK_STREAM;

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
                continue;
            return -1;
        }
        if (r == 0)
            return 0;
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
                continue;
            return -1;
        }
        if (w == 0)
            return 0;
        left -= w;
        p += w;
    }
    return (int)n;
}

// send_frame_and_recv: writes 4-byte BE length + payload, then reads status(4)|dlen(4)|data
// Returns 0 on success. On success returns latency_us, bytes_sent and bytes_recv to caller.
static int send_frame_and_recv(const void *payload, uint32_t payload_len, vector<char> &out_status_and_data,
                               long &out_latency_us, size_t &out_bytes_sent, size_t &out_bytes_recv)
{
    using namespace chrono;
    auto start = high_resolution_clock::now();

    unique_lock<mutex> lk(sock_mtx);

    uint32_t len_be = htonl(payload_len);
    if (writen(sockfd, &len_be, sizeof(len_be)) != (int)sizeof(len_be))
        return -EIO;
    if (payload_len > 0 && writen(sockfd, payload, payload_len) != (int)payload_len)
        return -EIO;

    uint32_t status_be, dlen_be;
    if (readn(sockfd, &status_be, sizeof(status_be)) != (int)sizeof(status_be))
        return -EIO;
    if (readn(sockfd, &dlen_be, sizeof(dlen_be)) != (int)sizeof(dlen_be))
        return -EIO;
    uint32_t dlen = ntohl(dlen_be);
    out_status_and_data.resize(4 + dlen);
    memcpy(out_status_and_data.data(), &status_be, 4);
    if (dlen && readn(sockfd, out_status_and_data.data() + 4, dlen) != (int)dlen)
        return -EIO;

    auto end = high_resolution_clock::now();
    out_latency_us = duration_cast<microseconds>(end - start).count();
    out_bytes_sent = sizeof(len_be) + payload_len; // 4 + payload
    out_bytes_recv = sizeof(status_be) + sizeof(dlen_be) + dlen; // 8 + dlen

    return 0;
}

// ------------------------ High level operation helpers ------------------------
static int do_getattr(const char *path, struct stat *stbuf)
{
    string p(path);
    uint32_t op_be = htonl(OP_GETATTR);
    uint32_t pathlen_be = htonl(static_cast<uint32_t>(p.size()));

    vector<char> payload;
    payload.insert(payload.end(), reinterpret_cast<char *>(&op_be), reinterpret_cast<char *>(&op_be) + 4);
    payload.insert(payload.end(), reinterpret_cast<char *>(&pathlen_be), reinterpret_cast<char *>(&pathlen_be) + 4);
    payload.insert(payload.end(), p.begin(), p.end());

    vector<char> resp;
    long latency_us = 0; size_t bytes_sent = 0, bytes_recv = 0;
    if (send_frame_and_recv(payload.data(), (uint32_t)payload.size(), resp, latency_us, bytes_sent, bytes_recv) != 0)
        return -EIO;

    // Log the network request
    metrics.log(opcode_to_string(OP_GETATTR), latency_us, bytes_sent, bytes_recv, "none");

    if (resp.size() < 4)
        return -EIO;
    uint32_t status;
    memcpy(&status, resp.data(), 4);
    if (status != 0)
        return -(int)status;

    if (resp.size() < 4 + sizeof(struct stat))
        return -EIO;
    memcpy(stbuf, resp.data() + 4, sizeof(struct stat));
    return 0;
}

static int do_readdir(const char *path, void *buf, fuse_fill_dir_t filler)
{
    string p(path);
    uint32_t op_be = htonl(OP_READDIR);
    uint32_t pathlen_be = htonl(static_cast<uint32_t>(p.size()));

    vector<char> payload;
    payload.insert(payload.end(), reinterpret_cast<char *>(&op_be), reinterpret_cast<char *>(&op_be) + 4);
    payload.insert(payload.end(), reinterpret_cast<char *>(&pathlen_be), reinterpret_cast<char *>(&pathlen_be) + 4);
    payload.insert(payload.end(), p.begin(), p.end());

    vector<char> resp;
    long latency_us = 0; size_t bytes_sent = 0, bytes_recv = 0;
    if (send_frame_and_recv(payload.data(), (uint32_t)payload.size(), resp, latency_us, bytes_sent, bytes_recv) != 0)
        return -EIO;

    metrics.log(opcode_to_string(OP_READDIR), latency_us, bytes_sent, bytes_recv, "none");

    if (resp.size() < 4)
        return -EIO;
    uint32_t status;
    memcpy(&status, resp.data(), 4);
    if (status != 0)
        return -(int)status;

    const char *ptr = resp.data() + 4;
    size_t len = resp.size() - 4;
    size_t i = 0;
    while (i < len)
    {
        if (ptr[i] == '\0') { ++i; continue; }
        const char *name = ptr + i;
        size_t nlen = strlen(name);
        filler(buf, name, NULL, 0, static_cast<fuse_fill_dir_flags>(0));
        i += nlen + 1;
    }
    return 0;
}

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
    long latency_us = 0; size_t bytes_sent = 0, bytes_recv = 0;
    if (send_frame_and_recv(payload.data(), (uint32_t)payload.size(), resp, latency_us, bytes_sent, bytes_recv) != 0)
        return -EIO;

    metrics.log(opcode_to_string(create ? OP_CREATE : OP_OPEN), latency_us, bytes_sent, bytes_recv, "none");

    if (resp.size() < 4)
        return -EIO;
    uint32_t status;
    memcpy(&status, resp.data(), 4);
    if (status != 0)
        return -(int)status;

    if (resp.size() < 4 + 8)
        return -EIO;
    uint64_t fdbe;
    memcpy(&fdbe, resp.data() + 4, 8);
    out_serverfd = be64toh(fdbe);

    {
        lock_guard<mutex> lk(fdpath_mtx);
        fd_to_path[out_serverfd] = p;
    }
    return 0;
}

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

        vector<char> payload;
        payload.insert(payload.end(), reinterpret_cast<char *>(&op_be), reinterpret_cast<char *>(&op_be) + 4);
        payload.insert(payload.end(), reinterpret_cast<char *>(&fd_be), reinterpret_cast<char *>(&fd_be) + 8);
        payload.insert(payload.end(), reinterpret_cast<char *>(&off_be), reinterpret_cast<char *>(&off_be) + 8);
        payload.insert(payload.end(), reinterpret_cast<char *>(&size_be), reinterpret_cast<char *>(&size_be) + 4);

        vector<char> resp;
        long latency_us = 0; size_t bytes_sent = 0, bytes_recv = 0;
        if (send_frame_and_recv(payload.data(), (uint32_t)payload.size(), resp, latency_us, bytes_sent, bytes_recv) != 0)
            return -EIO;

        // mark this network-backed read as a cache MISS for metrics purposes
        metrics.log(opcode_to_string(OP_READ), latency_us, bytes_sent, bytes_recv, "miss");

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
            break;
    }

    *out_read = total_read;
    return 0;
}

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

        vector<char> payload;
        payload.insert(payload.end(), reinterpret_cast<char *>(&op_be), reinterpret_cast<char *>(&op_be) + 4);
        payload.insert(payload.end(), reinterpret_cast<char *>(&fd_be), reinterpret_cast<char *>(&fd_be) + 8);
        payload.insert(payload.end(), reinterpret_cast<char *>(&off_be), reinterpret_cast<char *>(&off_be) + 8);
        payload.insert(payload.end(), reinterpret_cast<char *>(&size_be), reinterpret_cast<char *>(&size_be) + 4);
        payload.insert(payload.end(), buf + total_written, buf + total_written + this_chunk);

        vector<char> resp;
        long latency_us = 0; size_t bytes_sent = 0, bytes_recv = 0;
        if (send_frame_and_recv(payload.data(), (uint32_t)payload.size(), resp, latency_us, bytes_sent, bytes_recv) != 0)
            return -EIO;

        metrics.log(opcode_to_string(OP_WRITE), latency_us, bytes_sent, bytes_recv, "none");

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
            break;
    }

    *out_written = total_written;
    return 0;
}

static int do_release(uint64_t serverfd)
{
    uint32_t op_be = htonl(OP_RELEASE);
    uint64_t fd_be = htobe64(serverfd);

    vector<char> payload;
    payload.insert(payload.end(), reinterpret_cast<char *>(&op_be), reinterpret_cast<char *>(&op_be) + 4);
    payload.insert(payload.end(), reinterpret_cast<char *>(&fd_be), reinterpret_cast<char *>(&fd_be) + 8);

    vector<char> resp;
    long latency_us = 0; size_t bytes_sent = 0, bytes_recv = 0;
    if (send_frame_and_recv(payload.data(), (uint32_t)payload.size(), resp, latency_us, bytes_sent, bytes_recv) != 0)
        return -EIO;

    metrics.log(opcode_to_string(OP_RELEASE), latency_us, bytes_sent, bytes_recv, "none");

    if (resp.size() < 4)
        return -EIO;
    uint32_t status;
    memcpy(&status, resp.data(), 4);
    if (status != 0)
        return -(int)status;
    return 0;
}

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
    long latency_us = 0; size_t bytes_sent = 0, bytes_recv = 0;
    if (send_frame_and_recv(payload.data(), (uint32_t)payload.size(), resp, latency_us, bytes_sent, bytes_recv) != 0)
        return -EIO;

    metrics.log(opcode_to_string(OP_UNLINK), latency_us, bytes_sent, bytes_recv, "none");

    if (resp.size() < 4)
        return -EIO;
    uint32_t status;
    memcpy(&status, resp.data(), 4);
    if (status != 0)
        return -(int)status;

    cache_invalidate_path(p);
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
    long latency_us = 0; size_t bytes_sent = 0, bytes_recv = 0;
    if (send_frame_and_recv(payload.data(), (uint32_t)payload.size(), resp, latency_us, bytes_sent, bytes_recv) != 0)
        return -EIO;

    metrics.log(opcode_to_string(OP_MKDIR), latency_us, bytes_sent, bytes_recv, "none");

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
    long latency_us = 0; size_t bytes_sent = 0, bytes_recv = 0;
    if (send_frame_and_recv(payload.data(), (uint32_t)payload.size(), resp, latency_us, bytes_sent, bytes_recv) != 0)
        return -EIO;

    metrics.log(opcode_to_string(OP_RMDIR), latency_us, bytes_sent, bytes_recv, "none");

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
    long latency_us = 0; size_t bytes_sent = 0, bytes_recv = 0;
    if (send_frame_and_recv(payload.data(), (uint32_t)payload.size(), resp, latency_us, bytes_sent, bytes_recv) != 0)
        return -EIO;

    metrics.log(opcode_to_string(OP_TRUNCATE), latency_us, bytes_sent, bytes_recv, "none");

    if (resp.size() < 4)
        return -EIO;
    uint32_t status;
    memcpy(&status, resp.data(), 4);
    if (status != 0)
        return -(int)status;

    cache_invalidate_path(p);
    return 0;
}

static int do_utimens(const char *path, const struct timespec tv[2])
{
    string p(path);
    uint32_t op_be = htonl(OP_UTIMENS);
    uint32_t pathlen_be = htonl(static_cast<uint32_t>(p.size()));
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
    long latency_us = 0; size_t bytes_sent = 0, bytes_recv = 0;
    if (send_frame_and_recv(payload.data(), (uint32_t)payload.size(), resp, latency_us, bytes_sent, bytes_recv) != 0)
        return -EIO;

    metrics.log(opcode_to_string(OP_UTIMENS), latency_us, bytes_sent, bytes_recv, "none");

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
    long latency_us = 0; size_t bytes_sent = 0, bytes_recv = 0;
    if (send_frame_and_recv(payload.data(), (uint32_t)payload.size(), resp, latency_us, bytes_sent, bytes_recv) != 0)
        return -EIO;

    metrics.log(opcode_to_string(OP_STATFS), latency_us, bytes_sent, bytes_recv, "none");

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
static int nf_getattr(const char *path, struct stat *stbuf, struct fuse_file_info *)
{
    return do_getattr(path, stbuf);
}

static int nf_readdir(const char *path, void *buf, fuse_fill_dir_t filler, off_t, struct fuse_file_info *, fuse_readdir_flags)
{
    return do_readdir(path, buf, filler);
}

static int nf_open(const char *path, struct fuse_file_info *fi)
{
    uint64_t serverfd = 0;
    int flags = fi->flags;
    int r = do_open_or_create(path, flags, 0644, false, serverfd);
    if (r < 0)
        return r;
    fi->fh = serverfd;
    return 0;
}

static int nf_create(const char *path, mode_t mode, struct fuse_file_info *fi)
{
    uint64_t serverfd = 0;
    int r = do_open_or_create(path, fi->flags, mode, true, serverfd);
    if (r < 0)
        return r;
    fi->fh = serverfd;
    return 0;
}

// Serve reads from block cache when possible. For missing blocks, fetch from server.
static int nf_read(const char *path, char *buf, size_t size, off_t offset, struct fuse_file_info *fi)
{
    if (size == 0)
        return 0;

    string spath(path);
    uint64_t serverfd = (uint64_t)fi->fh;

    size_t bytes_filled = 0;
    uint64_t first_block = (uint64_t)offset / CACHE_BLOCK_SIZE;
    uint64_t last_block = (uint64_t)(offset + size - 1) / CACHE_BLOCK_SIZE;

    for (uint64_t b = first_block; b <= last_block; ++b)
    {
        off_t block_offset = (off_t)(b * CACHE_BLOCK_SIZE);
        size_t within_block_offset = 0;
        if (b == first_block)
            within_block_offset = (size_t)((uint64_t)offset % CACHE_BLOCK_SIZE);
        size_t want = CACHE_BLOCK_SIZE - within_block_offset;
        size_t remaining_needed = size - bytes_filled;
        if (want > remaining_needed)
            want = remaining_needed;

        string key = make_block_key(spath, b);

        char tmpblock[CACHE_BLOCK_SIZE];
        size_t got_block_len = 0;
        bool in_cache = cache_get_block(key, tmpblock, got_block_len);

        if (in_cache)
        {
            // cache hit: log and copy
            metrics.log(opcode_to_string(OP_READ), 0, 0, got_block_len, "hit");
        }
        else
        {
            // cache miss: fetch entire block from server
            vector<char> rbuf(CACHE_BLOCK_SIZE);
            size_t server_got = 0;
            int rr = do_read(serverfd, rbuf.data(), CACHE_BLOCK_SIZE, block_offset, &server_got);
            if (rr < 0) return rr;
            cache_put_block(key, rbuf.data(), server_got);
            memcpy(tmpblock, rbuf.data(), server_got);
            got_block_len = server_got;
        }

        size_t avail_in_block = got_block_len > within_block_offset ? (got_block_len - within_block_offset) : 0;
        size_t to_copy = min(avail_in_block, want);
        if (to_copy > 0)
        {
            memcpy(buf + bytes_filled, tmpblock + within_block_offset, to_copy);
            bytes_filled += to_copy;
        }
        if (got_block_len < within_block_offset + want)
        {
            break;
        }
    }

    return (int)bytes_filled;
}

static int nf_write(const char *path, const char *buf, size_t size, off_t offset, struct fuse_file_info *fi)
{
    size_t wrote = 0;
    uint64_t serverfd = (uint64_t)fi->fh;
    int r = do_write(serverfd, buf, size, offset, &wrote);
    if (r < 0)
        return r;

    string spath;
    {
        lock_guard<mutex> lk(fdpath_mtx);
        auto it = fd_to_path.find(serverfd);
        if (it != fd_to_path.end()) spath = it->second; else spath = string(path);
    }
    if (!spath.empty())
    {
        uint64_t first_block = (uint64_t)offset / CACHE_BLOCK_SIZE;
        uint64_t last_block = (uint64_t)(offset + wrote - 1) / CACHE_BLOCK_SIZE;
        for (uint64_t b = first_block; b <= last_block; ++b)
        {
            string key = make_block_key(spath, b);
            cache_erase_key(key);
        }
    }

    return (int)wrote;
}

static int nf_release(const char *path, struct fuse_file_info *fi)
{
    uint64_t serverfd = (uint64_t)fi->fh;

    string spath;
    {
        lock_guard<mutex> lk(fdpath_mtx);
        auto it = fd_to_path.find(serverfd);
        if (it != fd_to_path.end()) { spath = it->second; fd_to_path.erase(it); }
    }
    if (!spath.empty()) cache_invalidate_path(spath);

    return do_release(serverfd);
}

static int nf_unlink(const char *path) { return do_unlink(path); }
static int nf_mkdir(const char *path, mode_t mode) { return do_mkdir(path, mode); }
static int nf_rmdir(const char *path) { return do_rmdir(path); }
static int nf_truncate(const char *path, off_t size, struct fuse_file_info *) { return do_truncate(path, size); }
static int nf_utimens(const char *path, const struct timespec tv[2], struct fuse_file_info *) { return do_utimens(path, tv); }
static int nf_statfs(const char *path, struct statvfs *stbuf) { return do_statfs(path, stbuf); }

static struct fuse_operations nf_ops;

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

    sockfd = connect_to_server(host, port);
    if (sockfd < 0)
        die("connect_to_server");

    vector<char *> fargs;
    fargs.push_back(argv[0]);
    fargs.push_back(const_cast<char *>(mountpoint));
    for (int i = 4; i < argc; ++i) fargs.push_back(argv[i]);
    int fargc = (int)fargs.size();
    fargs.push_back(nullptr);

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

    int ret = fuse_main(fargc, fargs.data(), &nf_ops, nullptr);

    if (sockfd >= 0) { close(sockfd); sockfd = -1; }
    return ret;
}

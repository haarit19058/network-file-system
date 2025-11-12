// nfuse_client_with_cache_and_metrics.cpp
// Rewritten FUSE client with:
// 1. Client-side LRU block cache with a 10-second timeout.
// 2. Per-file-handle, sequential-only, write-behind batching.
// 3. Robust per-request metrics logging (timestamp, opcode, latency_us, bytes_sent, bytes_recv, cache_hit/miss).

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
#include <condition_variable> // NEW: For the connection pool
#include "constants.cpp"

using namespace std;

 // 128 KB chunk size for network ops

// ----------------------------- Operation enum -----------------------------
// This must match the enum in utils.cpp
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
    OP_WRITE_BATCH = 14 // Added for logging batched writes
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
    case OP_WRITE_BATCH: return "WRITE_BATCH"; // Added
    default: return "UNKNOWN";
    }
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

    // Logs a single filesystem operation metric
    void log(const string &opcode, long latency_us, size_t bytes_sent, size_t bytes_recv, const string &cache_status)
    {
        lock_guard<mutex> lk(mtx);
        if (!out.is_open())
            return;
        using namespace chrono;
        auto now = duration_cast<microseconds>(system_clock::now().time_since_epoch()).count();
        out << now << "," << opcode << "," << latency_us << "," << bytes_sent << "," << bytes_recv << "," << cache_status << "\n";
        out.flush(); // Flush to ensure data is written
    }
};

static MetricLogger metrics; // Global metrics logger instance

// --------------------------- Global socket state ---------------------------
// REMOVED:
// static int sockfd = -1;
// static mutex sock_mtx; // Mutex to protect all read/write access to sockfd

// NEW: Connection pool state
static std::vector<int> g_socket_pool;
static std::mutex g_socket_pool_mtx;
static std::condition_variable g_socket_pool_cv;
// static const int POOL_SIZE = 16; // 16 concurrent network connections
static const char *g_server_host = nullptr; // For reconnects
static const char *g_server_port = nullptr; // For reconnects


// ---------------------------- Caching parameters ---------------------------
static const chrono::seconds CACHE_TIMEOUT(10);              // 10-second cache timeout

// ------------------------------- Cache types ------------------------------
// Value stored in the read cache
struct CacheValue
{
    string data; // actual bytes
    size_t size; // data.size()
    chrono::steady_clock::time_point timestamp; // For timeout
};

// LRU list of (key, value) pairs
using LRUList = list<pair<string, CacheValue>>;

static mutex cache_mtx;
static LRUList lru_list; // Holds the cache entries in LRU order (front = newest)
static unordered_map<string, LRUList::iterator> cache_map; // Maps key -> iterator in lru_list
static size_t cache_size_bytes = 0; // Current total size of data in cache

// Map serverfd -> path so we can invalidate by fd on release
static mutex fdpath_mtx;
static unordered_map<uint64_t, string> fd_to_path;

// --------------------------- Attribute Cache -----------------------------
// NEW: Add a cache for file attributes (struct stat)
struct AttrCacheValue
{
    struct stat stbuf;
    chrono::steady_clock::time_point timestamp;
};
static mutex attr_cache_mtx;
static unordered_map<string, AttrCacheValue> attr_cache;


// Evicts least-recently-used items until cache is under capacity
static void cache_evict_if_needed()
{
    while (cache_size_bytes > CACHE_CAPACITY_BYTES && !lru_list.empty())
    {
        // Get the least-recently-used item (back of the list)
        auto it = prev(lru_list.end());
        size_t entry_size = it->second.size;
        
        // Remove from both map and list
        cache_map.erase(it->first);
        cache_size_bytes -= entry_size;
        lru_list.pop_back();
    }
}

// Puts a block of data into the LRU cache
static void cache_put_block(const string &key, const char *data, size_t len)
{
    if (len > CACHE_CAPACITY_BYTES)
        return; // Don't cache blocks larger than the cache itself
    
    lock_guard<mutex> lk(cache_mtx);

    auto it = cache_map.find(key);
    if (it != cache_map.end())
    {
        // --- Update existing entry ---
        auto lit = it->second;
        cache_size_bytes -= lit->second.size; // Subtract old size
        
        // Update data, size, and timestamp
        lit->second.data.assign(data, data + len);
        lit->second.size = len;
        lit->second.timestamp = chrono::steady_clock::now();
        
        cache_size_bytes += len; // Add new size
        
        // Move to front of LRU list
        lru_list.splice(lru_list.begin(), lru_list, lit);
    }
    else
    {
        // --- Add new entry ---
        CacheValue cv;
        cv.data.assign(data, data + len);
        cv.size = len;
        cv.timestamp = chrono::steady_clock::now();
        
        // Add to front of list
        lru_list.emplace_front(key, std::move(cv));
        // Store iterator in map
        cache_map[key] = lru_list.begin();
        cache_size_bytes += len;
    }

    // Evict old entries if we're over capacity
    cache_evict_if_needed();
}

// Tries to read a block from cache. Returns true if present and not stale.
static bool cache_get_block(const string &key, char *out_buf, size_t &out_len)
{
    lock_guard<mutex> lk(cache_mtx);
    auto it = cache_map.find(key);
    if (it == cache_map.end())
        return false; // Not in cache

    // --- Check for staleness ---
    auto age = chrono::steady_clock::now() - it->second->second.timestamp;
    if (age > CACHE_TIMEOUT)
    {
        // Cache entry is stale. Evict it and report a miss.
        cache_size_bytes -= it->second->second.size;
        lru_list.erase(it->second);
        cache_map.erase(it);
        return false; // Stale, treat as miss
    }

    // --- Cache hit and not stale ---
    
    // Move to front of LRU list
    lru_list.splice(lru_list.begin(), lru_list, it->second);
    
    // Copy data out
    const CacheValue &cv = it->second->second;
    memcpy(out_buf, cv.data.data(), cv.size);
    out_len = cv.size;
    
    return true;
}

// Remove all blocks for a full path (prefix matching "path:")
static void cache_invalidate_path(const string &path)
{
    lock_guard<mutex> lk(cache_mtx);
    string prefix = path + ":";
    
    // Iterate and remove all entries whose keys start with "path:"
    for (auto it = lru_list.begin(); it != lru_list.end();)
    {
        if (it->first.rfind(prefix, 0) == 0) // Check for prefix
        {
            cache_size_bytes -= it->second.size;
            cache_map.erase(it->first);
            it = lru_list.erase(it); // Erase and move to next valid iterator
        }
        else
            ++it;
    }
}

// Remove exactly one key (e.g., after a write)
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

// Creates a unique key for a file block, e.g., "/data/file.txt:1"
static string make_block_key(const string &path, uint64_t block_idx)
{
    return path + ":" + to_string(block_idx);
}

// NEW: Invalidate attribute cache for a path
static void attr_cache_invalidate(const string &path)
{
    lock_guard<mutex> lk(attr_cache_mtx);
    attr_cache.erase(path);
}

// --------------------------- Write Batching State --------------------------
// static const size_t BATCH_WRITE_THRESHOLD = 1 * 1024 * 1024; // 1 MB
static mutex g_write_buffer_mtx;
// We use std::string as a dynamic byte buffer
static unordered_map<uint64_t, string> g_write_buffers;
static unordered_map<uint64_t, off_t> g_write_buffer_start_offsets;

// Forward declaration
static int do_batch_write(uint64_t serverfd, const char *buf, size_t size, off_t offset, size_t *out_written);

// Flushes the write buffer for a given file handle
// This function MUST be called with g_write_buffer_mtx *unlocked* or with lock=false if already locked.
static int flush_write_buffer(uint64_t serverfd, bool lock = true)
{
    // This unique_lock will be conditionally acquired
    std::unique_lock<mutex> lk(g_write_buffer_mtx, std::defer_lock);
    if (lock) {
        lk.lock();
    }

    auto it = g_write_buffers.find(serverfd);
    if (it == g_write_buffers.end() || it->second.empty())
    {
        return 0; // Nothing to flush
    }

    cout << "  Flushing write buffer for fd=" << serverfd 
         << " (size=" << it->second.size() 
         << ", offset=" << g_write_buffer_start_offsets[serverfd] << ")" << endl;

    // --- Invalidate caches *before* writing ---
    // This ensures read-your-writes consistency.
    string spath;
    {
        lock_guard<mutex> lk_fd(fdpath_mtx);
        auto it_path = fd_to_path.find(serverfd);
        if (it_path != fd_to_path.end())
            spath = it_path->second;
    }

    if (!spath.empty())
    {
        off_t start_offset = g_write_buffer_start_offsets[serverfd];
        off_t end_offset = start_offset + it->second.size();
        uint64_t first_block = (uint64_t)start_offset / CACHE_BLOCK_SIZE;
        uint64_t last_block = (uint64_t)(end_offset - 1) / CACHE_BLOCK_SIZE;

        for (uint64_t b = first_block; b <= last_block; ++b)
        {
            string key = make_block_key(spath, b);
            cache_erase_key(key);
        }
        // A successful write also changes file attributes (size, mtime)
        attr_cache_invalidate(spath);
    }
    
    // --- Send data to server ---
    size_t wrote = 0;
    int r = do_batch_write(serverfd, 
                         it->second.data(), 
                         it->second.size(), 
                         g_write_buffer_start_offsets[serverfd], 
                         &wrote);

    // --- Clear buffer ---
    it->second.clear();
    g_write_buffer_start_offsets.erase(serverfd);
    g_write_buffers.erase(it);

    return (r < 0) ? r : 0;
}

// NEW: Gets a socket from the pool, waiting if none are available
static int get_socket()
{
    std::unique_lock<std::mutex> lk(g_socket_pool_mtx);
    // Wait until the pool is not empty
    g_socket_pool_cv.wait(lk, []{ return !g_socket_pool.empty(); });

    int s = g_socket_pool.back();
    g_socket_pool.pop_back();
    return s;
}

// NEW: Returns a socket to the pool and notifies a waiting thread
static void release_socket(int s)
{
    std::lock_guard<std::mutex> lk(g_socket_pool_mtx);
    g_socket_pool.push_back(s);
    g_socket_pool_cv.notify_one(); // Wake up one waiting thread
}

// NEW: RAII helper to manage socket checkout/check-in
// This also handles socket errors by closing the bad socket
// and replacing it with a new one.
class SocketGuard
{
    int s_ = -1;
    bool had_error_ = false;

public:
    SocketGuard() {
        s_ = get_socket();
    }

    ~SocketGuard() {
        if (s_ < 0) return; // No socket was acquired

        if (had_error_) {
            // Socket is bad, close it
            cout << "Socket error detected. Closing and reconnecting." << endl;
            ::close(s_);
            // Try to add a new connection to the pool to replace it
            int new_s = connect_to_server(g_server_host, g_server_port);
            if (new_s >= 0) {
                release_socket(new_s);
            } else {
                cerr << "Failed to reconnect to server to replace bad socket." << endl;
                // Pool size will shrink, which is acceptable.
            }
        } else {
            // Socket is good, return it to the pool
            release_socket(s_);
        }
    }

    int get() { return s_; }
    void invalidate() { had_error_ = true; }
};



// Reads exactly n bytes from fd into buf
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
            return -1; // Error
        }
        if (r == 0)
            return 0; // EOF
        left -= r;
        p += r;
    }
    return (int)n;
}

// Writes exactly n bytes from buf to fd
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
            return -1; // Error
        }
        if (w == 0)
            return 0; // Should not happen for sockets
        left -= w;
        p += w;
    }
    return (int)n;
}

// Core network protocol function.
// Sends a 4-byte BE length + payload.
// Receives a 4-byte BE status + 4-byte BE dlen + data.
// Returns 0 on success.
// On success, logs metrics and populates out_latency_us, out_bytes_sent, out_bytes_recv.
static int send_frame_and_recv(uint32_t opcode, const void *payload, uint32_t payload_len, 
                               vector<char> &out_resp_data,
                               long &out_latency_us, size_t &out_bytes_sent, size_t &out_bytes_recv)
{
    using namespace chrono;
    auto start = high_resolution_clock::now();

    // MODIFIED: Use the SocketGuard to get a socket from the pool
    SocketGuard sock_guard;
    int s = sock_guard.get();
    if (s < 0) return -EIO; // Should not happen if pool is managed

    // REMOVED: unique_lock<mutex> lk(sock_mtx);

    // --- Send request ---
    // 4-byte length prefix (opcode + payload)
    uint32_t total_len = 4 + payload_len;
    uint32_t len_be = htonl(total_len);
    if (writen(s, &len_be, sizeof(len_be)) != (int)sizeof(len_be)) {
        sock_guard.invalidate(); // Mark socket as bad
        return -EIO;
    }
    
    // 4-byte opcode
    uint32_t op_be = htonl(opcode);
    if (writen(s, &op_be, sizeof(op_be)) != (int)sizeof(op_be)) {
        sock_guard.invalidate();
        return -EIO;
    }

    // Payload
    if (payload_len > 0 && writen(s, payload, payload_len) != (int)payload_len) {
        sock_guard.invalidate();
        return -EIO;
    }

    // --- Receive response ---
    uint32_t status_be, dlen_be;
    // 4-byte status
    if (readn(s, &status_be, sizeof(status_be)) != (int)sizeof(status_be)) {
        sock_guard.invalidate();
        return -EIO;
    }
    // 4-byte data length
    if (readn(s, &dlen_be, sizeof(dlen_be)) != (int)sizeof(dlen_be)) {
        sock_guard.invalidate();
        return -EIO;
    }
    
    uint32_t status = ntohl(status_be);
    uint32_t dlen = ntohl(dlen_be);

    // Resize output vector to hold status + data
    out_resp_data.resize(4 + dlen);
    memcpy(out_resp_data.data(), &status_be, 4); // Copy status in

    // Read data payload if present
    if (dlen && readn(s, out_resp_data.data() + 4, dlen) != (int)dlen) {
        sock_guard.invalidate();
        return -EIO;
    }

    // REMOVED: lk.unlock(); // Unlock socket
    
    auto end = high_resolution_clock::now();
    out_latency_us = duration_cast<microseconds>(end - start).count();
    
    // Calculate RTT bytes
    out_bytes_sent = sizeof(len_be) + sizeof(op_be) + payload_len; // 4 + 4 + payload
    out_bytes_recv = sizeof(status_be) + sizeof(dlen_be) + dlen;   // 4 + 4 + dlen

    // Return the status code (0 for success, errno otherwise)
    return (int)status;
}

// ------------------------ High level operation helpers ------------------------
// These helpers format the request payload, call send_frame_and_recv, 
// log metrics, and parse the response.

static int do_getattr(const char *path, struct stat *stbuf)
{
    string p(path);
    uint32_t pathlen_be = htonl(static_cast<uint32_t>(p.size()));

    vector<char> payload;
    payload.insert(payload.end(), reinterpret_cast<char *>(&pathlen_be), reinterpret_cast<char *>(&pathlen_be) + 4);
    payload.insert(payload.end(), p.begin(), p.end());

    vector<char> resp;
    long latency_us = 0;
    size_t bytes_sent = 0, bytes_recv = 0;
    
    int status = send_frame_and_recv(OP_GETATTR, payload.data(), (uint32_t)payload.size(), resp, latency_us, bytes_sent, bytes_recv);
    
    // Log the network request
    metrics.log(opcode_to_string(OP_GETATTR), latency_us, bytes_sent, bytes_recv, "none");

    if (status != 0) return -status; // Return -errno

    if (resp.size() < 4 + sizeof(struct stat))
        return -EIO;
        
    memcpy(stbuf, resp.data() + 4, sizeof(struct stat));
    return 0; // Success
}

static int do_readdir(const char *path, void *buf, fuse_fill_dir_t filler)
{
    string p(path);
    uint32_t pathlen_be = htonl(static_cast<uint32_t>(p.size()));

    vector<char> payload;
    payload.insert(payload.end(), reinterpret_cast<char *>(&pathlen_be), reinterpret_cast<char *>(&pathlen_be) + 4);
    payload.insert(payload.end(), p.begin(), p.end());

    vector<char> resp;
    long latency_us = 0;
    size_t bytes_sent = 0, bytes_recv = 0;
    
    int status = send_frame_and_recv(OP_READDIR, payload.data(), (uint32_t)payload.size(), resp, latency_us, bytes_sent, bytes_recv);

    metrics.log(opcode_to_string(OP_READDIR), latency_us, bytes_sent, bytes_recv, "none");

    if (status != 0) return -status;

    // Parse the 0-terminated string blob
    const char *ptr = resp.data() + 4;
    size_t len = resp.size() - 4;
    size_t i = 0;
    while (i < len)
    {
        const char *name = ptr + i;
        size_t nlen = strlen(name);
        if (nlen == 0) { // Should only happen at the end, but good to check
             ++i;
             continue;
        }
        filler(buf, name, NULL, 0, static_cast<fuse_fill_dir_flags>(0));
        i += nlen + 1; // Move past name and null terminator
    }
    return 0;
}

static int do_open_or_create(const char *path, int flags, int mode, bool create, uint64_t &out_serverfd)
{
    string p(path);
    uint32_t op = create ? OP_CREATE : OP_OPEN;
    uint32_t pathlen_be = htonl(static_cast<uint32_t>(p.size()));
    uint32_t flags_be = htonl(static_cast<uint32_t>(flags));

    vector<char> payload;
    payload.insert(payload.end(), reinterpret_cast<char *>(&pathlen_be), reinterpret_cast<char *>(&pathlen_be) + 4);
    payload.insert(payload.end(), p.begin(), p.end());
    payload.insert(payload.end(), reinterpret_cast<char *>(&flags_be), reinterpret_cast<char *>(&flags_be) + 4);

    if (create)
    {
        uint32_t mode_be = htonl(static_cast<uint32_t>(mode));
        payload.insert(payload.end(), reinterpret_cast<char *>(&mode_be), reinterpret_cast<char *>(&mode_be) + 4);
    }

    vector<char> resp;
    long latency_us = 0;
    size_t bytes_sent = 0, bytes_recv = 0;
    int status = send_frame_and_recv(op, payload.data(), (uint32_t)payload.size(), resp, latency_us, bytes_sent, bytes_recv);

    metrics.log(opcode_to_string(op), latency_us, bytes_sent, bytes_recv, "none");

    if (status != 0) return -status;

    if (resp.size() < 4 + 8) return -EIO;
    
    uint64_t fdbe;
    memcpy(&fdbe, resp.data() + 4, 8);
    out_serverfd = be64toh(fdbe);

    // Store path for this fd for cache invalidation
    {
        lock_guard<mutex> lk(fdpath_mtx);
        fd_to_path[out_serverfd] = p;
    }
    return 0;
}

// Network read helper
static int do_read(uint64_t serverfd, char *buf, size_t size, off_t offset, size_t *out_read)
{
    size_t total_read = 0;

    // We must chunk reads to respect our CHUNK_SIZE protocol limit
    while (total_read < size)
    {
        size_t this_chunk = std::min((size_t)CHUNK_SIZE, size - total_read);

        uint64_t fd_be = htobe64(serverfd);
        uint64_t off_be = htobe64((uint64_t)(offset + total_read));
        uint32_t size_be = htonl(static_cast<uint32_t>(this_chunk));

        vector<char> payload;
        payload.insert(payload.end(), reinterpret_cast<char *>(&fd_be), reinterpret_cast<char *>(&fd_be) + 8);
        payload.insert(payload.end(), reinterpret_cast<char *>(&off_be), reinterpret_cast<char *>(&off_be) + 8);
        payload.insert(payload.end(), reinterpret_cast<char *>(&size_be), reinterpret_cast<char *>(&size_be) + 4);

        vector<char> resp;
        long latency_us = 0;
        size_t bytes_sent = 0, bytes_recv = 0;
        int status = send_frame_and_recv(OP_READ, payload.data(), (uint32_t)payload.size(), resp, latency_us, bytes_sent, bytes_recv);

        // mark this network-backed read as a cache MISS for metrics purposes
        metrics.log(opcode_to_string(OP_READ), latency_us, bytes_sent, bytes_recv, "miss");
        
        if (status != 0) return -status;

        size_t dlen = resp.size() - 4;
        if (dlen == 0)
            break; // Server returned 0 bytes (EOF)

        if (buf)
            memcpy(buf + total_read, resp.data() + 4, dlen);

        total_read += dlen;
        
        // If server returned less than we asked for, we hit EOF
        if (dlen < this_chunk)
            break;
    }

    *out_read = total_read;
    return 0;
}

// Network write helper (for regular, non-batched writes)
static int do_write(uint64_t serverfd, const char *buf, size_t size, off_t offset, size_t *out_written)
{
    size_t total_written = 0;

    // We must chunk writes to respect our CHUNK_SIZE protocol limit
    while (total_written < size)
    {
        size_t this_chunk = std::min((size_t)CHUNK_SIZE, size - total_written);

        uint64_t fd_be = htobe64(serverfd);
        uint64_t off_be = htobe64((uint64_t)(offset + total_written));
        uint32_t size_be = htonl(static_cast<uint32_t>(this_chunk));

        vector<char> payload;
        payload.insert(payload.end(), reinterpret_cast<char *>(&fd_be), reinterpret_cast<char *>(&fd_be) + 8);
        payload.insert(payload.end(), reinterpret_cast<char *>(&off_be), reinterpret_cast<char *>(&off_be) + 8);
        payload.insert(payload.end(), reinterpret_cast<char *>(&size_be), reinterpret_cast<char *>(&size_be) + 4);
        payload.insert(payload.end(), buf + total_written, buf + total_written + this_chunk);

        vector<char> resp;
        long latency_us = 0;
        size_t bytes_sent = 0, bytes_recv = 0;
        int status = send_frame_and_recv(OP_WRITE, payload.data(), (uint32_t)payload.size(), resp, latency_us, bytes_sent, bytes_recv);

        metrics.log(opcode_to_string(OP_WRITE), latency_us, bytes_sent, bytes_recv, "none");

        if (status != 0) return -status;
        
        if (resp.size() < 4 + 4) return -EIO; // Must have status + bytes_written
        
        uint32_t wrote_be;
        memcpy(&wrote_be, resp.data() + 4, 4);
        uint32_t wrote = ntohl(wrote_be);

        total_written += wrote;
        
        // If server wrote less than we sent, something is wrong (e.g., disk full)
        if (wrote < this_chunk)
            break;
    }

    *out_written = total_written;
    return 0;
}

// Network write helper for BATCHED writes (uses different opcode for logging)
static int do_batch_write(uint64_t serverfd, const char *buf, size_t size, off_t offset, size_t *out_written)
{
    size_t total_written = 0;

    // Chunking logic is identical to do_write
    while (total_written < size)
    {
        size_t this_chunk = std::min((size_t)CHUNK_SIZE, size - total_written);

        uint64_t fd_be = htobe64(serverfd);
        uint64_t off_be = htobe64((uint64_t)(offset + total_written));
        uint32_t size_be = htonl(static_cast<uint32_t>(this_chunk));

        vector<char> payload;
        payload.insert(payload.end(), reinterpret_cast<char *>(&fd_be), reinterpret_cast<char *>(&fd_be) + 8);
        payload.insert(payload.end(), reinterpret_cast<char *>(&off_be), reinterpret_cast<char *>(&off_be) + 8);
        payload.insert(payload.end(), reinterpret_cast<char *>(&size_be), reinterpret_cast<char *>(&size_be) + 4);
        payload.insert(payload.end(), buf + total_written, buf + total_written + this_chunk);

        vector<char> resp;
        long latency_us = 0;
        size_t bytes_sent = 0, bytes_recv = 0;
        // *** Use OP_WRITE_BATCH here ***
        int status = send_frame_and_recv(OP_WRITE_BATCH, payload.data(), (uint32_t)payload.size(), resp, latency_us, bytes_sent, bytes_recv);

        // *** Log as WRITE_BATCH ***
        metrics.log(opcode_to_string(OP_WRITE_BATCH), latency_us, bytes_sent, bytes_recv, "none");
        
        if (status != 0) return -status;
        if (resp.size() < 4 + 4) return -EIO;
        
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
    uint64_t fd_be = htobe64(serverfd);

    vector<char> payload;
    payload.insert(payload.end(), reinterpret_cast<char *>(&fd_be), reinterpret_cast<char *>(&fd_be) + 8);

    vector<char> resp;
    long latency_us = 0;
    size_t bytes_sent = 0, bytes_recv = 0;
    int status = send_frame_and_recv(OP_RELEASE, payload.data(), (uint32_t)payload.size(), resp, latency_us, bytes_sent, bytes_recv);

    metrics.log(opcode_to_string(OP_RELEASE), latency_us, bytes_sent, bytes_recv, "none");

    if (status != 0) return -status;
    return 0;
}

static int do_unlink(const char *path)
{
    string p(path);
    uint32_t pathlen_be = htonl(static_cast<uint32_t>(p.size()));

    vector<char> payload;
    payload.insert(payload.end(), reinterpret_cast<char *>(&pathlen_be), reinterpret_cast<char *>(&pathlen_be) + 4);
    payload.insert(payload.end(), p.begin(), p.end());

    vector<char> resp;
    long latency_us = 0;
    size_t bytes_sent = 0, bytes_recv = 0;
    int status = send_frame_and_recv(OP_UNLINK, payload.data(), (uint32_t)payload.size(), resp, latency_us, bytes_sent, bytes_recv);

    metrics.log(opcode_to_string(OP_UNLINK), latency_us, bytes_sent, bytes_recv, "none");
    
    if (status == 0)
    {
        // On successful unlink, invalidate all cache entries for this path
        cache_invalidate_path(p);
        // MODIFIED: Also invalidate attributes
        attr_cache_invalidate(p);
    }

    return -status;
}

static int do_mkdir(const char *path, mode_t mode)
{
    string p(path);
    uint32_t pathlen_be = htonl(static_cast<uint32_t>(p.size()));
    uint32_t mode_be = htonl(static_cast<uint32_t>(mode));

    vector<char> payload;
    payload.insert(payload.end(), reinterpret_cast<char *>(&pathlen_be), reinterpret_cast<char *>(&pathlen_be) + 4);
    payload.insert(payload.end(), p.begin(), p.end());
    payload.insert(payload.end(), reinterpret_cast<char *>(&mode_be), reinterpret_cast<char *>(&mode_be) + 4);

    vector<char> resp;
    long latency_us = 0;
    size_t bytes_sent = 0, bytes_recv = 0;
    int status = send_frame_and_recv(OP_MKDIR, payload.data(), (uint32_t)payload.size(), resp, latency_us, bytes_sent, bytes_recv);

    metrics.log(opcode_to_string(OP_MKDIR), latency_us, bytes_sent, bytes_recv, "none");

    return -status;
}

static int do_rmdir(const char *path)
{
    string p(path);
    uint32_t pathlen_be = htonl(static_cast<uint32_t>(p.size()));

    vector<char> payload;
    payload.insert(payload.end(), reinterpret_cast<char *>(&pathlen_be), reinterpret_cast<char *>(&pathlen_be) + 4);
    payload.insert(payload.end(), p.begin(), p.end());

    vector<char> resp;
    long latency_us = 0;
    size_t bytes_sent = 0, bytes_recv = 0;
    int status = send_frame_and_recv(OP_RMDIR, payload.data(), (uint32_t)payload.size(), resp, latency_us, bytes_sent, bytes_recv);

    metrics.log(opcode_to_string(OP_RMDIR), latency_us, bytes_sent, bytes_recv, "none");

    return -status;
}

static int do_truncate(const char *path, off_t size)
{
    string p(path);
    uint32_t pathlen_be = htonl(static_cast<uint32_t>(p.size()));
    uint64_t size_be = htobe64((uint64_t)size);

    vector<char> payload;
    payload.insert(payload.end(), reinterpret_cast<char *>(&pathlen_be), reinterpret_cast<char *>(&pathlen_be) + 4);
    payload.insert(payload.end(), p.begin(), p.end());
    payload.insert(payload.end(), reinterpret_cast<char *>(&size_be), reinterpret_cast<char *>(&size_be) + 8);

    vector<char> resp;
    long latency_us = 0;
    size_t bytes_sent = 0, bytes_recv = 0;
    int status = send_frame_and_recv(OP_TRUNCATE, payload.data(), (uint32_t)payload.size(), resp, latency_us, bytes_sent, bytes_recv);

    metrics.log(opcode_to_string(OP_TRUNCATE), latency_us, bytes_sent, bytes_recv, "none");

    if (status == 0)
    {
        // Truncate invalidates all cache entries for this path
        cache_invalidate_path(p);
        // MODIFIED: Also invalidate attributes
        attr_cache_invalidate(p);
    }
    
    return -status;
}

static int do_utimens(const char *path, const struct timespec tv[2])
{
    string p(path);
    uint32_t pathlen_be = htonl(static_cast<uint32_t>(p.size()));
    uint64_t at_sec = htobe64((uint64_t)tv[0].tv_sec);
    uint64_t at_nsec = htobe64((uint64_t)tv[0].tv_nsec);
    uint64_t mt_sec = htobe64((uint64_t)tv[1].tv_sec);
    uint64_t mt_nsec = htobe64((uint64_t)tv[1].tv_nsec);

    vector<char> payload;
    payload.insert(payload.end(), reinterpret_cast<char *>(&pathlen_be), reinterpret_cast<char *>(&pathlen_be) + 4);
    payload.insert(payload.end(), p.begin(), p.end());
    payload.insert(payload.end(), reinterpret_cast<char *>(&at_sec), reinterpret_cast<char *>(&at_sec) + 8);
    payload.insert(payload.end(), reinterpret_cast<char *>(&at_nsec), reinterpret_cast<char *>(&at_nsec) + 8);
    payload.insert(payload.end(), reinterpret_cast<char *>(&mt_sec), reinterpret_cast<char *>(&mt_sec) + 8);
    payload.insert(payload.end(), reinterpret_cast<char *>(&mt_nsec), reinterpret_cast<char *>(&mt_nsec) + 8);

    vector<char> resp;
    long latency_us = 0;
    size_t bytes_sent = 0, bytes_recv = 0;
    int status = send_frame_and_recv(OP_UTIMENS, payload.data(), (uint32_t)payload.size(), resp, latency_us, bytes_sent, bytes_recv);

    metrics.log(opcode_to_string(OP_UTIMENS), latency_us, bytes_sent, bytes_recv, "none");

    // MODIFIED: Invalidate attributes on success
    if (status == 0)
    {
        // Utimens invalidates attributes
        attr_cache_invalidate(p);
    }
    return -status;
}

static int do_statfs(const char *path, struct statvfs *stbuf)
{
    string p(path);
    uint32_t pathlen_be = htonl(static_cast<uint32_t>(p.size()));

    vector<char> payload;
    payload.insert(payload.end(), reinterpret_cast<char *>(&pathlen_be), reinterpret_cast<char *>(&pathlen_be) + 4);
    payload.insert(payload.end(), p.begin(), p.end());

    vector<char> resp;
    long latency_us = 0;
    size_t bytes_sent = 0, bytes_recv = 0;
    int status = send_frame_and_recv(OP_STATFS, payload.data(), (uint32_t)payload.size(), resp, latency_us, bytes_sent, bytes_recv);

    metrics.log(opcode_to_string(OP_STATFS), latency_us, bytes_sent, bytes_recv, "none");

    if (status != 0) return -status;
    
    if (resp.size() < 4 + sizeof(struct statvfs)) return -EIO;
        
    memcpy(stbuf, resp.data() + 4, sizeof(struct statvfs));
    return 0;
}

// ------------------------------ FUSE callbacks ------------------------------
// These are the functions FUSE calls for filesystem operations.

// MODIFIED: Replaced with a cached version
static int nf_getattr(const char *path, struct stat *stbuf, struct fuse_file_info *)
{
    cout << "FUSE: getattr(path=" << path << ")" << endl;
    string spath(path);

    // --- 1. Try attribute cache first ---
    {
        lock_guard<mutex> lk(attr_cache_mtx);
        auto it = attr_cache.find(spath);
        if (it != attr_cache.end())
        {
            // Check for staleness
            auto age = chrono::steady_clock::now() - it->second.timestamp;
            if (age <= CACHE_TIMEOUT)
            {
                // Cache hit and not stale
                cout << "  [Attr cache hit] path=" << path << endl;
                memcpy(stbuf, &it->second.stbuf, sizeof(struct stat));
                return 0;
            }
            else
            {
                // Stale, evict
                attr_cache.erase(it);
            }
        }
    }

    // --- 2. Cache miss or stale, fetch from server ---
    cout << "  [Attr cache miss] path=" << path << " -> fetching from server..." << endl;
    int r = do_getattr(path, stbuf);
    if (r == 0)
    {
        // --- 3. Store in cache on success ---
        AttrCacheValue acv;
        memcpy(&acv.stbuf, stbuf, sizeof(struct stat));
        acv.timestamp = chrono::steady_clock::now();
        {
            lock_guard<mutex> lk(attr_cache_mtx);
            attr_cache[spath] = acv;
        }
    }
    return r;
}

static int nf_readdir(const char *path, void *buf, fuse_fill_dir_t filler, off_t, struct fuse_file_info *, fuse_readdir_flags)
{
    cout << "FUSE: readdir(path=" << path << ")" << endl;
    return do_readdir(path, buf, filler);
}

static int nf_open(const char *path, struct fuse_file_info *fi)
{
    cout << "FUSE: open(path=" << path << ", flags=" << fi->flags << ")" << endl;
    uint64_t serverfd = 0;
    fi->direct_io = 0; // Tell FUSE we don't want its page caching
    int r = do_open_or_create(path, fi->flags, 0644, false, serverfd);
    if (r < 0)
        return r;
    fi->fh = serverfd; // Store server's FD in the file handle
    return 0;
}

static int nf_create(const char *path, mode_t mode, struct fuse_file_info *fi)
{
    cout << "FUSE: create(path=" << path << ", mode=" << mode << ", flags=" << fi->flags << ")" << endl;
    uint64_t serverfd = 0;
    fi->direct_io = 0;
    int r = do_open_or_create(path, fi->flags, mode, true, serverfd);
    if (r < 0)
        return r;
    fi->fh = serverfd;
    return 0;
}

// MODIFIED: Implemented read-ahead logic on cache miss
static int nf_read(const char *path, char *buf, size_t size, off_t offset, struct fuse_file_info *fi)
{
    cout << "FUSE: read(path=" << path << ", size=" << size << ", offset=" << offset << ")" << endl;
    if (size == 0) return 0;

    string spath(path);
    uint64_t serverfd = (uint64_t)fi->fh;

    size_t bytes_filled = 0;
    uint64_t first_block = offset / CACHE_BLOCK_SIZE;
    uint64_t last_block  = (offset + size - 1) / CACHE_BLOCK_SIZE;

    // This loop iterates over the blocks the *user* is requesting
    for (uint64_t b = first_block; b <= last_block; ++b)
    {
        off_t block_start_offset = b * CACHE_BLOCK_SIZE;
        size_t within_block_offset = (b == first_block) ? (offset % CACHE_BLOCK_SIZE) : 0;
        size_t want_from_this_block = min(CACHE_BLOCK_SIZE - within_block_offset, size - bytes_filled);

        string key = make_block_key(spath, b);

        char tmpblock[CACHE_BLOCK_SIZE];
        size_t got_block_len = 0;

        // ---- Try cache first ----
        bool in_cache = cache_get_block(key, tmpblock, got_block_len);

        if (in_cache)
        {
            // Cache Hit
            cout << "  [Cache hit] key=" << key << " len=" << got_block_len << endl;
            metrics.log(opcode_to_string(OP_READ), 0, 0, 0, "hit");
        }
        else
        {
            // Cache Miss
            cout << "  [Cache miss] key=" << key << " -> initiating read-ahead..." << endl;

            // --- READ-AHEAD LOGIC (REVISED) ---
            // On a miss, always try to read a full CHUNK_SIZE (128KB)
            // starting from the beginning of the block that was missed.
            // This pre-fetches subsequent blocks for future reads.
            off_t readahead_offset = block_start_offset;
            // size_t READAHEAD_SIZE = CHUNK_SIZE; // Always ask for 128KB

            cout << "  Aggressive Re-ahead: requesting " << READAHEAD_SIZE << " bytes from offset " << readahead_offset << endl;

            vector<char> readahead_buf(READAHEAD_SIZE);
            size_t server_got = 0;

            int rr = do_read(serverfd, readahead_buf.data(), READAHEAD_SIZE, readahead_offset, &server_got);
            if (rr < 0) return rr;
            if (server_got == 0) break; // EOF

            // --- POPULATE CACHE ---
            // Now, chunk the readahead_buf and populate the cache
            size_t bytes_cached = 0;
            uint64_t current_block_idx = b;
            while (bytes_cached < server_got)
            {
                size_t chunk_to_cache = min(CACHE_BLOCK_SIZE,(int)(server_got - bytes_cached));
                string current_key = make_block_key(spath, current_block_idx);
                
                cout << "  Populating cache for key=" << current_key << " len=" << chunk_to_cache << endl;
                cache_put_block(current_key, readahead_buf.data() + bytes_cached, chunk_to_cache);
                
                bytes_cached += chunk_to_cache;
                current_block_idx++;
            }

            // --- GET DATA FOR *THIS* BLOCK ---
            // Now that the cache is populated, get the block we originally missed.
            // This is now guaranteed to be a cache hit.
            in_cache = cache_get_block(key, tmpblock, got_block_len);
            if (!in_cache) {
                // This should never happen if logic is correct
                cerr << "FATAL: Read-ahead failed to cache block " << key << endl;
                return -EIO;
            }
        }

        // ---- Copy requested range from block into user buffer ----
        
        size_t avail_in_block = (got_block_len > within_block_offset)
                                    ? (got_block_len - within_block_offset)
                                    : 0;

        size_t to_copy = min(avail_in_block, want_from_this_block);
        if (to_copy > 0)
        {
            memcpy(buf + bytes_filled, tmpblock + within_block_offset, to_copy);
            bytes_filled += to_copy;
        }

        if (to_copy < want_from_this_block)
            break; // Hit EOF (short read)
    }

    cout << "  Total bytes read: " << bytes_filled << endl;
    return static_cast<int>(bytes_filled);
}


// Implements write-behind batching
static int nf_write(const char *path, const char *buf, size_t size, off_t offset, struct fuse_file_info *fi)
{
    cout << "FUSE: write(path=" << path << ", size=" << size << ", offset=" << offset << ")" << endl;
    uint64_t serverfd = (uint64_t)fi->fh;
    int r = 0;

    lock_guard<mutex> lk(g_write_buffer_mtx);
    
    auto it = g_write_buffers.find(serverfd);
    bool sequential = false;
    
    if (it != g_write_buffers.end())
    {
        // Buffer exists, check if this write is sequential
        off_t next_expected_offset = g_write_buffer_start_offsets[serverfd] + it->second.size();
        if (offset == next_expected_offset)
        {
            sequential = true;
        }
    }
    else
    {
        // No buffer exists, this is the start of a new batch
        sequential = true;
        g_write_buffers[serverfd] = "";
        g_write_buffer_start_offsets[serverfd] = offset;
    }

    if (!sequential)
    {
        cout << "  Non-sequential write detected. Flushing buffer first." << endl;
        // This is a seek. Flush the old buffer before starting a new one.
        // We pass lock=false because we already hold the mutex.
        r = flush_write_buffer(serverfd, false); 
        
        // Start new buffer
        g_write_buffers[serverfd] = "";
        g_write_buffer_start_offsets[serverfd] = offset;
    }
    
    if (r < 0) return r; // Error during flush

    // Append new data to the batch
    g_write_buffers[serverfd].append(buf, size);
    
    // Check if buffer is full
    if (g_write_buffers[serverfd].size() >= BATCH_WRITE_THRESHOLD)
    {
        cout << "  Write buffer threshold reached. Flushing." << endl;
        r = flush_write_buffer(serverfd, false);
    }
    
    // We *always* return success (size) because the write is buffered.
    // Errors will be reported on fsync, release, or the *next* write.
    return (r < 0) ? r : (int)size;
}

static int nf_fsync(const char *path, int, struct fuse_file_info *fi)
{
    cout << "FUSE: fsync(path=" << path << ")" << endl;
    uint64_t serverfd = (uint64_t)fi->fh;
    
    // fsync *must* flush the write buffer
    return flush_write_buffer(serverfd);
}

static int nf_release(const char *path, struct fuse_file_info *fi)
{
    cout << "FUSE: release(path=" << path << ")" << endl;
    uint64_t serverfd = (uint64_t)fi->fh;
    
    // --- 1. Flush any pending writes ---
    // This is critical. We must write all buffered data before closing.
    int flush_err = flush_write_buffer(serverfd);
    
    // --- 2. Invalidate read cache ---
    string spath;
    {
        lock_guard<mutex> lk(fdpath_mtx);
        auto it = fd_to_path.find(serverfd);
        if (it != fd_to_path.end())
        {
            spath = it->second;
            fd_to_path.erase(it); // Clean up fd->path map
        }
    }
    if (!spath.empty())
    {
        cache_invalidate_path(spath); // Invalidate all read blocks for this path
        // MODIFIED: Also invalidate attributes on release
        attr_cache_invalidate(spath);
    }

    // --- 3. Tell server to release the FD ---
    int release_err = do_release(serverfd);
    
    // Prioritize returning the flush error if one occurred
    return (flush_err < 0) ? flush_err : release_err;
}

static int nf_unlink(const char *path) 
{ 
    cout << "FUSE: unlink(path=" << path << ")" << endl;
    return do_unlink(path); 
}
static int nf_mkdir(const char *path, mode_t mode) 
{ 
    cout << "FUSE: mkdir(path=" << path << ")" << endl;
    return do_mkdir(path, mode); 
}
static int nf_rmdir(const char *path) 
{ 
    cout << "FUSE: rmdir(path=" << path << ")" << endl;
    return do_rmdir(path); 
}
// MODIFIED: Fixed critical bug with write-batching
static int nf_truncate(const char *path, off_t size, struct fuse_file_info *) 
{ 
    cout << "FUSE: truncate(path=" << path << ", size=" << size << ")" << endl;
    string spath(path);

    // --- 1. Flush all open write buffers for this path ---
    // This is critical to prevent stale buffered writes from
    // overwriting the truncate.
    vector<uint64_t> fds_to_flush;
    {
        lock_guard<mutex> lk(fdpath_mtx);
        for (auto const& [fd, p] : fd_to_path)
        {
            if (p == spath)
            {
                fds_to_flush.push_back(fd);
            }
        }
    }
    
    cout << "  Truncate: found " << fds_to_flush.size() << " open handles for this path. Flushing..." << endl;
    for (uint64_t fd : fds_to_flush)
    {
        // This will lock g_write_buffer_mtx
        int r = flush_write_buffer(fd); 
        if (r < 0) return r; // Report flush error
    }

    // --- 2. Send truncate op (which also invalidates caches) ---
    // do_truncate will invalidate both block and attribute caches on success
    return do_truncate(path, size); 
}
static int nf_utimens(const char *path, const struct timespec tv[2], struct fuse_file_info *) 
{ 
    cout << "FUSE: utimens(path=" << path << ")" << endl;
    return do_utimens(path, tv); 
}
static int nf_statfs(const char *path, struct statvfs *stbuf) 
{ 
    cout << "FUSE: statfs(path=" << path << ")" << endl;
    return do_statfs(path, stbuf); 
}

static struct fuse_operations nf_ops;

void set_fuse_ops()
{
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
    nf_ops.fsync = nf_fsync; // Register fsync
}

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

    // MODIFIED: Store host/port and initialize connection pool
    g_server_host = host;
    g_server_port = port;

    printf("Connecting to server at %s:%s and filling connection pool (size=%d)...\n", host, port, POOL_SIZE);
    for (int i = 0; i < POOL_SIZE; ++i)
    {
        int s = connect_to_server(host, port);
        if (s < 0) {
            die("connect_to_server (during pool init)");
        }
        g_socket_pool.push_back(s);
    }
    printf("Connection pool filled.\n");

    // REMOVED:
    // sockfd = connect_to_server(host, port);
    // if (sockfd < 0)
    //    die("connect_to_server");
    // printf("Connected to server at %s:%s\n", host, port);
    
    // Prepare FUSE arguments
    vector<char *> fargs;
    fargs.push_back(argv[0]); // Program name
    // Add any FUSE-specific args *before* the mountpoint
    for (int i = 4; i < argc; ++i)
        fargs.push_back(argv[i]);
    fargs.push_back(const_cast<char *>(mountpoint)); // Mountpoint
    
    int fargc = (int)fargs.size();

    // Set up the operations table
    set_fuse_ops();

    // Start the FUSE main loop
    int ret = fuse_main(fargc, fargs.data(), &nf_ops, nullptr);

    // MODIFIED: Close all sockets in the pool on exit
    {
        std::lock_guard<std::mutex> lk(g_socket_pool_mtx);
        for (int s : g_socket_pool) {
            close(s);
        }
        g_socket_pool.clear();
    }

    return ret;
}
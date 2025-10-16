// nfuse.cpp
// FUSE client that forwards operations to server over a persistent socket.
// Usage: ./nfuse <mountpoint> <server-host> <server-port>
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
#include <endian.h>
#include <sys/stat.h>
#include <sys/statvfs.h>
#include <fcntl.h>
using namespace std;

enum Op : uint32_t {
    OP_GETATTR=1, OP_READDIR=2, OP_OPEN=3, OP_READ=4, OP_WRITE=5,
    OP_CREATE=6, OP_UNLINK=7, OP_MKDIR=8, OP_RMDIR=9, OP_TRUNCATE=10,
    OP_UTIMENS=11, OP_STATFS=12, OP_RELEASE=13
};

static int sockfd = -1;
static mutex sock_mtx;

static void die(const char *m){ perror(m); exit(1); }

static int connect_to_server(const char *host, const char *port) {
    struct addrinfo hints{}, *res;
    hints.ai_family = AF_UNSPEC; hints.ai_socktype = SOCK_STREAM;
    if (getaddrinfo(host, port, &hints, &res) != 0) die("getaddrinfo");
    int s = socket(res->ai_family, res->ai_socktype, res->ai_protocol);
    if (s < 0) die("socket");
    if (connect(s, res->ai_addr, res->ai_addrlen) < 0) die("connect");
    freeaddrinfo(res);
    return s;
}

// forward declarations for readn/writen so they can be used above their definitions
static int readn(int fd, void *buf, size_t n);
static int writen(int fd, const void *buf, size_t n);

static int send_frame_and_recv(const void *payload, uint32_t payload_len, vector<char> &out_status_and_data) {
    
    return 0;
}

static int readn(int fd, void *buf, size_t n) {
    
    return (int)n;
}
static int writen(int fd, const void *buf, size_t n) {
   
    return (int)n;
}

// Helpers to build requests
static int do_getattr(const char *path, struct stat *stbuf) {
    
    return 0;
}

// note: added 'fuse_readdir_flags flags' to match FUSE 3.5 API
static int do_readdir(const char *path, void *buf, fuse_fill_dir_t filler, off_t offset, fuse_readdir_flags flags) {
    
    return 0;
}

static int do_open_or_create(const char *path, int flags, int mode, bool create, uint64_t &out_serverfd) {
    
    return 0;
}

static int do_read(uint64_t serverfd, char *buf, size_t size, off_t offset, size_t *out_read) {
    
    return 0;
}

static int do_write(uint64_t serverfd, const char *buf, size_t size, off_t offset, size_t *out_written) {
    
    return 0;
}

static int do_release(uint64_t serverfd) {
   
    return 0;
}

static int do_unlink(const char *path) {
    
    return 0;
}

static int do_mkdir(const char *path, mode_t mode) {
    
    return 0;
}

static int do_rmdir(const char *path) {
    
    return 0;
}

static int do_truncate(const char *path, off_t size) {
    
    return 0;
}

static int do_utimens(const char *path, const struct timespec tv[2]) {
    
    return 0;
}

static int do_statfs(const char *path, struct statvfs *stbuf) {
    
    return 0;
}

// FUSE callbacks
static int nf_getattr(const char *path, struct stat *stbuf, struct fuse_file_info *fi) {
    (void)fi;
    return do_getattr(path, stbuf);
}
// added fuse_readdir_flags
static int nf_readdir(const char *path, void *buf, fuse_fill_dir_t filler, off_t offset, struct fuse_file_info *fi, fuse_readdir_flags flags) {
    (void)fi;
    return do_readdir(path, buf, filler, offset, flags);
}
static int nf_open(const char *path, struct fuse_file_info *fi) {
    uint64_t serverfd;
    int flags = fi->flags;
    int r = do_open_or_create(path, flags, 0644, false, serverfd);
    if (r < 0) return r;
    fi->fh = (uint64_t)serverfd;
    return 0;
}
static int nf_create(const char *path, mode_t mode, struct fuse_file_info *fi) {
    uint64_t serverfd;
    int r = do_open_or_create(path, fi->flags, mode, true, serverfd);
    if (r < 0) return r;
    fi->fh = (uint64_t)serverfd;
    return 0;
}
static int nf_read(const char *path, char *buf, size_t size, off_t offset, struct fuse_file_info *fi) {
    (void)path;
    size_t got;
    int r = do_read((uint64_t)fi->fh, buf, size, offset, &got);
    if (r < 0) return r;
    return (int)got;
}
static int nf_write(const char *path, const char *buf, size_t size, off_t offset, struct fuse_file_info *fi) {
    (void)path;
    size_t wrote;
    int r = do_write((uint64_t)fi->fh, buf, size, offset, &wrote);
    if (r < 0) return r;
    return (int)wrote;
}
static int nf_release(const char *path, struct fuse_file_info *fi) {
    (void)path;
    return do_release((uint64_t)fi->fh);
}
static int nf_unlink(const char *path) { return do_unlink(path); }
static int nf_mkdir(const char *path, mode_t mode) { return do_mkdir(path, mode); }
static int nf_rmdir(const char *path) { return do_rmdir(path); }
static int nf_truncate(const char *path, off_t size, struct fuse_file_info *fi) {
    (void)fi; return do_truncate(path, size);
}
static int nf_utimens(const char *path, const struct timespec tv[2], struct fuse_file_info *fi) {
    (void)fi; return do_utimens(path, tv);
}
static int nf_statfs(const char *path, struct statvfs *stbuf) { return do_statfs(path, stbuf); }








// We'll zero-init and then assign fields to avoid designated-initializer ordering issues.
static struct fuse_operations nf_ops;

int main(int argc, char **argv) {
    if (argc < 4) {
        fprintf(stderr,"Usage: %s <mountpoint> <server-host> <server-port> [fuse-args...]\n", argv[0]);
        return 1;
    }
    const char *mountpoint = argv[1];
    const char *host = argv[2];
    const char *port = argv[3];

    sockfd = connect_to_server(host, port);
    if (sockfd < 0) die("connect_to_server");

    // Build argv for fuse_main: program name + mountpoint + any remaining args
    vector<char*> fargs;
    fargs.push_back(argv[0]);
    fargs.push_back((char*)mountpoint);
    for (int i=4;i<argc;i++) fargs.push_back(argv[i]);
    int fargc = (int)fargs.size();
    fargs.push_back(nullptr);

    // zero-init then assign callbacks
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
    // net_oper.chmod = net_chmod;

    return fuse_main(fargc, fargs.data(), &nf_ops, nullptr);
}


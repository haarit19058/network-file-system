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

static int readn(int fd, void *buf, size_t n)
{
    size_t left = n;
    char *p = (char *)buf;
    while (left)
    {
        ssize_t r = ::read(fd, p, left);
        if (r <= 0)
            return r;
        left -= r;
        p += r;
    }
    return n;
}

static int writen(int fd, const void *buf, size_t n)
{
    size_t left = n;
    const char *p = (const char *)buf;
    while (left)
    {
        ssize_t w = ::write(fd, p, left);
        if (w <= 0)
            return w;
        left -= w;
        p += w;
    }
    return n;
}

static string joinpath(const string &root, const string &path)
{
    if (path.empty() || path == "/")
        return root;
    if (path[0] == '/')
        return root + path;
    return root + "/" + path;
}

void send_errno(int client, int eno)
{
    uint32_t status = htonl((uint32_t)eno);
    uint32_t zlen = htonl(0);
    writen(client, &status, sizeof(status));
    writen(client, &zlen, sizeof(zlen));
}

void send_ok_with_data(int client, const void *data, uint32_t dlen)
{
    uint32_t status = htonl(0);
    uint32_t dlen_be = htonl(dlen);
    writen(client, &status, sizeof(status));
    writen(client, &dlen_be, sizeof(dlen_be));
    if (dlen)
        writen(client, data, dlen);
};

bool statfs_handler(const char *p, int client, const string &root)
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
        return 0;
    }
    send_ok_with_data(client, &st, sizeof(st));
    return 1;
}

int read_handler(const char *p, int client, const string &root)
{
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
    vector<char> datab(size);
    ssize_t r = pread((int)sfd, datab.data(), size, (off_t)off);
    if (r == -1)
    {
        send_errno(client, errno);
        return 1;
    }
    send_ok_with_data(client, datab.data(), (uint32_t)r);
    return 0;
}

int open_create_handler(const char *p, int client, int op, const string &root)
{
    uint32_t pathlen;
    memcpy(&pathlen, p, 4);
    p += 4;
    pathlen = ntohl(pathlen);
    string path(p, p + pathlen);
    p += pathlen;
    int flags;
    memcpy(&flags, p, 4);
    p += 4;
    flags = ntohl(flags);
    string full = joinpath(root, path);
    int fd;
    if (op == OP_CREATE)
    {
        int mode;
        memcpy(&mode, p, 4);
        p += 4;
        mode = ntohl(mode);
        fd = open(full.c_str(), flags | O_CREAT, mode);
    }
    else
    {
        fd = open(full.c_str(), flags);
    }
    if (fd == -1)
    {
        send_errno(client,errno);
        return 1;
    }
    // send fd as uint64
    uint64_t fdbe = htobe64((uint64_t)fd);
    send_ok_with_data(client,&fdbe, sizeof(fdbe));
    return 0;
}

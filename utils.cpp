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

static int readn(int fd, void* buf, size_t n) {
    size_t left = n; char *p = (char*)buf;
    while (left) {
        ssize_t r = ::read(fd, p, left);
        if (r <= 0) return r;
        left -= r; p += r;
    }
    return n;
}

static int writen(int fd, const void* buf, size_t n) {
    size_t left = n; const char *p = (const char*)buf;
    while (left) {
        ssize_t w = ::write(fd, p, left);
        if (w <= 0) return w;
        left -= w; p += w;
    }
    return n;
}

static string joinpath(const string &root, const string &path) {
    if (path.empty() || path == "/") return root;
    if (path[0] == '/') return root + path;
    return root + "/" + path;
}





void send_errno(int client,int eno){
    uint32_t status = htonl((uint32_t)eno);
    uint32_t zlen = htonl(0);
    writen(client, &status, sizeof(status));
    writen(client, &zlen, sizeof(zlen));
}

void send_ok_with_data(int client,const void *data, uint32_t dlen) {
    uint32_t status = htonl(0);
    uint32_t dlen_be = htonl(dlen);
    writen(client, &status, sizeof(status));
    writen(client, &dlen_be, sizeof(dlen_be));
    if (dlen) writen(client, data, dlen);
};




bool statfs_handler(const char* p,int client,const string &root){
    uint32_t pathlen; memcpy(&pathlen, p, 4); p += 4; pathlen = ntohl(pathlen);
    string path(p, p + pathlen);
    string full = joinpath(root, path);
    struct statvfs st;
    if (statvfs(full.c_str(), &st) == -1) { send_errno(client,errno); return 0; }
    send_ok_with_data(client,&st, sizeof(st));
    return 1;
}

int write_handler(const char* p, int client, const string &root) {
    uint32_t pathlen;
    memcpy(&pathlen, p, 4); p += 4; pathlen = ntohl(pathlen);
    string path(p, p + pathlen); p += pathlen;
    uint64_t offset;
    memcpy(&offset, p, 8); p += 8; offset = be64toh(offset);
    uint32_t size;
    memcpy(&size, p, 4); p += 4; size = ntohl(size);
    string full = joinpath(root, path);
    int fd = open(full.c_str(), O_WRONLY);
    if (fd == -1) { send_errno(client, errno); return 1; }
    ssize_t w = pwrite(fd, p, size, offset);
    if (w < 0) { send_errno(client, errno); close(fd); return 1; }
    uint32_t written = htonl((uint32_t)w);
    send_ok_with_data(client, &written, sizeof(written));
    return 0;
}

int release_handler(int client, const string &root, const char *p) {
    uint32_t pathlen;
    memcpy(&pathlen, p, 4);
    p += 4;
    pathlen = ntohl(pathlen);
    string path(p, p + pathlen);
    string full = joinpath(root, path);
    send_ok_with_data(client, nullptr, 0);
    return 0;
}

int unlink_handler(const char* p, int client, const string &root) {
    uint32_t pathlen;
    memcpy(&pathlen, p, 4); p += 4; pathlen = ntohl(pathlen);
    string path(p, p + pathlen);
    string full = joinpath(root, path);
    if (unlink(full.c_str()) == -1) {
        send_errno(client, errno);
        return 1;
    }
    send_ok_with_data(client, nullptr, 0);
    return 0;
}


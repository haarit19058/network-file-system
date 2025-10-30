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




// -------------------------------utility functions ---------------------------------------------

static string joinpath(const string &root, const string &path) {
    if (path.empty() || path == "/") return root;
    if (path[0] == '/') return root + path;
    return root + "/" + path;
}




// -------------------- read and write to the socket in buffer format ------------------------------

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




// ----------------------- send data and errors --------------------------------------


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




// ------------------------------- handler functions ----------------------------------------

int utimens_handler(int client,const string &root,const char* p){
    uint32_t pathlen; memcpy(&pathlen, p, 4); p += 4; pathlen = ntohl(pathlen);
    string path(p, p + pathlen); p += pathlen;

    uint64_t at_sec, at_nsec, mt_sec, mt_nsec;
    memcpy(&at_sec, p, 8); p+=8; at_sec = be64toh(at_sec);
    memcpy(&at_nsec, p, 8); p+=8; at_nsec = be64toh(at_nsec);
    memcpy(&mt_sec, p, 8); p+=8; mt_sec = be64toh(mt_sec);
    memcpy(&mt_nsec, p, 8); p+=8; mt_nsec = be64toh(mt_nsec);

    struct timespec times[2];
    times[0].tv_sec = (time_t)at_sec; times[0].tv_nsec = (long)at_nsec;
    times[1].tv_sec = (time_t)mt_sec; times[1].tv_nsec = (long)mt_nsec;

    string full = joinpath(root, path);
    if (utimensat(AT_FDCWD, full.c_str(), times, AT_SYMLINK_NOFOLLOW) == -1) { send_errno(client,errno); return 1; }
    send_ok_with_data(client,nullptr, 0);
    return 0;
}

int statfs_handler(int client,const string &root,const char* p){
    uint32_t pathlen; memcpy(&pathlen, p, 4); p += 4; pathlen = ntohl(pathlen);
    string path(p, p + pathlen);
    string full = joinpath(root, path);
    struct statvfs st;
    if (statvfs(full.c_str(), &st) == -1) { send_errno(client,errno); return 1; }
    send_ok_with_data(client,&st, sizeof(st));
    return 0;
}

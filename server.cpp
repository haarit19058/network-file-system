// server.cpp
// Simple single-connection TCP server that serves a directory using a tiny protocol.
// Usage: ./server <root-path> <port>

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

enum Op : uint32_t {
    OP_GETATTR=1, OP_READDIR=2, OP_OPEN=3, OP_READ=4, OP_WRITE=5,
    OP_CREATE=6, OP_UNLINK=7, OP_MKDIR=8, OP_RMDIR=9, OP_TRUNCATE=10,
    OP_UTIMENS=11, OP_STATFS=12, OP_RELEASE=13
};

int handle_one(int client, const string &root) {
    while (true ) {
        
        try {
            if (op == OP_GETATTR) {
                
            } else if (op == OP_READDIR) {
                
            } else if (op == OP_OPEN || op == OP_CREATE) {
                
            } else if (op == OP_READ) {
                
            } else if (op == OP_WRITE) {
                
            } else if (op == OP_RELEASE) {
                
            } else if (op == OP_UNLINK) {
                
            } else if (op == OP_MKDIR) {
                
            } else if (op == OP_RMDIR) {
                
            } else if (op == OP_TRUNCATE) {
                
            } else if (op == OP_UTIMENS) {
                
            } else if (op == OP_STATFS) {
                
            } else {
                send_errno(EINVAL);
            }
        } catch(...) {
            send_errno(EIO);
        }
    }
    return 0;
}

int main(int argc, char **argv) {
    if (argc < 3) {
        fprintf(stderr, "Usage: %s <root-path> <port>\n", argv[0]);
        return 1;
    }
    string root = argv[1];
    int port = atoi(argv[2]);

    int listenfd = socket(AF_INET, SOCK_STREAM, 0);
    int on = 1; setsockopt(listenfd, SOL_SOCKET, SO_REUSEADDR, &on, sizeof(on));
    struct sockaddr_in sa{};
    sa.sin_family = AF_INET; sa.sin_addr.s_addr = INADDR_ANY; sa.sin_port = htons(port);
    if (bind(listenfd, (struct sockaddr*)&sa, sizeof(sa)) == -1) { perror("bind"); return 1;}
    if (listen(listenfd, 10) == -1) { perror("listen"); return 1;}
    printf("Server serving root=%s on port %d\n", root.c_str(), port);

    while (true) {
        struct sockaddr_in cli; socklen_t clilen = sizeof(cli);
        int client = accept(listenfd, (struct sockaddr*)&cli, &clilen);
        if (client == -1) { perror("accept"); continue; }
        printf("Client connected: %s\n", inet_ntoa(cli.sin_addr));
        handle_one(client, root);
        close(client);
        printf("Client disconnected\n");
    }
    close(listenfd);
    return 0;
}


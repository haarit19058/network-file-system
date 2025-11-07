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
#include "utils.cpp"
#include <queue>
#include <condition_variable>
#include <thread>
#include <atomic>
#include <signal.h>

using namespace std;
static std::queue<int> client_queue;
static std::mutex queue_mtx;
static std::condition_variable queue_cv;
static std::atomic<bool> stop_all{false};

enum Op : uint32_t {
    OP_GETATTR=1, OP_READDIR=2, OP_OPEN=3, OP_READ=4, OP_WRITE=5,
    OP_CREATE=6, OP_UNLINK=7, OP_MKDIR=8, OP_RMDIR=9, OP_TRUNCATE=10,
    OP_UTIMENS=11, OP_STATFS=12, OP_RELEASE=13
};

int handle_one(int client, const string &root)  {
    while (true ) {
        
        uint32_t len_be;

        // Read the first 4 bytes from the client: message length in network byte order (big-endian)
        if (readn(client, &len_be, sizeof(len_be)) != sizeof(len_be)) return 0;

        uint32_t len = ntohl(len_be); // convert length to host byte order
        if (len < 4) return 0;        // minimum length must include opcode

        // Allocate buffer to hold the entire message
        vector<char> buf(len);

        // Read the full message into buffer
        if (readn(client, buf.data(), len) != (int)len) return 0;

        const char *p = buf.data();   // pointer to traverse the payload
        uint32_t op;

        // Extract the operation code (first 4 bytes of the message)
        memcpy(&op, p, 4); 
        p += 4;           // move pointer past the opcode
        op = ntohl(op);   // convert opcode to host byte order


        try {
            switch (op)
            {
            
            case OP_UTIMENS:
                if(utimens_handler(client,root,p))continue;
                break;

            case OP_STATFS:
                if(statfs_handler(client,root,p))continue;
                break;
            
            default:
                send_errno(client,EINVAL);
                break;
            }

        } catch(...) {
            send_errno(client,EIO);
        }
    }
    return 0;
}

void worker_thread_func(const string &root) {
    while (true) {
        int client = -1;

        // Waiting for a client connection from the queue
        {
            std::unique_lock<std::mutex> lock(queue_mtx);
            queue_cv.wait(lock, [] {
                return !client_queue.empty() || stop_all.load();
            });

            if (stop_all && client_queue.empty())
                return; // graceful exit

            client = client_queue.front();
            client_queue.pop();
        }

        // Processing the client connection
        handle_one(client, root);
        close(client);
        fprintf(stderr, "[Worker %lu] client done\n",
                (unsigned long)std::hash<std::thread::id>{}(std::this_thread::get_id()));
    }
}

void signal_handler(int) {
    {
        std::lock_guard<std::mutex> lock(queue_mtx);
        stop_all = true;
    }
    queue_cv.notify_all();
}

int main(int argc, char **argv) {
    if (argc < 3) {
        fprintf(stderr, "Usage: %s <root-path> <port>\n", argv[0]);
        return 1;
    }
    string root = argv[1];
    int port = atoi(argv[2]);

    signal(SIGINT, signal_handler);
    signal(SIGTERM, signal_handler);
    int listenfd = socket(AF_INET, SOCK_STREAM, 0);
    int on = 1; setsockopt(listenfd, SOL_SOCKET, SO_REUSEADDR, &on, sizeof(on));
    struct sockaddr_in sa{};
    sa.sin_family = AF_INET; sa.sin_addr.s_addr = INADDR_ANY; sa.sin_port = htons(port);
    if (bind(listenfd, (struct sockaddr*)&sa, sizeof(sa)) == -1) { perror("bind"); return 1;}
    if (listen(listenfd, 10) == -1) { perror("listen"); return 1;}
    printf("Server serving root=%s on port %d\n", root.c_str(), port);

    // Starting thread pool
    const int NUM_THREADS = 8;
    vector<std::thread> workers;
    for (int i = 0; i < NUM_THREADS; ++i)
        workers.emplace_back(worker_thread_func, root);


    while (true) {
    struct sockaddr_in cli; socklen_t clilen = sizeof(cli);
    int client = accept(listenfd, (struct sockaddr*)&cli, &clilen);
    if (client == -1) {
        if (errno == EINTR) break;
        perror("accept");
        continue;
    }

    char addrbuf[64];
    inet_ntop(AF_INET, &cli.sin_addr, addrbuf, sizeof(addrbuf));
    printf("[Main] Accepted connection from %s\n", addrbuf);

    {
        std::lock_guard<std::mutex> lock(queue_mtx);
        if (client_queue.size() > 100) {
            fprintf(stderr, "Queue full — rejecting new client\n");
            close(client);
            continue;
        }

        client_queue.push(client);
    }
    queue_cv.notify_one();
}
    {
    std::lock_guard<std::mutex> lock(queue_mtx);
    stop_all = true;
    }
    queue_cv.notify_all();

    for (auto &t : workers)
        if (t.joinable()) t.join();

    close(listenfd);
    return 0;
}


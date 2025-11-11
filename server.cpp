// server.cpp
// Multi-threaded TCP server for the network filesystem.
// Usage: ./server <root-path> <port>

#include <iostream>
#include <string>
#include <vector>
#include <thread>
#include <cstring> // for memcpy
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <unistd.h>
#include <endian.h> // ntohl

// Include the handlers and protocol definitions
#include "utils.cpp"
#include "threadpool.cpp"
#define NUM_THREADS 16

using std::string;
using std::vector;
using std::cout;
using std::cerr;
using std::endl;

RWLockManager g_lock_manager;

/**
 * @brief Handles all requests from a single connected client.
 * * This function runs in a dedicated thread per client. It reads
 * requests, dispatches them to the correct handler, and continues
 * until the client disconnects.
 * * Network Protocol (Client -> Server):
 * [ 4-byte total_len_be | 4-byte opcode_be | (total_len - 4) bytes of payload ]
 * * @param client The client's socket file descriptor.
 * @param root The root directory path to serve.
 * @return int 0 on clean disconnect, 1 on error.
 */
int handle_one(int client, const string &root, RWLockManager &g_lock_manager)  {
    while (true) {
        
        uint32_t len_be;

        // 1. Read the 4-byte total length prefix
        if (readn(client, &len_be, sizeof(len_be)) != sizeof(len_be)) {
            // Client disconnected or read error
            return 0; 
        }

        uint32_t len = ntohl(len_be); // Total length of (opcode + payload)
        if (len < 4) {
             cerr << "Error: invalid packet length " << len << endl;
             return 1; // Protocol error
        }

        // 2. Allocate buffer and read the rest of the packet (opcode + payload)
        vector<char> buf(len);
        if (readn(client, buf.data(), len) != (int)len) {
            // Client disconnected or read error
            return 0;
        }

        // 3. Extract opcode and payload pointer
        const char *p = buf.data();
        uint32_t op;
        memcpy(&op, p, 4); p += 4;
        op = ntohl(op);

        // 4. Dispatch to the correct handler
        // Handlers are responsible for sending their own replies.
        // A non-zero return from a handler is an error, but we continue
        // serving the client.
        try {
            switch (op)
            {
            case OP_GETATTR:
                if (getattr_handler(client, root, p, g_lock_manager)) continue;
                break;
            case OP_READDIR:
                if (readdir_handler(client, root, p, g_lock_manager)) continue;
                break;
            case OP_OPEN:
                if (open_create_handler(client, root, p, op, g_lock_manager)) continue;
                break;
            case OP_READ:
                if (read_handler(client, root, p, g_lock_manager)) continue;
                break;
            case OP_WRITE:
            case OP_WRITE_BATCH: // <-- ADDED THIS CASE
                // Server treats WRITE and WRITE_BATCH identically.
                if (write_handler(client, root, p, g_lock_manager)) continue;
                break;
            case OP_CREATE:
                if (open_create_handler(client, root, p, op, g_lock_manager)) continue;
                break;
            case OP_UNLINK:
                if (unlink_handler(client, root, p, g_lock_manager)) continue;
                break;
            case OP_MKDIR:
                if (mkdir_handler(client, root, p, g_lock_manager)) continue;
                break;
            case OP_RMDIR:
                if (rmdir_handler(client, root, p, g_lock_manager)) continue;
                break;
            case OP_TRUNCATE:
                if (truncate_handler(client, root, p, g_lock_manager)) continue;
                break;
            case OP_UTIMENS:
                if (utimens_handler(client, root, p, g_lock_manager)) continue;
                break;
            case OP_STATFS:
                if (statfs_handler(client, root, p, g_lock_manager)) continue;
                break;
            case OP_RELEASE:
                if (release_handler(client, root, p, g_lock_manager)) continue;
                break;
            default:
                cerr << "Error: unknown opcode " << op << endl;
                send_errno(client, EINVAL); // Invalid argument
                break;
            }
        } catch (const std::exception& e) {
            cerr << "Error: exception in handler: " << e.what() << endl;
            send_errno(client, EIO); // I/O error
        } catch (...) {
            cerr << "Error: unknown exception in handler." << endl;
            send_errno(client, EIO);
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
    if (listenfd < 0) {
        perror("socket");
        return 1;
    }

    int on = 1; 
    setsockopt(listenfd, SOL_SOCKET, SO_REUSEADDR, &on, sizeof(on));
    
    struct sockaddr_in sa{};
    sa.sin_family = AF_INET; 
    sa.sin_addr.s_addr = INADDR_ANY; 
    sa.sin_port = htons(port);
    
    if (bind(listenfd, (struct sockaddr*)&sa, sizeof(sa)) == -1) { 
        perror("bind"); 
        close(listenfd);
        return 1;
    }
    if (listen(listenfd, 10) == -1) { 
        perror("listen"); 
        close(listenfd);
        return 1;
    }
    
    printf("Server serving root=%s on port %d\n", root.c_str(), port);

    ThreadPool pool(NUM_THREADS); 
    printf("Initialized Thread Pool with %d worker threads.\n", NUM_THREADS);

    while (true) {
        struct sockaddr_in cli;
        socklen_t clilen = sizeof(cli);

        int client = accept(listenfd, (struct sockaddr*)&cli, &clilen);
        if (client == -1) {
            perror("accept");
            continue;
        }

        cout << "Client connected: " << inet_ntoa(cli.sin_addr) << endl;

        // Create a new thread for each client
        // Pass root by value to avoid lifetime issues
        pool.enqueue([client, root]() {
            handle_one(client, root, g_lock_manager); // handle client requests
            close(client);            // close client socket when done
            cout << "Client disconnected." << endl;
        });
    }    
    
    close(listenfd);
    return 0;
}
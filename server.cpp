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
#include <thread>

#include "utils.cpp"

using namespace std;


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
            case OP_GETATTR:
                if (getattr_handler(client, root, p)) continue;
                break;

            case OP_READDIR:
                if (readdir_handler(client, root, p)) continue;
                break;

            case OP_OPEN:
                if (open_create_handler(client, root, p, op)) continue;
                break;

            case OP_READ:
                if (read_handler(client, root, p)) continue;
                break;

            case OP_WRITE:
                if (write_handler(client, root, p)) continue;
                break;

            case OP_WRITE_BATCH:
                if (write_handler(client, root, p)) continue;
                break;

            case OP_CREATE:
                if (open_create_handler(client, root, p, op)) continue;
                break;

            case OP_UNLINK:
                if (unlink_handler(client, root, p)) continue;
                break;

            case OP_MKDIR:
                if (mkdir_handler(client, root, p)) continue;
                break;

            case OP_RMDIR:
                if (rmdir_handler(client, root, p)) continue;
                break;

            case OP_TRUNCATE:
                if (truncate_handler(client, root, p)) continue;
                break;

            case OP_UTIMENS:
                if (utimens_handler(client, root, p)) continue;
                break;

            case OP_STATFS:
                if (statfs_handler(client, root, p)) continue;
                break;

            case OP_RELEASE:
                if (release_handler(client, root, p)) continue;
                break;
            
            default:
                send_errno(client, EINVAL);
                break;
            }
        } catch (...) {
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
    int on = 1; setsockopt(listenfd, SOL_SOCKET, SO_REUSEADDR, &on, sizeof(on));
    struct sockaddr_in sa{};
    sa.sin_family = AF_INET; sa.sin_addr.s_addr = INADDR_ANY; sa.sin_port = htons(port);
    if (bind(listenfd, (struct sockaddr*)&sa, sizeof(sa)) == -1) { perror("bind"); return 1;}
    if (listen(listenfd, 10) == -1) { perror("listen"); return 1;}
    printf("Server serving root=%s on port %d\n", root.c_str(), port);

    while (true) {
        struct sockaddr_in cli;
        socklen_t clilen = sizeof(cli);

        int client = accept(listenfd, (struct sockaddr*)&cli, &clilen);
        if (client == -1) {
            perror("accept");
            continue;
        }

        cout << "Client connected: " << inet_ntoa(cli.sin_addr) << std::endl;

        // Create a new thread for each client
        thread([client, root]() {
            handle_one(client, root); // handle client requests
            close(client);            // close client socket when done
            cout << "Client disconnected\n";
        }).detach(); // detach so thread cleans up automatically
    }    
    close(listenfd);
    return 0;
}


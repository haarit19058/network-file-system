#include <iostream>
#include <thread>
#include <fcntl.h>
#include <unistd.h>
#include <cstring>

void write_to_file(int fd, char ch) {
    char buff[126];
    memset(buff, ch, sizeof(buff));
    buff[125] = '\n';

    for (int i = 0; i < 1024; i++) {
        write(fd, buff, sizeof(buff));
    }

    close(fd);
}

int main() {
    int fd1 = open("./mntdir/file.txt", O_WRONLY);
    int fd2 = open("./new_mntdir/file.txt", O_WRONLY);

    if (fd1 < 0 || fd2 < 0) {
        std::cerr << "Failed to open files.\n";
        return 1;
    }

    // Thread 1 → writes 'S'
    std::thread t1(write_to_file, fd1, 'S');

    // Thread 2 → writes 'V'
    std::thread t2(write_to_file, fd2, 'V');

    t1.join();
    t2.join();

    return 0;
}
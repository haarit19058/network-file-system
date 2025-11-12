#!/bin/bash
# run_fs.sh — build and run server/client with logs (with Ctrl+C handling)

set -e  # Exit immediately if a command fails

# Function to clean up background processes on exit or Ctrl+C
cleanup() {
    echo ""
    echo "🧹 Cleaning up..."
    if [[ -n "$SERVER_PID" ]] && kill -0 "$SERVER_PID" 2>/dev/null; then
        echo "Killing server (PID $SERVER_PID)..."
        kill "$SERVER_PID" 2>/dev/null || true
    fi
    if [[ -n "$CLIENT_PID" ]] && kill -0 "$CLIENT_PID" 2>/dev/null; then
        echo "Killing client (PID $CLIENT_PID)..."
        kill "$CLIENT_PID" 2>/dev/null || true
    fi
    wait 2>/dev/null || true
    echo "✅ Cleanup complete."
    fusermount3 -u mntdir
    rm server.o client.o
}
trap cleanup INT TERM EXIT   # Trap Ctrl+C (SIGINT), kill, or script exit

# Remove old logs
rm -f *.log
rm -f server.log client.log

# Build both
echo "🧱 Building server and client..."
g++ -o server.o  ./server/server.cpp
# g++ -o client.o -lfuse3 client.cpp    # For arch-linux users
g++ ./client/client.cpp -o client.o $(pkg-config fuse3 --cflags --libs)  # For ubuntu users


if [ ! -d mntdir ]; then
    mkdir mntdir
    chown $USER:$USER mntdir
    chmod 777 mntdir
fi


# Run both and log outputs
echo "🚀 Starting server and client..."
./server.o "$1" 3030 > server.log 2>&1  &
SERVER_PID=$!
echo "Server running with PID $SERVER_PID"

sleep 1  # give server time to start

./client.o mntdir 127.0.0.1 3030 -f -o attr_timeout=60 -o entry_timeout=60 > client.log 2>&1  &
# -o attr_timeout=60 -o entry_timeout=60

CLIENT_PID=$!
echo "Client running with PID $CLIENT_PID"

# Wait for both to finish or Ctrl+C
wait "$SERVER_PID" "$CLIENT_PID"


# ---------------------------------------
# Write a 100MB file using 4KB blocks.
# This will do many sequential writes before one final close.
# dd if=/dev/zero of=/mnt/your-mount/testfile.dat bs=4k count=25600
## New running mechanism
Just run ./test.sh "folder_name" (Here, folder_name refers to root directory of server that you want to share) to run it on your local machine, see client.log and server.log to check function call logging


## Manual Compialtion of client and server separately

for server
g++ -o server.o ./server/server.cpp

for client
g++ -o client.o ./client/client.cpp



./server.o dirpath port
./client.o mntdir serverip serverport

fusemount3 -u mntdir
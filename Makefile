CXX = g++
CXXFLAGS = -std=c++17 
LDFLAGS =
# may need -pthread for fuse
LIBS = -lfuse3

all: server client

server: server.cpp
	$(CXX) $(CXXFLAGS) server.cpp -o server.out

client: client.cpp
	$(CXX) $(CXXFLAGS) client.cpp -o client.out $(LIBS)

clean:
	rm -f server.out client.out


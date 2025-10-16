CXX = g++
CXXFLAGS = -std=c++17 -O2
LDFLAGS =
# may need -pthread for fuse
LIBS = -lfuse3

all: server nfuse

server: server.cpp
	$(CXX) $(CXXFLAGS) server.cpp -o server

nfuse: nfuse.cpp
	$(CXX) $(CXXFLAGS) nfuse.cpp -o nfuse $(LIBS)

clean:
	rm -f server nfuse


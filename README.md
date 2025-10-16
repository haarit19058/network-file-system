# how to run ??

See for yourself the working of this demo nfs that we will reiimplement with more features
Creds : chatgpt (for writing code as i told) and gfg (for teachign the basics of sockets and fuse3)

1. make
2. mkdir mntdir                -- create a directory to mount the nfs filesystem
3. ./server . 3000              -- start the server on port 3000 and tell it to use . as root dir
4. ./nfuse ./mntdir 127.0.0.1 3000   -- tell the client to mount the directory at mntdir and connect to server at port 3000
5. fusermount -u mntdir         -- after completion unmount the nfs filesystem


fuse3 is the cpp library that you would need to install and it depents on the distro that you use

for aarch 
- sudo pacman -S fuse3 

for other distros you can use chatgpt



Here is what we will do next ....
- Will make a repository and add you guys as collaborators ..
- I will create a raw template of nfuse.cpp and server.cpp
- Then we will divide the function implemenatation 
- We will populate the files by this saturday night 
- Then we will work on multithreading and client side caching


Why this ??
We need to look legit in github repo and we still need to understand what is happening in the code.
Therefore we will repopulate the files.

Add comments in your parts so that anyone can understand the code 





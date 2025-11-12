## Server side
### Start a NFS server
sudo systemctl enable --now nfs-server
sudo systemctl status nfs-server
sudo systemctl enable --now rpcbind

### Make a folder that you want to treat as root
sudo mkdir -p /srv/nfs_share
sudo chown nobody:nobody /srv/nfs_share
sudo chmod 777 /srv/nfs_share

### Use your IP
sudo nano /etc/exports
/srv/nfs_share 10.7.44.227/18(sync,wdelay,hide,no_subtree_check,sec=sys,rw,secure,no_root_squash,no_all_squash)

### Export
sudo exportfs -ra
sudo exportfs -v

## Client side
1. Install NFS client utilities

On Arch Linux:
sudo pacman -S nfs-utils

On Ubuntu/Debian:
sudo apt install nfs-common

2. Create a mount directory
sudo mkdir -p /mnt/nfs_client

3. Mount the share
sudo mount -t nfs -o vers=3 <server-ip>:/srv/nfs_share /mnt/nfs_client

You can now access files from the server:
ls /mnt/nfs_client

## disconnect
sudo unmount ./mntdir


fio for 50M file 
-------------------------------
NFS v3
Run status group 0 (all jobs):
READ: bw=6603KiB/s (6761kB/s), 6603KiB/s-6603KiB/s (6761kB/s-6761kB/s), io=650MiB(68.2MB), run=10081-10081msec

Run status group 0 (all jobs):
WRITE: bw=1535KiB/s (1572kB/s), 1535KiB/s-1535KiB/s (1572kB/s-1572kB/s), io=150MiB (15.7MB), run=10006-10006msec

------------------------------

--------------------------------
NFS v4

Run status group 0 (all jobs):
READ: bw=8878KiB/s (9091kB/s), 8878KiB/s-8878KiB/s (9091kB/s-9091kB/s), io=870MiB (91.2MB), run=10035-10035msec

Run status group 0 (all jobs):
WRITE: bw=1206KiB/s (1234kB/s), 1206KiB/s-1206KiB/s (1234kB/s-1234kB/s), io=120MiB (12.6MB), run=10193-10193msec

---------------------------------



-------------------------------
LNFS v1


```cpp
#pragma once
#define CHUNK_SIZE 128*1024*1024
#define POOL_SIZE 8
#define CACHE_BLOCK_SIZE 4096
#define CACHE_CAPACITY_BYTES 20*1024*1024
#define READAHEAD_SIZE 10*1024*1024
#define BATCH_WRITE_THRESHOLD 1*1024*1024

```

Run status group 0 (all jobs):
READ: bw=2580KiB/s (2642kB/s), 2580KiB/s-2580KiB/s (2642kB/s-2642kB/s), io=310MiB (32.5MB), run=12304-12304msec

Run status group 0 (all jobs):
WRITE: bw=1270KiB/s (1300kB/s), 1270KiB/s-1270KiB/s (1300kB/s-1300kB/s), io=130MiB (13.6MB), run=10483-10483msec

-------------------------------

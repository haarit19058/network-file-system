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
# TAR Multithreaded & Parallelized Library

This library allow to append file in archive from clustered application.

Actually we use Redis cache to synchronize write lock between each instance of tarp.
Successfully tested in kubernetes cluster, with multiple container and multi node.

This script is designed to run on POSIX output storage system not S3.

Tested With NFS 4.2.

TAR block size is set to 512 bytes.

## Quick Start on Debian
### Install Redis Instance

````bash
apt -y install redis-server
systemctl enable --now redis-server.service
sed -i 's/bind 127.0.0.1/bind 0.0.0.0/' /etc/redis/redis.conf
systemctl start redis-server.service
````

### Code instantiation
````bash
pip3 install tarp
````

````python
import tarp
import redis_lock

rlock = redis_lock.StrictRedis(host="192.168.1.42", port=6379, db=1)
tar_file_name = "output.tar"
output_write_path = "/tmp/"
name = "my_file.txt"

with tarp(tar_file_name, output_write_path, rlock) as t:
    t.add_file_to_archive_stream(name)
````

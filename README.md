# TAR Multithreaded & Parallelized Library

This library allow to append file in archive from clustered application.

Actually we use Redis cache to synchronize write lock between each instance of tarp.
Successfully tested in kubernetes cluster, with multiple container and multi node.

This script is designed to run on POSIX output storage system not S3.

Tested With NFS 4.2.


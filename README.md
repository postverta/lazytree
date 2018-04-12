## lazytree

`lazytree` is a golang library that lays the groundwork for a lazy-loading
distributed file system with heterogeneous data sources.

### The Grand Vision

The original design goal is to achieve something similar to NFS but the actual
data can be served by any data source as long as the data schema can be mapped
to trees and files. For instance, the `source` directory of the file system can
be "mounted" from Github, and files are checked out on-the-fly from Github when
accessed, in a way similar to [GVFS](https://github.com/Microsoft/GVFS). On the
other hand, the library files within the `node_modules` directory can be
downloaded on-the-fly from `npmjs.com` or `unpkg.com`.

In short: no more `git clone` and `npm install`. Simply mount and go.

The reality is that such goal was a little too ambitious. I did write the
drivers for `github` and `unpkg`, but the performance was abysmal. The root
cause is simply that those data serving services are not designed for
high-frequency access of individual files. They are optimized for batch
downloading mostly, due to the WAN latency. It is still interesting to see how
the concept plays out in a local cluster with low latency and abundant
throughput.

### Usage in Postverta

As a result, the actual usage of `lazytree` in Postverta is as an in-memory
file system for the coding workspace. To persist the in-memory file system, we
also add the functionalities to efficiently serialize and deserialize the file
system into and from a compressed blob (using the awesome snappy compression
algorithm from Google). The blobs are then stowed away in Azure's blob storage
service.

We chose the in-memory file system + blob storage solution over other
alternatives that leverage off-the-shelf distributed file system (e.g. Ceph or
Azure File), because of its simplicity and robustness. Distributed file systems
are complex distributed systems that can have unpredictable performance. If not
configured correctly, they can also cause data corruption and loss. On the
other hand, in-memory file system provides highly consistent and predictable
performance and the blob storage service is usually the most robust cloud
service out there.

To see how the library is used, please refer to the `worktree` directory of the
`pv_exec` repository.

### Benchmarks

A `react` template workspace with 16K files and 100MB before compression

| Filesystem | Image size | Load/mount time | Save time (to blob service) | Save time (to local file) | `yarn install` time | Image copy time |
| --- | --- | --- | --- | --- | --- | --- |
| `lazytree` | 27.5MB | 0.4s | 0.89s | 0.27s | 9.71s | <1s |
| `ceph rbd` with `btrfs` | 208MB | 0.12s | N/A | N/A | 8.65s | 3.5s |

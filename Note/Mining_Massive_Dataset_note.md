# Mining Massive Dataset - Notes

[TOC]

## Chapter 2: MapReduce and the New Software Stack

### 2.1 Distributed File Systems

#### 2.1.1 Physical Organization of Compute Nodes

__Clustering Computing:__

- the new parallel-computing architecture
- Compute nodes are stored on racks, perhaps 8–64 on a rack.
  - nodes on a single rack are connected by a network, typically gigabit Ethernet
- racks are connected by another level of network or a switch
- the bandwidth of inter-rack communication is greater than the intrarack Ethernet
- principle failure modes:
  - the loss of a single node (e.g., the disk at that node crashes)
  - the loss of an entire rack (e.g., the network connecting its nodes to each other and to the outside world fails)



__How to solve the problem of component failure in the clustering computing?__

1. Files must be stored redundantly.
2. Computations must be divided into tasks, such that if any one task fails to execute to completion, it can be restarted without affecting other tasks.



#### 2.1.2 Large-Scale File-System Organization

To solve the problems mentioned above, we have __distributed file system (DFS)__.

> __DFS Implementations__
>
> 1. $Google$ $File$ $System$ (GFS), the original of the class.
> 2. $Hadoop$ $Distributed$ $File$ $System$ (HDFS), an open-source DFS used with Hadoop, an implementation of MapReduce.
> 3. $CloudStore$, an open-source DFS originally developed by Kosmix.



In DFS:

- Files can be enormous, possibly a terabyte in size. If you have only small files, there is no point using a DFS for them.
- Files are rarely updated.
- Files are divided into __chunks__, which are typically __64 MB__ in size:
  - chunks are replicated
  - the nodes holding copies of one chunk should be located on different racks
  - both the chunk size and the degree of replication can be decided by the user
- Use __master node (name node)__ to find the chunks of a file
  - the master node is itself replicated, and a directory for the file system as a whole knows where to find its copies
  - the directory itself can be replicated, and all participants using the DFS know where the directory copies are.



### 2.2 MapReduce


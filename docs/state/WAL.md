# WAL


## Overview

It is essential in [Raft Consensus](https://raft.github.io/raft.pdf) that the replicated log is stored durably on the nodes in the cluster, so that in case of failure, the node can start up and replay its log up to the point in time it failed. Essentially, the replicated log acts as a [Write Ahead Log](https://en.wikipedia.org/wiki/Write-ahead_logging), ensuring that the log is both atomic and durable.


## What is a Write Ahead Log?

A Write Ahead Log is a an append-only data structure that resides on disk instead of in memory. Events are first logged to the write ahead log, and once a majority have agreed upon the state in the log, then the events can be applied to the state machine shared between all of the nodes in the cluster.


# Implementation

For this take on raft consensus, the replicated log is implemented using [BoltDB](https://github.com/etcd-io/bbolt). `BoltDB` is an embedded key-value store, which is written to disk directly. `BoltDB` was designed for heavy read operations and is the storage layer for etcd, another implementation of raft consensus used in [Kubernetes](https://kubernetes.io), a container orchestration and management tool. 

The underlying data structure for `BoltDB` uses B+ tree, making it suitable for heavy read applications. All key-value pairs are called `inodes`. The difference between a B+ tree and a B-tree is that B+ trees chilld nodes do not hold pointers to siblings, which reduces speed of range scans but saves additional space on disk.

`BoltDB` is extremely lightweight and only has a few major types:

  1. DB
  2. Bucket
  3. Tx
  4. Cursor

`DB` --> the actual database, which is written to a single file
`Bucket` --> think of a bucket as a collection of key-value pairs, or like a directory. You can also have nested buckets as well
`Tx` --> transactions, which can be either read-write or read-only. Read-only transactions can be run concurrently while read-write transactions are locking on the database, but ensure atomicity
`Cursor` --> the cursor is used to iterate over keys in the tree

The replicated log is contained under a `replog` bucket, where all key-value pairs are the index as the key, and the log entry as the value, where the key will increase monotonically with the log entries as they are applied. 


# Indexes

A separate sub bucket in the replicated log bucket is kept that tracks all of the first entry for each term. This is useful for nodes that have failed and are brought back into the cluster or new nodes are added since it helps reduce the number of failed AppendEntryRPCs between the node and leader as the node is brought back online.


## Sources

[WAL](../pkg/wal/WAL.go)
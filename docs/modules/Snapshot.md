# Snapshot


## Overview

In [Raft Consensus](https://raft.github.io/raft.pdf), as the log grows over time, a snapshotting and log compaction strategy is applied to reduce the number of logs in the replicated log and the overall storage usage on the system. When a threshold determined by the implementation is met, the leader takes a snapshot of the current state within the state machine as well as state of logs up to the last applied log.

Let's imagine that our state machine is the following:
```
x <- number
y <- number
z <- number
```

and we have log entries that look like the following:
```
L1:
  index 0
  term 1
  x <- +1

L2:
  index 1
  term 1
  y <- +3

L3:
  index 2
  term 2
  x <- -1

L4:
  index 3
  term 3
  z <- +2
```

if all of these logs have been applied and a snapshot is triggered, the snapshot would contain the following state:
```
LastAppliedIndex 3
LastAppliedTerm 3
State 
  x 0
  y 3
  z 2
```

essentially, a backup is being created, which systems can use to restore state much more quickly in cases of failure than if just the replicated log was used to restore the state


## Implementation

Since the state machine uses `BoltDb` as the underlying database technology, snapshots become as simple as taking a backup of the current database. This writes the entire content of the original database file to a new file, essentially just creating a copy. This copy is then compressed and all logs in the replicated log up the last applied are removed.

While some implementations use a static, fixed interval for snapshotting and log compaction based on a value such as the log size or a timeframe, this implementation takes a more dynamic approach. The algorithm is as follows:
```
  1. Determine the available space on the current mount where the database and log are written to
  2. if the database and log exceed the maximum size in bytes calculated by available space in bytes / fraction of available space to use, snapshot
  3. Recalculate available space on the drive for the next calculation for snapshotting
```

This looks to limit log size the more the drive starts to fill up on the system, so over time if the drive fills up, the overall size the log can be will shrink to decrease the need to perform system maintenance to free space for the raft nodes. This should also increase the longevity of the cluster.


## Replay

When a node crashes or is first brought into the cluster, it attempts to replay both the logs available to it on the replicated log and the latest snapshot available to it. If a leader recognizes that its log is has a higher minimum index than what the node has, it sends the latest snapshot available to it to the new follower. The follower can then replay the later snapshot, reducing the need to large amounts of AppendEntryRPCs when the node is syncing its current state up to the state of the cluster.


## Sources

[Snapshot](../pkg/snapshot/Snapshot.go)
# Replication


## Overview

One of the major components of [Raft Consensus](https://raft.github.io/raft.pdf) is the replicated log shared between systems within the cluster that allows nodes to safely replicate state on data changes. The replicated log helps maintain a log of all actions that have been performed on the state machine and can be viewed as an audit, due to its append only design.


### Replicated Log

In this doc, the focus will be on the `AppendEntryRPC`, which is the remote procedure call used to request votes from other systems during an election phase.

The general layout of the request is:
```proto
message AppendEntry {
  int64 Term = 1;
  string LeaderId = 2;
  int64 PrevLogIndex = 3;
  int64 PrevLogTerm = 4;
  repeated LogEntry Entries = 5;
  int64 LeaderCommitIndex = 6;
}
```

1. Term --> the current term on the leader
2. LeaderId --> the unique identifier for the leader
3. PrevLogIndex --> the index of the log being appended for NextIndex
4. PrevLogTerm --> the term of the log being appended for NextIndex
5. Entries --> the entries since previous log index being applied
6. LeaderCommitIndex --> the index of the latest log applied to the state machine by the leader

The `LogEntry` message consists of:
```proto
message CommandPayload {
  bytes Collection = 2;
  bytes Value = 3;
}

message Command {
  bytes RequestId = 1;
  bytes Action = 2;
  CommandPayload Payload = 3;
}

message LogEntry {
  int64 Index = 1;
  int64 Term = 2;
  Command Command = 3;
}
```

where the index is the index of the log, the term is the current term of the applied log, and the command is the command, encoded to bytes, that mutates the state machine. The command contains the actions against the state machine, as well as the payload associated with that action.


### Heartbeat

When a leader is elected, it begins sending heartbeats to each node at a set inverval until a new command is entered from the client and a log is created. Any `AppendEntryRPC` can act as a heartbeat, but if no logs are available, the leader will send a heartbeat with no entries so follower nodes do not begin a new election.


## Algorithm

The basic algorithm is as follows:
```
Leader node in the cluster -->

  if no new logs are added to replicated log before heartbeat interval:
    broadcast heartbeat to all available nodes in the cluster:
      a. if success reply is received, maintain the node status as alive
      b. otherwise, label node as dead

    continue to next heartbeat or available log

  if new log is added to replicated log:
    for each system in the cluster:
      a. determine the next index that is to be applied to the system
      b. send an AppendEntryRPC to the node with the entries to be applied to it's replicated log
      c. if reply is failure and the term is higher from the reply, set the leader to follower
      d. set the follower next index to the latest index in the reply from the follower

    continue to next heartbeat or available log

Follower node in the cluster -->

  if request term is less than the current term of the system:
    return false with the term of the current system

  if the previous log being sent has index less than current log index of replicated log or the log at the index is not the term of the request:
    return false wit the current term and last log index

  otherwise:
    for each entry being applied in the request:
      if the index of the entry being applied is a different term than the current log:
        remove logs from the index forward
      
      append the new entry to the replicated log

    if the commit index of the leader is higher than the commit index of the current system:
      set the commit index of the current system to the minimum of the request commit index and the current commit index of the system

    return true with the current term and latest log index after appending the new logs
```

## Optimizations

In a system where replication is occuring, overhead is incurred due to network latency and processing of replicated logs by follower nodes, which increases overall latency for client responses on write operations. To mitigate this, the following has been implemented:

### Replication as a Pipeline

If the raft implementation treats replication as a pipeline, it needs to minimize processing on the leader. Replication should occur in as few steps as possible and should only mutate data is truly needed. To achieve this, logs are minimimally encoded/decoded on the leader and followers and any additional post processing is avoided, with incoming entries expected to already be in a semi-formatted structure that raft expects.

### Reduce Memory Allocation

Performance can take a hit with unnecessary memory allocations, as this adds overhead due to go garbage collector having to free more variables. To avoid this, incoming data should flow through the pipeline and should not be reassigned if needed. The same goes for errors and other variables.

### Separate Handling of Writes and Reads

If the raft implementation treats only writes as log entries, since these are the only operations mutating the state machine, this will significantly reduce the overhead introduced from file io operations performed by the write ahead log. By minimizing writes, this will reduce overall response time for writes and will significantly reduce the response time for reads since reads will not invoke any additional file operations.

**With the above optimizations, latency was reduced by a factor of 5 from the v1 implementation of rdb and throughput was increased by 11%**

While throughput increased modestly with these changes, the latency had a massive improvement, which will actually have a much more important impact on user experience since individiual requests will be returned to client much quicker, improving overall user experience.


## Sources

[Replicated Log](../pkg/replog/RepLog.go)
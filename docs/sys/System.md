# System


## Overview

```mermaid
graph TD;
  metadata[("In-Memory Metadata (State, Status, Registry, Log Metadata)")]
  campaign[Campaign Module]
  replication[Replication Module]
  snapshot[Snapshot Module]
  request[Request Module (HTTP)]
  db_log[(boltDB Log)]
  db_state[(State Machine DB)]
  grpc[GRPC Servers]
  http[HTTP Server]

  metadata -.->|Read/Write| campaign
  metadata -.->|Read/Write| replication
  metadata -.->|Read/Write| snapshot
  metadata -.->|Read| request

  campaign -. "Internal Timer" .-> campaign
  campaign -->|Election Trigger| grpc
  replication -->|Log Management, Heartbeating| grpc
  replication -->|Reset Timer| campaign
  replication -->|Writes| db_log
  db_log -->|Reads| replication

  db_state -->|Swap Implementation| snapshot
  snapshot -->|Compaction & Snapshot| db_log

  grpc -. "Manage Connection Pool" .-> grpc
  request -->|Client Requests| http

  classDef default fill:#f9f,stroke:#333,stroke-width:2px;
  classDef servers fill:#bbf,stroke:#f66,stroke-width:2px,stroke-dasharray: 5, 5;
  class grpc,http servers;
```
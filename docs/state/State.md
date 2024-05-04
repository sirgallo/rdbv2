# State


## Overview

In [Raft Consensus](https://raft.github.io/raft.pdf), each system stores a copy of the state. The state (state maachine) is somewhat ambiguous, so this implementation settled on a simple replicated key-value store as its state machine.


## Implementation 

The key-value store is implemented using boltdb as the underlying database. Using boltdb has a few advantages to an in memory state machine:
  
  1. BoltDb is optimized for read operations, due to the B-Tree data structure it utilizes for its internal data structure
  2. Since BoltDb is on disk, data is stored durably on the system
  3. The entire database is stored on one file, which simplifies deployment
  4. BoltDb is embedded and written completely in Go, so it is easy to build out
  5. BoltDb supports backups natively, which can be used to snapshot the state of the system, reducing snapshot complexity

The database takes the core of `BoltDb`, as a key-value store, but adds additional functionality on top of it:

  The database is separated into collections, where collections are groups of objects with the same structure. The values do not need to be strictly key value pairs. 

  An indexing scheme is applied to collections, where values/fields are indexed to keys generated for the objects within the collection. Since `BoltDb` buckets are B-trees, the key-value pairs are already sorted by default in ascending order. Building off of this, when an object is inserted into a bucket, indexed values from the object are mapped to index buckets, where the value is stored as the key and the key of the object in the main collection is stored as the value. This allows for quick lookups of objects and removes the need to know the key beforehand if the object being stored is known. Also, range queries can be applied to the indexes for values, and the result will be a list of the associated objects sorted in ascending order.

  In the root of the database, both a bucket for collection names and index names is also kept, so that a user can query existing collections within the database


## Sources

[StateMachine](../pkg/statemachine/StateMachine.go)
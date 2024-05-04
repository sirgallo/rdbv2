package wal

import (
	"sync"
	bolt "go.etcd.io/bbolt"
)


type WAL struct {
	Mutex sync.Mutex
	DBFile string
	DB *bolt.DB
}

type SnapshotEntry struct {
	LastIncludedIndex int64
	LastIncludedTerm int64
	SnapshotFilePath string
}

type StatOP = string


const NAME = "WAL"

const SubDirectory = "rdb/replication"
const FileName = "replication.db"

const ReplicatedLog = "replication"
const ReplicatedLogWAL = ReplicatedLog + "_wal"
const ReplicatedLogStats = ReplicatedLog + "_stats"
const ReplicatedLogIndex = ReplicatedLog + "_index"
const ReplogTotalElementsKey = "total"
const ReplogSizeKey = "size"

const Snapshot = "snapshot"
const SnapshotKey = "current_snapshot"

const SystemStats = "system_stats"

const (
	ADD StatOP = "ADD"
	SUB StatOP = "SUB"
)

const (
	MAX_STATS = 1000
	CHUNK_SIZE = 500
)
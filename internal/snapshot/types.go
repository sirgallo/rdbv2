package snapshot

import (
	"sync"
	"time"
	"github.com/sirgallo/logger"
	
	"github.com/sirgallo/rdbv2/internal/pool"
	"github.com/sirgallo/rdbv2/internal/snapshot/snapshot_proto"
	"github.com/sirgallo/rdbv2/internal/system"
)


type SnapshotServiceOpts struct {
	Port int
	Pool *pool.Pool
	CurrentSystem *system.System
	Systems *sync.Map
}

type SnapshotService struct {
	snapshot_proto.UnimplementedSnapshotServiceServer
	Port string
	Pool *pool.Pool
	CurrentSystem *system.System
	Systems *sync.Map
	AttemptSnapshotTimer *time.Timer
	Log logger.CustomLog

	SnapshotStartSignal chan bool
	UpdateSnapshotForSystemSignal chan string
	ProcessIncomingSnapshotSignal chan *snapshot_proto.SnapshotChunk
}

type SnapshotInfo string
type SnapshotError string


const NAME = "Snapshot"
const RPCTimeout = 200 * time.Millisecond
const AttemptSnapshotInterval = 1 * time.Minute
const SnapshotTriggerAppliedIndex = 10000

const ChunkSize = 1000000	// we will stream 1MB chunks
const FractionOfAvailableSizeToTake = 1000 // let's take consistent snapshots

const (
	ATTEMPTING_COMPACTION SnapshotInfo = "attempting to compact from start to index"
	MINIMUM_SUCCESSFUL_RESPONSES SnapshotInfo = "minimum successful responses received on snapshot broadcast"
	RPC_SERVER_LISTENING SnapshotInfo = "snapshot grpc server listening"
	UPDATE_SNAPSHOT SnapshotInfo = "updating snapshot"
	SNAPSHOT_SUCCESS SnapshotInfo = "snapshot processed log compacted, returning successful response"
	SNAPSHOT_WRITTEN SnapshotInfo = "snapshot written, updating index and compacting logs"
	STREAM_SUCCESS SnapshotInfo = "result from stream received"
	TOTAL_REMOVED SnapshotInfo = "total bytes and keys removed"
)

const (
	COMPACTION_ERROR SnapshotError = "error compacting logs"
	CONNECTION_POOL_ERROR SnapshotError = "failed connection"
	MINIMUM_SUCCESSFUL_RESPONSES_ERROR SnapshotError = "minimum successful responses not received on snapshot broadcast"
	READ_ERROR SnapshotError = "read error"
	RPC_ERROR SnapshotError = "error on snapshot rpc"
	UPDATE_SNAPSHOT_ERROR SnapshotError = "error updating snapshot"
	UPDATE_SYSTEM_ERROR SnapshotError = "error updating system metadata"
	STREAM_SEND_ERROR SnapshotError = "error sending and closing stream"
	STREAM_REC_ERROR SnapshotError = "error receiving on stream"
	SYSTEM_ERROR SnapshotError = "system error"
	WRITE_ERROR SnapshotError = "write error"
)
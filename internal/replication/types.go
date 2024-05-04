package replication

import (
	"sync"
	"time"
	"github.com/sirgallo/logger"
	
	"github.com/sirgallo/rdbv2/internal/replication/replication_proto"
	"github.com/sirgallo/rdbv2/internal/pool"
	"github.com/sirgallo/rdbv2/internal/state"
	"github.com/sirgallo/rdbv2/internal/system"
)


type ReplicationOpts struct {
	Port int
	Pool *pool.Pool
	CurrentSystem  *system.System
	SystemsList []*system.System
	Systems *sync.Map
}

type ReplicationService struct {
	replication_proto.UnimplementedReplicationServiceServer
	
	Port string
	Pool *pool.Pool
	CurrentSystem *system.System
	Systems *sync.Map
	HeartbeatTimer *time.Timer
	ReplicationTimer *time.Timer
	Log logger.CustomLog

	AppendLogSignal chan *state.StateOperation
	ReadChannel chan *state.StateOperation
	WriteChannel chan *state.StateOperation
	LeaderAcknowledgedSignal chan bool
	ResetTimeoutSignal chan bool
	ForceHeartbeatSignal chan bool
	SyncLogChannel chan string
	SendSnapshotToSystemSignal chan string
	StateResponseChannel chan *state.StateResponse
	ApplyLogsFollowerChannel chan int64
	AppendLogsFollowerRespChannel chan bool
	AppendLogsFollowerChannel chan *replication_proto.AppendEntry
}

type ReplicatedLogRequest struct {
	Host string
	AppendEntry *replication_proto.AppendEntry
}

type ReplicationResponseChannels struct {
	BroadcastClose chan struct{}
	SuccessChan chan int
	HigherTermDiscovered chan int64
}

type ReplicationInfo = string
type ReplicationError = string


const NAME = "Replication"

const (
	HeartbeatInterval = 250 * time.Millisecond
	ReplicationInterval = 25 * time.Millisecond
	RPCTimeout = 500 * time.Millisecond
	AppendLogBuffSize = 10000
	ResponseBuffSize = 10000
)

const (
	ACKNOWLEDGED_LEADER ReplicationInfo = "acknowledged leader and returning successful response"
	APPLY_LOGS ReplicationInfo = "applying logs to state machine and appending to WAL"
	HEARTBEATING ReplicationInfo = "sending heartbeats..."
	LEADER_ELECTED ReplicationInfo = "sending heartbeats after election..."
	MINIMUM_RESPONSES_RECEIVED ReplicationInfo = "at least min responses received"
	PREPARING_SYNC ReplicationInfo = "preparing to sync logs"
	RPC_SERVER_LISTENING ReplicationInfo = "replication grpc server listening"
)

const (
	APPEND_WAL_ERROR ReplicationError = "append error"
	BROADCAST_ERROR ReplicationError = "error broadcasting RPC"
	CONNECTION_POOL_ERROR ReplicationError = "failed connection"
	DECODE_ERROR ReplicationError = "error decoding"
	ENCODE_ERROR ReplicationError = "error encoding"
	EXP_BACKOFF_ERROR ReplicationError ="exponential backoff attempt error"
	HIGHER_TERM_ERROR ReplicationError = "higher term discovered"
	HOST_UNREACHABLE ReplicationError = "unreachable, setting status to dead"
	MINIMUM_RESPONSES_RECEIVED_ERROR ReplicationError = "votes received did not meet requirement for minimum"
	PREPARE_APPEND_ENTRY_RPC_ERROR ReplicationError = "error encoding log struct to string"
	READ_ERROR ReplicationError = "read error"
	REPLICATED_LOG_ERROR ReplicationError = "replicated log error"
	STATE_MACHINE_ERROR ReplicationError = "error writing to state machine"
	SYNC_ERROR ReplicationError = "error syncing logs for host"
	TERM_LOWER_THAN_LOCAL_ERROR ReplicationError = "request term lower than current term"
	TERM_MISMATCH_ERROR ReplicationError = "log at request has mismatched term or does not exist"
)
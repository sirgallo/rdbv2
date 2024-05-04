package system

import (
	"sync"

	"github.com/sirgallo/rdbv2/internal/state"
	"github.com/sirgallo/rdbv2/internal/wal"
)


type SystemState string
type SystemStatus int

type System struct {
	Host string
	SysStatus SystemStatus
	SysState SystemState
	CurrentTerm int64
	CommitIndex int64
	LastApplied int64
	VotedFor string
	CurrentLeader string
	NextIndex int64

	WAL *wal.WAL
	State *state.State
	SystemMutex sync.Mutex
}

type StateTransitionOpts struct {
	CurrentTerm *int64
	VotedFor *string
} 

type SystemInfo string


const NAME = "System"
const DefaultLastLogIndex = -1 // -1 symbolizes empty log
const DefaultLastLogTerm = 0

const (
	Leader SystemState = "leader"
	Candidate SystemState = "candidate"
	Follower SystemState = "follower"
)

const (
	Dead SystemStatus = 0
	Ready SystemStatus = 1
	Busy SystemStatus = 2
)

const (
	TRANSITIONED_TO_CANDIDATE SystemInfo = "transitioned to candidate, starting election..."
	TRANSITIONED_TO_FOLLOWER SystemInfo = "transitioned to follower"
	TRANSITIONED_TO_LEADER SystemInfo = "transitioned to leader"
)
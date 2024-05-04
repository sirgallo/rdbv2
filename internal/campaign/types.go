package campaign

import (
	"sync"
	"time"
	"github.com/sirgallo/logger"
	
	"github.com/sirgallo/rdbv2/internal/campaign/campaign_proto"
	"github.com/sirgallo/rdbv2/internal/pool"
	"github.com/sirgallo/rdbv2/internal/system"
)


type TimeoutRange struct {
	Min int
	Max int
}

type CampaignOpts struct {
	Port int
	Pool *pool.Pool
	TimeoutRange TimeoutRange
	CurrentSystem *system.System
	Systems *sync.Map
}

type CampaignService struct {
	campaign_proto.UnimplementedCampaignServiceServer

	Port string
	Pool *pool.Pool
	CurrentSystem *system.System
	Systems *sync.Map
	Timeout time.Duration
	ElectionTimer *time.Timer
	Log logger.CustomLog

	ResetTimeoutSignal chan bool
	HeartbeatOnElection chan bool
}

type CampaignResponseChannels struct {
	BroadcastClose chan struct{}
	VotesChan chan int
	HigherTermDiscovered chan int64
}

type CampaignInfo string
type CampaignError string


const NAME = "Campaign"
const RPCTimeout = 200 * time.Millisecond

const (
	ACKNOWLEDGED_LEADER CampaignInfo = "acknowledged leader and returning successful response"
	APPLY_LOGS CampaignInfo = "applying logs to state machine and appending to WAL"
	HEARTBEATING CampaignInfo = "sending heartbeats..."
	LEADER_ELECTED CampaignInfo = "sending heartbeats after election..."
	MINIMUM_RESPONSES_RECEIVED CampaignInfo = "votes received meets requirement for minimum"
	PREPARING_SYNC CampaignInfo = "preparing to sync logs"
	RPC_SERVER_LISTENING CampaignInfo = "campaign grpc server listening"
	VOTE_RECEIVED CampaignInfo = "received requestVoteRPC"
	VOTE_GRANTED CampaignInfo = "granted vote"
)

const (
	APPEND_WAL_ERROR CampaignError = "append error"
	BROADCAST_ERROR CampaignError = "error broadcasting RPC"
	CONNECTION_POOL_ERROR CampaignError = "failed connection"
	DECODE_ERROR CampaignError = "error decoding"
	EXP_BACKOFF_ERROR CampaignError ="exponential backoff attempt error"
	HIGHER_TERM_ERROR CampaignError = "higher term discovered"
	HOST_UNREACHABLE CampaignError = "unreachable, setting status to dead"
	MINIMUM_RESPONSES_RECEIVED_ERROR CampaignError = "minimum required votes not received"
	PREPARE_APPEND_ENTRY_RPC_ERROR CampaignError = "error encoding log struct to string"
	READ_ERROR CampaignError = "read error"
	REPLICATED_LOG_ERROR CampaignError = "replicated log error"
	STATE_MACHINE_ERROR CampaignError = "error writing to state machine"
	SYNC_ERROR CampaignError = "error syncing logs for host"
	SYSTEM_ERROR CampaignError = "system error"
	TERM_LOWER_THAN_LOCAL_ERROR CampaignError = "request term lower than current term"
	TERM_MISMATCH_ERROR CampaignError = "log at request has mismatched term or does not exist"
)
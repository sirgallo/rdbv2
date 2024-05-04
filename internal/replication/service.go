package replication

import (
	"net"
	"time"
	"google.golang.org/grpc"
	"github.com/sirgallo/logger"
	
	"github.com/sirgallo/rdbv2/internal/replication/replication_proto"
	"github.com/sirgallo/rdbv2/internal/state"
	"github.com/sirgallo/rdbv2/internal/system"
	rdbUtils "github.com/sirgallo/rdbv2/internal/utils"
)


//=========================================== Replication Service


//	NewReplicationService:
//		create new replication service instance
func NewReplicationService(opts *ReplicationOpts) *ReplicationService {
	return &ReplicationService{
		Port: rdbUtils.NormalizePort(opts.Port),
		Pool: opts.Pool,
		CurrentSystem: opts.CurrentSystem,
		Systems: opts.Systems,
		AppendLogSignal: make(chan *state.StateOperation, AppendLogBuffSize),
		ReadChannel: make(chan *state.StateOperation, AppendLogBuffSize),
		WriteChannel: make(chan *state.StateOperation, AppendLogBuffSize),
		LeaderAcknowledgedSignal: make(chan bool),
		ForceHeartbeatSignal: make(chan bool),
		SyncLogChannel: make(chan string),
		SendSnapshotToSystemSignal: make(chan string),
		StateResponseChannel: make(chan *state.StateResponse, ResponseBuffSize),
		AppendLogsFollowerChannel: make(chan *replication_proto.AppendEntry, AppendLogBuffSize),
		AppendLogsFollowerRespChannel: make(chan bool),
		ApplyLogsFollowerChannel: make(chan int64),
		Log: *logger.NewCustomLog(NAME),
	}
}

//	Start the replication module/service:
//		launch the grc server for AppendEntryRPCbstart the log timeouts
func (rService *ReplicationService) StartReplicatedLogService(listener *net.Listener) {
	srv := grpc.NewServer()
	rService.Log.Info(RPC_SERVER_LISTENING, rService.CurrentSystem.Host, rService.Port)
	replication_proto.RegisterReplicationServiceServer(srv, rService)

	go func() {
		err := srv.Serve(*listener)
		if err != nil { rService.Log.Error("Failed to serve:", err.Error()) }
	}()

	rService.StartReplicatedLogTimeout()
}

// Start both leader and follower specific go routines
func (rService *ReplicationService) StartReplicatedLogTimeout() {
	go rService.LeaderGoRoutines()
	go rService.FollowerGoRoutines()
}

// 	Leader Go Routines:
//		separate go routines:
//			1.) heartbeat timeoutn and wait for timer to drain, signal heartbeat, and reset timer
//			2.) replicate log timeout and wait for timer to drain, signal to replicate logs to followers, and reset timer
//			3.) heartbeat on a set interval, heartbeat all of the followers in the cluster if leader
//			4.) append log signal on incoming logs from the request module, determine if the log is a read or write op and handle accordingly
//			5.) read operation handler on read operations, do not apply to replicated log and instead read directly from db -- since data is not modified, this is an optimization to improve latency on reads
//			6.) write operation handler appends logs to the replicated log in order as the leader receives them
//			7.) replicated log after the replicate log timeout completes, begin replication to followers
//			8.) sync logs for systems with inconsistent replicated logs, start a separate go routine to sync them back up to the leader
func (rService *ReplicationService) LeaderGoRoutines() {
	rService.HeartbeatTimer = time.NewTimer(HeartbeatInterval)
	rService.ReplicationTimer = time.NewTimer(ReplicationInterval)
	timeoutChan := make(chan bool)
	replicateLogsChan := make(chan bool)

	go func() {
		for {
			select {
			case <- rService.ResetTimeoutSignal:
				rService.resetHeartbeatTimer()
			case <- rService.HeartbeatTimer.C:
				timeoutChan <- true
				rService.resetHeartbeatTimer()
			}
		}
	}()

	go func() {
		for range rService.ReplicationTimer.C {
			replicateLogsChan <- true
			rService.resetReplicationTimer()
		}
	}()

	go func() {
		for {
			select {
			case <- timeoutChan:
				if rService.CurrentSystem.SysState == system.Leader {
					rService.Log.Info(HEARTBEATING)
					rService.Heartbeat()
				}
			case <- rService.ForceHeartbeatSignal:
				if rService.CurrentSystem.SysState == system.Leader {
					rService.attemptResetTimeout()
					rService.Log.Info(LEADER_ELECTED)
					rService.Heartbeat()
				}
			}
		}
	}()

	go func() {
		for newCmd := range rService.AppendLogSignal {
			if rService.CurrentSystem.SysState == system.Leader { 
				isReadOperation := func() bool {
					return newCmd.Action == state.GET || newCmd.Action == state.LISTCOLLECTIONS
				}()

				if isReadOperation {
					rService.ReadChannel <- newCmd
				}	else { rService.WriteChannel <- newCmd  }
			}
		}
	}()

	go func() {
		for readCmd := range rService.ReadChannel {
			resp, readErr := rService.CurrentSystem.State.Read(readCmd)
			if readErr != nil { rService.Log.Error(READ_ERROR, readErr.Error()) }
			rService.StateResponseChannel <- resp
		}
	}()

	go func() {
		var appendErr error

		for writeCmd := range rService.WriteChannel {
			appendErr = rService.AppendWALSync(writeCmd)
			if appendErr != nil { rService.Log.Error(APPEND_WAL_ERROR, appendErr.Error()) }
		}
	}()

	go func() {
		var replicationErr error

		for range replicateLogsChan {
			if rService.CurrentSystem.SysState == system.Leader {
				replicationErr = rService.ReplicateLogs()
				if replicationErr != nil { rService.Log.Error(REPLICATED_LOG_ERROR, replicationErr.Error()) }
			}
		}
	}()

	go func() {
		for host := range rService.SyncLogChannel {
			go func(host string) {
				_, syncErr := rService.SyncLogs(host)
				if syncErr != nil { rService.Log.Error(SYNC_ERROR, host, ":", syncErr.Error()) }
			}(host)
		}
	}()
}

// 	Follower Go Routines:
//		separate go routines:
//			1.) process logs as logs are received from AppendEntryRPCs from the leader, process the logs synchronously and signal to the request when complete
//			2.) apply logs to state machine when signalled by a request, and available, apply logs to the state machine up to the request commit index
func (rService *ReplicationService) FollowerGoRoutines() {
	go func() {
		var applyErr error

		for commitIndex := range rService.ApplyLogsFollowerChannel {
			applyErr = rService.ApplyLogsToStateFollower(commitIndex)
			if applyErr != nil { rService.Log.Error(STATE_MACHINE_ERROR, applyErr.Error()) }
		}
	}()
}

func (rService *ReplicationService) attemptResetTimeout() {
	select {
	case rService.ResetTimeoutSignal <- true:
	default:
	}
}
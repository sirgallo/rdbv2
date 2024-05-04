package replication

import (
	"context"
	"sync"
	"sync/atomic"
	"google.golang.org/grpc"
	"github.com/sirgallo/utils"
	
	"github.com/sirgallo/rdbv2/internal/replication/replication_proto"
	"github.com/sirgallo/rdbv2/internal/system"
)


//=========================================== Replication Client


// Heartbeat:
//	for all systems in the System Map, send an empty AppendEntryRPC:
//		if a higher term is discovered in a response, revert the leader back to follower state.
func (rService *ReplicationService) Heartbeat() error {
	var hbWG sync.WaitGroup
	var heartbeatErr error

	aliveSystems, _ := rService.GetAliveSystemsAndMinSuccessResps()
	rlRespChans := rService.createRLRespChannels(aliveSystems)

	defer close(rlRespChans.SuccessChan)
	defer close(rlRespChans.HigherTermDiscovered)

	requests := []ReplicatedLogRequest{}
	successfulResps := int64(0)

	var lastLogIndex int64
	lastLogIndex, _, heartbeatErr = rService.CurrentSystem.DetermineLastLogIdxAndTerm()
	if heartbeatErr != nil { return heartbeatErr }

	var preparedEntries *replication_proto.AppendEntry

	for _, sys := range aliveSystems {
		preparedEntries, heartbeatErr = rService.PrepareAppendEntryRPC(lastLogIndex, sys.NextIndex, true)
		if heartbeatErr != nil { return heartbeatErr }
		requests = append(requests, ReplicatedLogRequest{ Host: sys.Host, AppendEntry: preparedEntries })
	}

	hbWG.Add(1)
	go func() {
		defer hbWG.Done()
		for {
			select {
			case <- rlRespChans.BroadcastClose:
				return
			case <- rlRespChans.SuccessChan:
				atomic.AddInt64(&successfulResps, 1)
			case term :=<- rlRespChans.HigherTermDiscovered:
				rService.Log.Warn(HIGHER_TERM_ERROR)
				rService.CurrentSystem.TransitionToFollower(system.StateTransitionOpts{ CurrentTerm: &term })
				rService.attemptLeadAckSignal()
				return
			}
		}
	}()

	hbWG.Add(1)
	go func() {
		defer hbWG.Done()
		rService.broadcastAppendEntryRPC(requests, rlRespChans)
	}()

	hbWG.Wait()
	return nil
}

// ReplicateLogs:
//		1.) append the new log to the WAL on the Leader,
//			--> index is just the index of the last log + 1
//			--> term is the current term on the leader
//		2.) prepare AppendEntryRPCs for each system in the Systems Map
//			--> determine the next index of the system that the rpc is prepared for from the system object at NextIndex
//		3.) on responses
//			--> if the Leader receives a success signal from the majority of the nodes in the cluster, apply the logs to the state machine
//			--> if a response with a higher term than its own, revert to Follower state
//			--> if a response with a last log index less than current log index on leader, sync logs until up to date
func (rService *ReplicationService) ReplicateLogs() error {
	var repLogWG sync.WaitGroup
	var replicateErr error

	aliveSystems, minSuccessfulResps := rService.GetAliveSystemsAndMinSuccessResps()
	rlRespChans := rService.createRLRespChannels(aliveSystems)

	defer close(rlRespChans.SuccessChan)
	defer close(rlRespChans.HigherTermDiscovered)

	var lastLogIndex int64
	lastLogIndex, _, replicateErr = rService.CurrentSystem.DetermineLastLogIdxAndTerm()
	if replicateErr != nil { return replicateErr }
	if lastLogIndex == rService.CurrentSystem.CommitIndex { return nil }

	requests := []ReplicatedLogRequest{}
	successfulResps := int64(0)

	var preparedEntries *replication_proto.AppendEntry

	for _, sys := range aliveSystems {
		preparedEntries, replicateErr = rService.PrepareAppendEntryRPC(lastLogIndex, sys.NextIndex, false)
		if replicateErr != nil { return replicateErr }
		requests = append(requests, ReplicatedLogRequest{ Host: sys.Host, AppendEntry: preparedEntries })
	}

	repLogWG.Add(1)
	go func() {
		defer repLogWG.Done()

		var applyErr error
		for {
			select {
			case <- rlRespChans.BroadcastClose:		
				if successfulResps < int64(minSuccessfulResps) { rService.Log.Warn(MINIMUM_RESPONSES_RECEIVED_ERROR) }
				return
			case <- rlRespChans.SuccessChan:	
				atomic.AddInt64(&successfulResps, 1)
				if successfulResps >= int64(minSuccessfulResps) { 
					rService.Log.Info(MINIMUM_RESPONSES_RECEIVED, successfulResps)
					rService.Log.Info(APPLY_LOGS)

					rService.CurrentSystem.UpdateCommitIndex(lastLogIndex)
					applyErr = rService.ApplyLogs()
					if applyErr != nil { rService.Log.Error(STATE_MACHINE_ERROR, applyErr.Error()) }
					return
				}
			case term :=<- rlRespChans.HigherTermDiscovered:
				rService.Log.Warn(HIGHER_TERM_ERROR)
				rService.CurrentSystem.TransitionToFollower(system.StateTransitionOpts{ CurrentTerm: &term })
				rService.attemptLeadAckSignal()
				return
			}
		}
	}()

	repLogWG.Add(1)
	go func() {
		defer repLogWG.Done()
		broadcastErr := rService.broadcastAppendEntryRPC(requests, rlRespChans)
		if broadcastErr != nil { rService.Log.Error(BROADCAST_ERROR, broadcastErr.Error()) }
	}()

	repLogWG.Wait()
	return nil
}

// broadcastAppendEntryRPC:
//		utilized by all three functions above
//
//		for requests to be broadcasted:
//			1.) send AppendEntryRPCs in parallel to each follower in the cluster
//			2.) in each go routine handling each request, perform exponential backoff on failed requests until max retries
//			3.)
//				if err: remove system from system map and close all connections -- it has failed
//				if res:
//					if success:
//						--> update total successful replies and update the next index of the system to the last log index of the reply
//					else if failure and the reply has higher term than the leader:
//						--> update the state of the leader to follower, recognize a higher term and thus a more correct log
//						--> signal that a higher term has been discovered and cancel all leftover requests
//					otherwise if failure:
//						--> sync the follower up to the leader for any inconsistent log entries
func (rService *ReplicationService) broadcastAppendEntryRPC(requestsPerHost []ReplicatedLogRequest, rlRespChans ReplicationResponseChannels) error {
	var appendEntryWG sync.WaitGroup

	defer close(rlRespChans.BroadcastClose)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	for _, req := range requestsPerHost {
		appendEntryWG.Add(1)
		go func(req ReplicatedLogRequest) {
			defer appendEntryWG.Done()

			s, _ := rService.Systems.Load(req.Host)
			sys := s.(*system.System)
			conn, connErr := rService.Pool.GetConnection(sys.Host, rService.Port)
			if connErr != nil {
				rService.Log.Error(CONNECTION_POOL_ERROR, sys.Host + rService.Port, ":", connErr.Error())
				return
			}

			select {
			case <- ctx.Done():
				rService.Pool.PutConnection(sys.Host, conn)
				return
			default:
				res, rpcErr := rService.clientAppendEntryRPC(conn, sys, req)
				if rpcErr != nil { return }

				if res.Success {
					rlRespChans.SuccessChan <- 1
				} else {
					if res.Term > rService.CurrentSystem.CurrentTerm {
						rService.Log.Warn(HIGHER_TERM_ERROR, res.Term)
						rService.CurrentSystem.TransitionToFollower(system.StateTransitionOpts{ CurrentTerm: &res.Term })
						rlRespChans.HigherTermDiscovered <- res.Term
						cancel()
					} else {
						rService.Log.Warn(PREPARING_SYNC, sys.Host)
						sys.SetStatus(system.Busy)
						rService.SyncLogChannel <- sys.Host
					}
				}

				rService.Pool.PutConnection(sys.Host, conn)
			}
		}(req)
	}

	appendEntryWG.Wait()
	return nil
}

// clientAppendEntryRPC:
//		helper method for making individual rpc calls
//
//		perform exponential backoff
//			--> success: update system NextIndex and return result
//			--> error: remove system from system map and close all open connections
func (rService *ReplicationService) clientAppendEntryRPC(conn *grpc.ClientConn, sys *system.System, req ReplicatedLogRequest) (*replication_proto.AppendEntryResponse, error) {
	client := replication_proto.NewReplicationServiceClient(conn)
	appendEntryRPC := func() (*replication_proto.AppendEntryResponse, error) {
		ctx, cancel := context.WithTimeout(context.Background(), RPCTimeout)
		defer cancel()

		res, err := client.AppendEntryRPC(ctx, req.AppendEntry)
		if err != nil {
			rService.Log.Error(EXP_BACKOFF_ERROR, err.Error())
			return utils.GetZero[*replication_proto.AppendEntryResponse](), err 
		}

		return res, nil
	}

	maxRetries := 3
	expOpts := utils.ExpBackoffOpts{ MaxRetries: &maxRetries, TimeoutInNanosecs: 10000000 }
	expBackoff := utils.NewExponentialBackoffStrat[*replication_proto.AppendEntryResponse](expOpts)
	res, err := expBackoff.PerformBackoff(appendEntryRPC)
	if err != nil {
		rService.Log.Warn(sys.Host, HOST_UNREACHABLE)
		sys.SetStatus(system.Dead)
		rService.Pool.CloseConnections(sys.Host)
		return nil, err
	}

	sys.UpdateNextIndex(res.NextLogIndex)
	return res, nil
}

func (rService *ReplicationService) createRLRespChannels(aliveSystems []*system.System) ReplicationResponseChannels {
	broadcastClose := make(chan struct{})
	successChan := make(chan int, len(aliveSystems))
	higherTermDiscovered := make(chan int64, 1)
	return ReplicationResponseChannels{ BroadcastClose: broadcastClose, SuccessChan: successChan, HigherTermDiscovered: higherTermDiscovered }
}

func (rService *ReplicationService) attemptLeadAckSignal() {
	select {
	case rService.LeaderAcknowledgedSignal <- true:
	default:
	}
}
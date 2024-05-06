package replication

import (
	"context"
	"sync"
	"google.golang.org/grpc"
	"github.com/sirgallo/utils"
	
	"github.com/sirgallo/rdbv2/internal/replication/replication_proto"
	"github.com/sirgallo/rdbv2/internal/system"
)


//=========================================== Replication Client


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
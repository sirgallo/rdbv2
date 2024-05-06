package replication

import (
	"sync"
	"sync/atomic"
	
	"github.com/sirgallo/rdbv2/internal/replication/replication_proto"
	"github.com/sirgallo/rdbv2/internal/system"
)


//=========================================== Replication


// Replicate:
//		1.) append the new log to the WAL on the Leader,
//			--> index is just the index of the last log + 1
//			--> term is the current term on the leader
//		2.) prepare AppendEntryRPCs for each system in the Systems Map
//			--> determine the next index of the system that the rpc is prepared for from the system object at NextIndex
//		3.) on responses
//			--> if the Leader receives a success signal from the majority of the nodes in the cluster, apply the logs to the state machine
//			--> if a response with a higher term than its own, revert to Follower state
//			--> if a response with a last log index less than current log index on leader, sync logs until up to date
func (rService *ReplicationService) Replicate() error {
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
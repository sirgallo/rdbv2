package replication

import (
	"sync"
	"sync/atomic"
	
	"github.com/sirgallo/rdbv2/internal/replication/replication_proto"
	"github.com/sirgallo/rdbv2/internal/system"
)


//=========================================== Replication Heartbeat


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
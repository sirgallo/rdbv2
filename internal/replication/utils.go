package replication

import (
	"github.com/sirgallo/utils"

	"github.com/sirgallo/rdbv2/internal/replication/replication_proto"
	"github.com/sirgallo/rdbv2/internal/system"
)


//=========================================== Replication Utils


//	PrepareAppendEntryRPC:
//		1.) determine what entries to get, which will be the next log index forward for that particular system
//		2.) batch the entries
//		3.) encode the command entries to string
//		4.) create the rpc request from the log entry
func (rService *ReplicationService) PrepareAppendEntryRPC(
	lastLogIndex int64,
	nextIndex int64,
	isHeartbeat bool,
) (*replication_proto.AppendEntry, error) {
	var prepareRPCErr error

	lastLog, prepareRPCErr := rService.CurrentSystem.WAL.Read(lastLogIndex)
	if lastLog == nil { lastLog = &replication_proto.LogEntry{ Index: utils.GetZero[int64](), Term: utils.GetZero[int64]() } }
	if prepareRPCErr != nil { return nil, prepareRPCErr }

	var entries []*replication_proto.LogEntry
	var previousLogIndex, previousLogTerm int64

	if isHeartbeat {
		previousLogIndex = lastLog.Index
		previousLogTerm = lastLog.Term
		entries = nil
	} else {
		previousLog, prepareRPCErr := rService.CurrentSystem.WAL.Read(nextIndex - 1)
		if prepareRPCErr != nil { return nil, prepareRPCErr }

		if previousLog != nil {
			previousLogIndex = previousLog.Index
			previousLogTerm = previousLog.Term
		} else {
			previousLogIndex = utils.GetZero[int64]()
			previousLogTerm = system.DefaultLastLogTerm + 1
		}

		entries, prepareRPCErr = func() ([]*replication_proto.LogEntry, error) {
			batchSize := rService.determineBatchSize()
			totalToSend := lastLogIndex - nextIndex

			if totalToSend <= int64(batchSize) {
				allEntries, rangeErr := rService.CurrentSystem.WAL.GetRange(nextIndex, lastLogIndex)
				if rangeErr != nil { return nil, rangeErr }
				return allEntries, nil
			} else { 
				indexUpToBatch := nextIndex + int64(batchSize - 1)
				entriesInBatch, rangeErr := rService.CurrentSystem.WAL.GetRange(nextIndex, indexUpToBatch)
				if rangeErr != nil { return nil, rangeErr }

				return entriesInBatch, nil
			}
		}()

		if prepareRPCErr != nil { return nil, prepareRPCErr }
	}

	return &replication_proto.AppendEntry{
		Term: rService.CurrentSystem.CurrentTerm,
		LeaderId: rService.CurrentSystem.Host,
		PrevLogIndex: previousLogIndex,
		PrevLogTerm: previousLogTerm,
		Entries: entries,
		LeaderCommitIndex: rService.CurrentSystem.CommitIndex,
	}, nil
}

// GetAliveSystemsAndMinSuccessResps:
//		helper method for both determining the current alive systems in the cluster and also the minimum successful responses needed for committing logs to the state machine.
//		--> minimum is found by floor(total alive systems / 2) + 1
func (rService *ReplicationService) GetAliveSystemsAndMinSuccessResps() ([]*system.System, int) {
	var aliveSystems []*system.System
	var sys *system.System

	rService.Systems.Range(func(key, value interface{}) bool {
		sys = value.(*system.System)
		if sys.SysStatus == system.Ready { aliveSystems = append(aliveSystems, sys) }
		return true
	})

	totAliveSystems := len(aliveSystems) + 1
	return aliveSystems, (totAliveSystems / 2) + 1
}

//	resetHeartbeatTimer:
//		used to reset the heartbeat timer:
//		--> if unable to stop the timer, drain the timer
//		--> reset the timer with the heartbeat interval
func (rService *ReplicationService) resetHeartbeatTimer() {
	if ! rService.HeartbeatTimer.Stop() {
		select {
		case <-rService.HeartbeatTimer.C:
		default:
		}
	}

	rService.HeartbeatTimer.Reset(HeartbeatInterval)
}

//	resetReplicationTimer:
//		same as above.
func (rService *ReplicationService) resetReplicationTimer() {
	if ! rService.ReplicationTimer.Stop() {
		select {
		case <-rService.ReplicationTimer.C:
		default:
		}
	}

	rService.ReplicationTimer.Reset(ReplicationInterval)
}

//	determineBatchSize:
//		TODO: not implemented
//			just use 10000 for testing right now --> TODO, make this dynamic maybe find a way to get latest network MB/s and avg log size and determine based on this
func (rService *ReplicationService) determineBatchSize() int {
	return 5000
}

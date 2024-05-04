package replication

import (
	"context"

	"github.com/sirgallo/rdbv2/internal/replication/replication_proto"
	"github.com/sirgallo/rdbv2/internal/system"
)


//=========================================== RepLog Server


/*
	AppendEntryRPC:
		grpc server implementation

		when an AppendEntryRPC is made to the appendEntry server
			1.) if the host of the incoming request is not in the systems map, store it
			2.) reset the election timeout regardless of success or failure response
			3.) if the request has a term lower than the current term of the system
				--> return a failure response with the term of the system
			4.) if the term of the replicated log on the system is not the term of the request or is not present
				--> return a failure response, with the earliest known index for the term, or from the latest term known on the 
					follower to update NextIndex
			5.) acknowledge that the request is legitimate and send signal to reset the leader election timeout
			6.) for all of the entries of the incoming request
				--> if the term of the replicated log associated with the index of the incoming entry is not the same
					as the request, remove up to the entry in the log on the system and begin appending logs
				--> otherwise, just prepare the batch of logs to be range appended to the WAL
			7.) if the commit index of the incoming request is higher than on the system, commit logs up to the commit index from
					last applied for the state machine on the system
			8.) if logs are at least up to date with the leader's commit index:
				--> return a success response with the index of the latest log applied to the replicated log
					else:
				--> return a failed response so the follower can sync itself up to the leader if inconsistent log length

		The AppendEntryRPC server can process requests asynchronously, but when appending to the replicated log, the request must pass
		the log entries into a buffer where they will be appended/processed synchronously in a separate go routine. For requests like heartbeats,
		this ensures that the context timeout period should not be reached unless extremely high system load, and should improve overall 
		throughput of requests sent to followers. So even though requests are processed asynchronously, logs are still processed synchronously.

		For applying logs to the statemachine, a separate go routine is also utilized. A signal is attempted with the current request leader
		commit index, and is dropped if the go routine is already in the process of applying logs to the state machine. I guess this could be 
		considered an "opportunistic" approach to state machine application. The above algorithm for application does not change.
*/

func (rService *ReplicationService) AppendEntryRPC(
		ctx context.Context,
		req *replication_proto.AppendEntry,
	) (*replication_proto.AppendEntryResponse, error) {
	var ok bool
	var s any
	var appendEntryRPCErr error

	s, ok = rService.Systems.Load(req.LeaderId)
	if ! ok {
		sys := &system.System{ Host: req.LeaderId, SysStatus: system.Ready }
		rService.Systems.Store(sys.Host, sys)
	} else {
		sys := s.(*system.System)
		sys.SetStatus(system.Ready)
	}

	rService.attemptLeadAckSignal()
	latestKnownLogTerm := int64(0)

	var total int
	total, appendEntryRPCErr = rService.CurrentSystem.WAL.GetTotal()
	if appendEntryRPCErr != nil { 
		rService.Log.Error(REPLICATED_LOG_ERROR, appendEntryRPCErr.Error())
		return nil, appendEntryRPCErr
	}

	var lastLog *replication_proto.LogEntry
	lastLog, appendEntryRPCErr = rService.CurrentSystem.WAL.GetLatest()
	if lastLog != nil { latestKnownLogTerm = lastLog.Term }
	if appendEntryRPCErr != nil { 
		rService.Log.Error(REPLICATED_LOG_ERROR, appendEntryRPCErr.Error())
		return nil, appendEntryRPCErr
	}
	
	failedIndexToFetch := req.PrevLogIndex - 1

	var lastIndexedLog *replication_proto.LogEntry
	lastIndexedLog, appendEntryRPCErr = rService.CurrentSystem.WAL.GetIndexedEntryForTerm(latestKnownLogTerm)
	if lastIndexedLog != nil { failedIndexToFetch = lastIndexedLog.Index }
	if appendEntryRPCErr != nil { 
		rService.Log.Error(REPLICATED_LOG_ERROR, appendEntryRPCErr.Error())
		return nil, appendEntryRPCErr
	}

	var failedNextIndex int64
	failedNextIndex, appendEntryRPCErr = func() (int64, error) {
		if total == 0 || failedIndexToFetch < 0 { return 0, nil }
		return failedIndexToFetch, nil
	}()

	if appendEntryRPCErr != nil { return nil, appendEntryRPCErr }

	handleReqTerm := func() bool { return req.Term >= rService.CurrentSystem.CurrentTerm }
	handleReqValidTermAtIndex := func() (bool, error) {
		currEntry, readErr := rService.CurrentSystem.WAL.Read(req.PrevLogIndex)
		if readErr != nil { return false, readErr }
		if total == 0 || req.Entries == nil { return true, nil } // special case for when a system has empty replicated log or hearbeats where we don't check
		
		return currEntry != nil && currEntry.Term == req.PrevLogTerm, nil
	}

	reqTermOk := handleReqTerm()
	if ! reqTermOk {
		rService.Log.Warn(TERM_LOWER_THAN_LOCAL_ERROR)
		return rService.generateResponse(failedNextIndex, false), nil
	}

	rService.CurrentSystem.SetCurrentLeader(req.LeaderId)

	var reqTermValid bool
	reqTermValid, appendEntryRPCErr = handleReqValidTermAtIndex()
	if appendEntryRPCErr != nil { 
		rService.Log.Error(READ_ERROR, appendEntryRPCErr.Error())
		return rService.generateResponse(failedNextIndex, false), nil
	}
	
	if ! reqTermValid {
		rService.Log.Warn(TERM_MISMATCH_ERROR)
		return rService.generateResponse(failedNextIndex, false), nil
	}

	ok, appendEntryRPCErr = rService.HandleReplicateLogs(req)
	if ! ok { return rService.generateResponse(failedNextIndex, false), nil }
	if appendEntryRPCErr != nil { 
		rService.Log.Error(REPLICATED_LOG_ERROR, appendEntryRPCErr.Error())
		return rService.generateResponse(failedNextIndex, false), nil
	}

	var lastLogIndex int64
	lastLogIndex, _, appendEntryRPCErr = rService.CurrentSystem.DetermineLastLogIdxAndTerm()
	if appendEntryRPCErr != nil { 
		rService.Log.Error(REPLICATED_LOG_ERROR, appendEntryRPCErr.Error())
		return rService.generateResponse(failedNextIndex, false), nil
	}

	nextLogIndex := lastLogIndex + 1
	successfulResp := rService.generateResponse(nextLogIndex, true)
	// rService.Log.Info(req.LeaderId, ACKNOWLEDGED_LEADER, successfulResp)
	return successfulResp, nil
}

/*
	Handle Replicate Logs:
		For incoming requests, if request contains log entries, pipe into buffer to be processed
		otherwise, attempt signalling to log application channel to update state machine
*/

func (rService *ReplicationService) HandleReplicateLogs(req *replication_proto.AppendEntry) (bool, error) {
	var logsToAppend []*replication_proto.LogEntry

	if req.Entries != nil {
		appendLogToReplicatedLog := func(entry *replication_proto.LogEntry) error {			
			logsToAppend = append(logsToAppend, entry)
			return nil
		}

		for _, entry := range req.Entries {
			currEntry, replicateErr := rService.CurrentSystem.WAL.Read(entry.Index)
			if replicateErr != nil { return false, replicateErr }
			if currEntry != nil {
				if currEntry.Term != entry.Term {
					var lastLogIndex int64
					lastLogIndex, _, replicateErr = rService.CurrentSystem.DetermineLastLogIdxAndTerm()
					if replicateErr != nil { return false, replicateErr }

					_, _, replicateErr = rService.CurrentSystem.WAL.RangeDelete(currEntry.Index, lastLogIndex)
					if replicateErr != nil { return false, replicateErr }
				}
			}
			
			replicateErr = appendLogToReplicatedLog(entry)
			if replicateErr != nil { return false, replicateErr }
		}
	
		rService.CurrentSystem.WAL.RangeAppend(logsToAppend)
		latestLog, replicateErr := rService.CurrentSystem.WAL.GetLatest()
		if replicateErr != nil { return false, replicateErr }
		if latestLog != nil { rService.CurrentSystem.UpdateCommitIndex(latestLog.Index) }
	}

	select {
	case rService.ApplyLogsFollowerChannel <- req.LeaderCommitIndex:
	default:
	}
	
	return true, nil
}

/*
	Process Logs Follower:
		helper method used for replicating the logs to the follower's replicated log

		instead of appending one at a time, we can batch all of the log entries into a single bolt db transaction to reduce 
		overhead and total transactions performed on the db, which should improve performance

		this is run in a separate go routine so that requests can be processed asynchronously, but logs can be processed synchronously
		as they enter the buffer
*/

/*
func (rlService *ReplicatedLogService) ProcessLogsFollower(req *replogrpc.AppendEntry) (bool, error) {
	logTransform := func(entry *replogrpc.LogEntry) *log.LogEntry {
		cmd, decErr := utils.DecodeStringToStruct[statemachine.StateMachineOperation](entry.Command)
		if decErr != nil {
			rlService.Log.Error("error on decode -->", decErr.Error())
			return nil
		}

		return &log.LogEntry{
			Index: entry.Index,
			Term: entry.Term,
			Command: *cmd,
		}
	}

	var logsToAppend []*log.LogEntry

	appendLogToReplicatedLog := func(entry *replogrpc.LogEntry) error {
		newLog := logTransform(entry)
		if newLog == nil { return errors.New("log transform failed, new log is null") }
		
		logsToAppend = append(logsToAppend, newLog)

		return nil
	}

	for idx, entry := range req.Entries {
		currEntry, readErr := rlService.CurrentSystem.WAL.Read(entry.Index)
		if readErr != nil { return false, readErr }

		if currEntry != nil {
			if currEntry.Term != entry.Term {
				transformedLogs := utils.Map[*replogrpc.LogEntry, *log.LogEntry](req.Entries[:idx + 1], logTransform)
				rangeUpdateErr := rlService.CurrentSystem.WAL.RangeAppend(transformedLogs)
				if rangeUpdateErr != nil { return false, rangeUpdateErr }
			}
		} else {
			appendErr := appendLogToReplicatedLog(entry)
			if appendErr != nil { return false, appendErr }
		}
	}

	rlService.CurrentSystem.WAL.RangeAppend(logsToAppend)

	latestLog, latestErr := rlService.CurrentSystem.WAL.GetLatest()
	if latestErr != nil { return false, latestErr }

	rlService.CurrentSystem.UpdateCommitIndex(latestLog.Index)

	return true, nil
}
*/

/*
	Apply Logs To State Machine Follower:
		helper method for applying logs to the state machine up to the leader's last commit index or 
		last known log on the system if it is less than the commit index of the leader

		again, this is run in a separate go routine, with opportunistic approach
*/

func (rService *ReplicationService) ApplyLogsToStateFollower(leaderCommitIndex int64) error {
	var applyLogsErr error

	min := func(idx1, idx2 int64) int64 {
		if idx1 < idx2 { return idx1 }
		return idx2
	}

	logAtCommitIndex, applyLogsErr := rService.CurrentSystem.WAL.Read(rService.CurrentSystem.CommitIndex)
	if logAtCommitIndex == nil { return nil }
	if applyLogsErr != nil { return applyLogsErr }

	if leaderCommitIndex > rService.CurrentSystem.CommitIndex {
		var lastLogIndex int64
		lastLogIndex, _, applyLogsErr = rService.CurrentSystem.DetermineLastLogIdxAndTerm()
		if applyLogsErr != nil { return applyLogsErr }
	
		minCommitIndex := min(leaderCommitIndex, lastLogIndex)
		rService.CurrentSystem.CommitIndex = minCommitIndex
		applyLogsErr = rService.ApplyLogs()
		if applyLogsErr != nil { return applyLogsErr }
	}

	return nil
}

func (rService *ReplicationService) generateResponse(
	lastLogIndex int64,
	success bool,
) *replication_proto.AppendEntryResponse {
	return &replication_proto.AppendEntryResponse{ Term: rService.CurrentSystem.CurrentTerm, NextLogIndex: lastLogIndex, Success: success }
}
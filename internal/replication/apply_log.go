package replication

import (
	"github.com/sirgallo/array"
	"github.com/sirgallo/utils"
	
	"github.com/sirgallo/rdbv2/internal/replication/replication_proto"
	"github.com/sirgallo/rdbv2/internal/state"
	"github.com/sirgallo/rdbv2/internal/system"
)


//=========================================== Apply Logs


/*
	shared apply log utility function
		1.) transform the logs to pass to the state machine
		2.) pass the transformed entries into the bulk apply function of the state machine, which will perform 
			the state machine operations while applying the logs, returning back the responses to the clients' 
			commands
		3.) block until completed and failed entries are returned
		4.) for all responses:
			if the commit failed: throw an error since the the state machine was incorrectly committed to
			if the commit completed: update the last applied field on the system to the index of the log
				entry
*/

func (rService *ReplicationService) ApplyLogs() error {
	var applyLogsErr error

	start := rService.CurrentSystem.LastApplied + 1 // next to apply after last known applied
	end := rService.CurrentSystem.CommitIndex  // include up to committed
	
	var logsToBeApplied []*replication_proto.LogEntry
	if start == end {
		entry, applyLogsErr := rService.CurrentSystem.WAL.Read(start)
		if entry == nil { return nil }
		if applyLogsErr != nil { return applyLogsErr }

		logsToBeApplied = append(logsToBeApplied, entry)
	} else {
		logsToBeApplied, applyLogsErr = rService.CurrentSystem.WAL.GetRange(start, end)
		if logsToBeApplied == nil { return nil }
		if applyLogsErr != nil { return applyLogsErr }
	}

	lastLogToBeApplied := logsToBeApplied[len(logsToBeApplied) - 1]
	transform := func(logEntry *replication_proto.LogEntry) *state.StateOperation {
		var stateOp *state.StateOperation

		stateOp, applyLogsErr = utils.DecodeStringToStruct[state.StateOperation](logEntry.Command)
		if applyLogsErr != nil { return nil }
		return stateOp
	}

	// NOTE: future update:
	// 	to potentially increase throughput, make this a channel where as operations are transformed, they are being consumed by writer
	//	bottleneck will then become disk speed
	stateOperations := array.Map[*replication_proto.LogEntry, *state.StateOperation](logsToBeApplied, transform)

	var bulkApplyResps []*state.StateResponse
	bulkApplyResps, applyLogsErr = rService.CurrentSystem.State.BulkWrite(stateOperations)
	if applyLogsErr != nil { return applyLogsErr }

	if rService.CurrentSystem.SysState == system.Leader {
		for _, resp := range bulkApplyResps { rService.StateResponseChannel <- resp }
	}
	
	rService.CurrentSystem.UpdateLastApplied(lastLogToBeApplied.Index)
	return nil
}
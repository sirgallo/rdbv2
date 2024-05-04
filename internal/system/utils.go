package system

import "github.com/sirgallo/rdbv2/internal/replication/replication_proto"


//=========================================== System Utils

/*
	Determine Last Log Idx And Term:
		get the last index and term from the replicated log
		1.) if the log length is greater than 0
			get the log at the end of the replicated log and return its index and term
		2.) otherwise
			we can assume this is a new system, so we default the index to -1 and term to 0
			to indicate this
*/

func (sys *System) DetermineLastLogIdxAndTerm() (int64, int64, error) {
	var lastLog *replication_proto.LogEntry
	var lastLogIndex, lastLogTerm int64
	var lastLogErr error

	lastLog, lastLogErr = sys.WAL.GetLatest()
	if lastLogErr != nil { return 0, 0, lastLogErr }
	
	if lastLog != nil {
		lastLogIndex = lastLog.Index
		lastLogTerm = lastLog.Term
	} else {
		lastLogIndex = DefaultLastLogIndex // -1 symbolizes empty log
		lastLogTerm = DefaultLastLogTerm
	}

	return lastLogIndex, lastLogTerm, nil
}
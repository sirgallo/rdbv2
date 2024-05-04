package replication 

import (
	"github.com/sirgallo/utils"

	"github.com/sirgallo/rdbv2/internal/replication/replication_proto"
	"github.com/sirgallo/rdbv2/internal/state"
)


//=========================================== RepLog Append WAL


/*
	As new requests are received, append the logs in order to the replicated log/WAL
*/

func (rService *ReplicationService) AppendWALSync(cmd *state.StateOperation) error {
	var appendErr error

	var lastLogIndex int64
	lastLogIndex, _, appendErr = rService.CurrentSystem.DetermineLastLogIdxAndTerm()
	if appendErr != nil { return appendErr }

	var encodedCmd string
	encodedCmd, appendErr = utils.EncodeStructToString[*state.StateOperation](cmd)
	if appendErr != nil {
		rService.Log.Error(ENCODE_ERROR, appendErr.Error())
		return appendErr 
	}

	newLog := &replication_proto.LogEntry{ Index: lastLogIndex + 1, Term: rService.CurrentSystem.CurrentTerm, Command: encodedCmd }
	appendErr = rService.CurrentSystem.WAL.Append(newLog)
	if appendErr != nil {
		rService.Log.Error(APPEND_WAL_ERROR, appendErr.Error())
		return appendErr 
	}

	return nil
}
package replication 

import (
	"github.com/sirgallo/rdbv2/internal/replication/replication_proto"
	"github.com/sirgallo/rdbv2/internal/state"
)


//=========================================== Replication Append WAL


//	AppendWALSync:
//		as new requests are received, append the logs in order to the replicated log/WAL.
func (rService *ReplicationService) AppendWALSync(cmd *state.StateOperation) error {
	var appendErr error

	var lastLogIndex int64
	lastLogIndex, _, appendErr = rService.CurrentSystem.DetermineLastLogIdxAndTerm()
	if appendErr != nil { return appendErr }

	appendErr = rService.CurrentSystem.WAL.Append(
		&replication_proto.LogEntry{ 
			Index: lastLogIndex + 1,
			Term: rService.CurrentSystem.CurrentTerm, 
			Command: &replication_proto.Command{
				RequestId: []byte(cmd.RequestId),
				Action: []byte(cmd.Action),
				Payload: &replication_proto.CommandPayload{
					Collection: []byte(cmd.Payload.Collection),
					Value: []byte(cmd.Payload.Value),
				},
			},
		},
	)

	if appendErr != nil {
		rService.Log.Error(APPEND_WAL_ERROR, appendErr.Error())
		return appendErr 
	}

	return nil
}
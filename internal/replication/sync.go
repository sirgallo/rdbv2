package replication

import (
	"google.golang.org/grpc"

	"github.com/sirgallo/rdbv2/internal/replication/replication_proto"
	"github.com/sirgallo/rdbv2/internal/system"
)


//=========================================== Replication Sync Logs


// Syncs:
//	helper method for handling syncing followers who have inconsistent logs
//		while unsuccessful response:
//			send AppendEntryRPC to follower with logs starting at the follower's NextIndex
//			if error: return false, error
//			on success: return true, nil --> the log is now up to date with the leader
//
//	if the earliest log on the leader is greater then the next index of the system,
//	send a signal to send the latest snapshot to the follower and perform log replication,
//	since this means that follower was not in the cluster during the latest log compaction event
func (rService *ReplicationService) Sync(host string) (bool, error) {
	var syncErr error

	s, _ := rService.Systems.Load(host)
	sys := s.(*system.System)

	var conn *grpc.ClientConn
	conn, syncErr = rService.Pool.GetConnection(sys.Host, rService.Port)
	if syncErr != nil {
		rService.Log.Error(CONNECTION_POOL_ERROR, sys.Host + rService.Port, ":", syncErr.Error())
		return false, syncErr
	}

	var earliestLog *replication_proto.LogEntry
	var lastLogIndex int64
	var preparedEntries *replication_proto.AppendEntry
	var res *replication_proto.AppendEntryResponse

	for {
		earliestLog, syncErr = rService.CurrentSystem.WAL.GetEarliest()
		if syncErr != nil { return false, syncErr }

		lastLogIndex, _, syncErr = rService.CurrentSystem.DetermineLastLogIdxAndTerm()
		if syncErr != nil { return false, syncErr }

		preparedEntries, syncErr = rService.PrepareAppendEntryRPC(lastLogIndex, sys.NextIndex, false)
		if syncErr != nil { return false, syncErr }

		res, syncErr = rService.clientAppendEntryRPC(conn, sys, ReplicatedLogRequest{ Host: sys.Host, AppendEntry: preparedEntries })
		if syncErr != nil { return false, syncErr }

		if res.Success {
			sys.SetStatus(system.Ready)
			rService.Pool.PutConnection(sys.Host, conn)
			return true, nil
		} else if earliestLog != nil && sys.NextIndex < earliestLog.Index {
			rService.SendSnapshotToSystemSignal <- sys.Host
			return true, nil
		}
	}
}
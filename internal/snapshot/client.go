package snapshot

import (
	"context"
	"errors"
	"io"
	"os"
	"sync"
	"sync/atomic"
	"google.golang.org/grpc"

	"github.com/sirgallo/rdbv2/internal/snapshot/snapshot_proto"
	"github.com/sirgallo/rdbv2/internal/stats"
	"github.com/sirgallo/rdbv2/internal/system"
)


//=========================================== Snapshot Client

//	Snapshot
//		snapshot the current state of of the state machine and the replicated log:
//			1.) get the latest known log entry, to set on the snapshotrpc for last included index and term of logs in the snapshot
//			2.) snapshot the current state machine
//				--> since the db is a boltdb instance, we'll take a backup of the data and gzipped, returning the filepath where the snapshot is stored on the system as a reference
//			3.) set the snapshot entry in the replicated log db in an index
//				--> the entry contains last included index, last included term, and the filepath on the system to the latest snapshot
//				--> snapshots are stored durably and snapshot entry can be seen as a pointer
//			4.) broadcast the new snapshot to all other systems. so send the entry with the filepath and file compressed to byte representation
//			5.) if the broadcast was successful, delete all logs in the replicated log up to the last included log
//				--> so compact the log only when the broadcast received the minimum number of successful responses in the quorum
func (snpService *SnapshotService) Snapshot() error {
	var snapshotErr error

	lastAppliedLog, snapshotErr := snpService.CurrentSystem.WAL.Read(snpService.CurrentSystem.LastApplied)
	if lastAppliedLog == nil { return nil }
	if snapshotErr != nil { return snapshotErr }

	var snapshotFile string
	snapshotFile, snapshotErr = snpService.CurrentSystem.State.SnapshotStat()
	if snapshotErr != nil { return snapshotErr }
	
	snpService.Log.Info(UPDATE_SNAPSHOT, snapshotFile)
	snapshotEntry := &snapshot_proto.SnapshotEntry{ LastIncludedIndex: lastAppliedLog.Index, LastIncludedTerm: lastAppliedLog.Term, SnapshotFilePath: snapshotFile }
	snapshotErr = snpService.CurrentSystem.WAL.SetSnapshot(snapshotEntry)
	if snapshotErr != nil { return snapshotErr }

	snpService.Log.Debug(ATTEMPTING_COMPACTION, lastAppliedLog.Index - 1)
	var totBytesRem, totKeysRem int64
	totBytesRem, totKeysRem, snapshotErr = snpService.CurrentSystem.WAL.DeleteLogsUpToLastIncluded(lastAppliedLog.Index - 1)
	if snapshotErr != nil { 
		snpService.Log.Error(WRITE_ERROR)
		return snapshotErr 
	}

	snpService.Log.Debug(TOTAL_REMOVED, totBytesRem, totKeysRem)
	initSnapShotRPC := &snapshot_proto.SnapshotChunk{ LastIncludedIndex: lastAppliedLog.Index, LastIncludedTerm: lastAppliedLog.Term, SnapshotFilePath: snapshotFile }

	var ok bool
	ok, snapshotErr = snpService.BroadcastSnapshotRPC(initSnapShotRPC)
	if snapshotErr != nil { return snapshotErr }
	if ! ok { return errors.New(string(MINIMUM_SUCCESSFUL_RESPONSES_ERROR)) }

	var statObj *stats.Stats
	statObj, snapshotErr = stats.CalculateCurrentStats()
	if snapshotErr != nil { return snapshotErr }

	snapshotErr = snpService.CurrentSystem.WAL.SetStat(*statObj)
	if snapshotErr != nil { return snapshotErr }

	snpService.CurrentSystem.SetStatus(system.Ready)
	return nil
}

//	UpdateIndividualSystem
//		when a node restarts and rejoins the cluster or a new node is added:
//			1.) set the node to send to busy so interactions with other followers continue
//			2.) create a snapshotrpc for the node that contains the last snapshot
//			3.) send the rpc to the node
//			4.) if successful, update the next index for the node to the last included index and set the system status to ready
func (snpService *SnapshotService) UpdateIndividualSystem(host string) error {
	var updateErr error

	s, _ := snpService.Systems.Load(host)
	sys := s.(*system.System)

	sys.SetStatus(system.Busy)
	snpService.Log.Info(UPDATE_SNAPSHOT, sys.Host)

	var snapshot *snapshot_proto.SnapshotEntry
	snapshot, updateErr = snpService.CurrentSystem.WAL.GetSnapshot()
	if updateErr != nil { 
		snpService.Log.Error(READ_ERROR)
		return updateErr 
	}

	snaprpc := &snapshot_proto.SnapshotChunk{
		LastIncludedIndex: snapshot.LastIncludedIndex,
		LastIncludedTerm: snapshot.LastIncludedTerm,
		SnapshotFilePath: snapshot.SnapshotFilePath,
	}

	_, updateErr = snpService.ClientSnapshotRPC(sys, snaprpc)
	if updateErr != nil { 
		snpService.Log.Error(RPC_ERROR, updateErr.Error())
		return updateErr
	}

	sys.UpdateNextIndex(snapshot.LastIncludedIndex)
	sys.SetStatus(system.Ready)
	return nil
}

//	BroadcastSnapshotRPC:
//		shared Broadcast RPC function:
//			for requests to be broadcasted:
//				1.) send SnapshotRPCs in parallel to each follower in the cluster
//				2.) in each go routine handling each request, perform exponential backoff on failed requests until max retries
//				3.)
//					if err: remove system from system map and close all connections -- it has failed
//					if res:
//						if success update total successful replies
//				4.) if total successful responses are greater than minimum, return success, otherwise failed
func (snpService *SnapshotService) BroadcastSnapshotRPC(snapshot *snapshot_proto.SnapshotChunk) (bool, error) {
	var snapshotWG sync.WaitGroup

	aliveSystems, minResps := snpService.GetAliveSystemsAndMinSuccessResps()
	successfulResps := int64(0)

	for _, sys := range aliveSystems {
		snapshotWG.Add(1)
		go func (sys *system.System) {
			defer snapshotWG.Done()
			res, rpcErr := snpService.ClientSnapshotRPC(sys, snapshot)
			if rpcErr != nil { 
				snpService.Log.Error(RPC_ERROR, rpcErr.Error())
				return
			}

			if res.Success { atomic.AddInt64(&successfulResps, 1) }
		}(sys)
	}

	snapshotWG.Wait()

	if successfulResps >= int64(minResps) {
		snpService.Log.Info(MINIMUM_SUCCESSFUL_RESPONSES, successfulResps)
		return true, nil
	} else { 
		snpService.Log.Warn(MINIMUM_SUCCESSFUL_RESPONSES_ERROR, successfulResps)
		return false, nil 
	}
}

//	ClientSnapshotRPC:
//		helper method for making individual rpc calls
//
//		a grpc stream is used here:
//			1.) open the file and read the snapshot file in chunks, passing the chunks into the snapshot stream channel
//			2.) as new chunks enter the channel, send them to the target follower
//			3.) close the snapshot stream when the file has been read, finish passing the rest of the chunks and return the response from closing the stream
func (snpService *SnapshotService) ClientSnapshotRPC(sys *system.System, initSnapshotShotReq *snapshot_proto.SnapshotChunk) (*snapshot_proto.SnapshotStreamResponse, error) {
	var clientSnapshotErr error

	var conn *grpc.ClientConn
	conn, clientSnapshotErr = snpService.Pool.GetConnection(sys.Host, snpService.Port)
	if clientSnapshotErr != nil {
		snpService.Log.Error(CONNECTION_POOL_ERROR, sys.Host + snpService.Port, ":", clientSnapshotErr.Error())
		return nil, clientSnapshotErr
	}

	client := snapshot_proto.NewSnapshotServiceClient(conn)

	var stream snapshot_proto.SnapshotService_StreamSnapshotRPCClient
	stream, clientSnapshotErr = client.StreamSnapshotRPC(context.Background())
	if clientSnapshotErr != nil { return nil, clientSnapshotErr }

	snapshotStreamChannel := make(chan []byte, 100000)

	go func() {
		snpService.ReadSnapshotContentStream(initSnapshotShotReq.SnapshotFilePath, snapshotStreamChannel)
		close(snapshotStreamChannel)
	}()

	sendSnapshotChunk := func(chunk []byte) error {
		snapshotWithChunk := &snapshot_proto.SnapshotChunk{
			LastIncludedIndex: initSnapshotShotReq.LastIncludedIndex,
			LastIncludedTerm: initSnapshotShotReq.LastIncludedTerm,
			SnapshotFilePath: initSnapshotShotReq.SnapshotFilePath,
			SnapshotChunk: chunk,
		}

		streamErr := stream.Send(snapshotWithChunk)
		if streamErr != nil { return streamErr }
		return nil
	}
	
	for chunk := range snapshotStreamChannel {
		clientSnapshotErr = sendSnapshotChunk(chunk)
		if clientSnapshotErr != nil { return nil, clientSnapshotErr }
	}

	var res *snapshot_proto.SnapshotStreamResponse
	res, clientSnapshotErr = stream.CloseAndRecv()
	if clientSnapshotErr != nil { return nil, clientSnapshotErr }

	snpService.Log.Debug(STREAM_SUCCESS, sys.Host)
	return res, nil
}

//	ReadSnapshotContentStream:
//		when sending the snapshot, read the current snapshot file in chunks to create a read stream.
func (snpService *SnapshotService) ReadSnapshotContentStream(snapshotFilePath string, snapshotStreamChannel chan []byte) error {
	var snapshotFile *os.File
	var readErr error

	snapshotFile, readErr = os.Open(snapshotFilePath)
	if readErr != nil { return readErr }
	defer snapshotFile.Close()

	for {
		buffer := make([]byte, ChunkSize)
		_, readErr = snapshotFile.Read(buffer)
		if readErr == io.EOF { return nil }
		if readErr != nil { return readErr }

		snapshotStreamChannel <- buffer
	}
}
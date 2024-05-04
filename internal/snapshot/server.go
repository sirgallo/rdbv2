package snapshot

import (
	"io"
	"os"

	"github.com/sirgallo/rdbv2/internal/snapshot/snapshot_proto"
)


//=========================================== Snapshot Server


/*
	StreamSnapshotRPC
		grpc server implementation

		as snapshot chunks enter the stream
			1.) if first chunk, set the metadata for the snapshot (last included index, term, and file path) and open the file
				to stream the compressed chunks into
			2.) for each chunk, write to the snapshot file
			3.) on stream end, break
			4.) index the snapshot in the wal db
			5.) compact the logs up to the last included log
			6.) return a successful response to the leader
*/

func (snpService *SnapshotService) StreamSnapshotRPC(stream snapshot_proto.SnapshotService_StreamSnapshotRPCServer) error {
	var snapshotFile *os.File
	var snapshotFilePath string
	var snapshotChunk *snapshot_proto.SnapshotChunk
	var firstChunkReceived bool
	var lastIncludedIndex, lastIncludedTerm int64
	var streamErr error

	for {
		snapshotChunk, streamErr = stream.Recv()
		if streamErr == io.EOF { break }
		if streamErr != nil { 
			snpService.Log.Error(STREAM_REC_ERROR, streamErr.Error())
			return streamErr 
		}

		if ! firstChunkReceived {
			snapshotFilePath = snapshotChunk.SnapshotFilePath
			lastIncludedIndex = snapshotChunk.LastIncludedIndex
			lastIncludedTerm = snapshotChunk.LastIncludedTerm
			firstChunkReceived = true
			
			snapshotFile, streamErr = os.Create(snapshotFilePath)
			if streamErr != nil { return streamErr }
			defer snapshotFile.Close()
		}

		_, streamErr = snapshotFile.Write(snapshotChunk.SnapshotChunk)
		if streamErr != nil { 
			snpService.Log.Error(WRITE_ERROR, streamErr.Error())
			return streamErr
		}
	}

	snpService.Log.Info(SNAPSHOT_WRITTEN)
	snapshotEntry := &snapshot_proto.SnapshotEntry{
		LastIncludedIndex: lastIncludedIndex,
		LastIncludedTerm: lastIncludedTerm,
		SnapshotFilePath: snapshotFilePath,
	}

	streamErr = snpService.CurrentSystem.WAL.SetSnapshot(snapshotEntry)
	if streamErr != nil { 
		snpService.Log.Error(UPDATE_SNAPSHOT_ERROR, streamErr.Error())
		return streamErr 
	}

	snpService.Log.Debug(ATTEMPTING_COMPACTION, lastIncludedIndex - 1)

	var totBytesRem, totKeysRem int64
	totBytesRem, totKeysRem, streamErr = snpService.CurrentSystem.WAL.DeleteLogsUpToLastIncluded(lastIncludedIndex - 1)
	if streamErr != nil { 
		snpService.Log.Error(COMPACTION_ERROR, streamErr.Error())
		return streamErr 
	}

	snpService.Log.Debug(TOTAL_REMOVED, totBytesRem, totKeysRem)
	snpService.Log.Info(SNAPSHOT_SUCCESS)
	
	streamErr = stream.SendAndClose(&snapshot_proto.SnapshotStreamResponse{ Success: true })
	if streamErr != nil { 
		snpService.Log.Error(STREAM_SEND_ERROR, streamErr.Error())
		return streamErr 
	}

	return nil
}
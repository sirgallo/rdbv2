package wal

import (
	bolt "go.etcd.io/bbolt"
	"google.golang.org/protobuf/proto"
	"github.com/sirgallo/utils"

	"github.com/sirgallo/rdbv2/internal/snapshot/snapshot_proto"
)


//=========================================== WAL Snapshot Ops


//	SetSnapshot:
//		set the snapshot entry for the latest snapshot.
//		the entry contains a reference to the file path where the latest snapshot is stored on disk.
func (wal *WAL) SetSnapshot(snapshot *snapshot_proto.SnapshotEntry) error {
	var setErr error

	transaction := func(tx *bolt.Tx) error {
		var encodedEntry []byte
		var setTxErr error

		bucket := tx.Bucket([]byte(Snapshot))

		encodedEntry, setTxErr = proto.Marshal(snapshot)
		if setTxErr != nil { return setTxErr }

		setTxErr = bucket.Put([]byte(SnapshotKey), encodedEntry)
		if setTxErr != nil { return setTxErr }
		return nil
	}

	setErr = wal.DB.Update(transaction)
	if setErr != nil { return setErr }
	return nil
}

//	GetSnapshot:
//		get the latest snapshot entry to get the path to the latest snapshot.
func (wal *WAL) GetSnapshot() (*snapshot_proto.SnapshotEntry, error) {
	var snapshotEntry *snapshot_proto.SnapshotEntry
	var getErr error
	
	transaction := func(tx *bolt.Tx) error {
		var decodeErr error

		bucket := tx.Bucket([]byte(Snapshot))

		val := bucket.Get([]byte(SnapshotKey))
		if val != nil {
			var decoded snapshot_proto.SnapshotEntry
			decodeErr = proto.Unmarshal(val, &decoded)
			if decodeErr != nil { return decodeErr }
			if &decoded != utils.GetZero[*snapshot_proto.SnapshotEntry]() { snapshotEntry = &decoded }
		}

		return nil
	}

	getErr = wal.DB.View(transaction)
	if getErr != nil { return nil, getErr }
	if snapshotEntry == nil { return nil, nil }

	return snapshotEntry, nil
}
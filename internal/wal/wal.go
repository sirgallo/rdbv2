package wal

import (
	"os"
	"path/filepath"
	bolt "go.etcd.io/bbolt"
	"github.com/sirgallo/logger"
)


var Log = logger.NewCustomLog(NAME)

/*
	Write Ahead Log
		1.) open the db using the filepath 
		2.) create the replog bucket if it does not already exist
		4.) also create both a stats bucket and an index bucket sub bucket
			--> the replog stats bucket contains both the total size of the replicated log and total entries
			--> the index bucket conains the first known entry for each term, which is used to 
					reduce the number of failed AppendEntryRPCs when a node is brought into the cluster
					and being synced back to the leader
		5.) create the snapshot bucket
			--> this contains a reference to the filepath for the most up to date snapshot for the cluster
		6.) create the stats bucket
			--> the stats bucket contains a time series of system stats as the system progresses
*/

func NewWAL () (*WAL, error) {
	var db *bolt.DB
	var homedir string
	var newWALErr error

	homedir, newWALErr = os.UserHomeDir()
	if newWALErr != nil { return nil, newWALErr }

	dbPath := filepath.Join(homedir, SubDirectory, FileName)
	db, newWALErr = bolt.Open(dbPath, 0600, nil)
	if newWALErr != nil { return nil, newWALErr }

	logTransaction := func(tx *bolt.Tx) error {
		var parent *bolt.Bucket
		var createErr error

		parent, createErr = tx.CreateBucketIfNotExists([]byte(ReplicatedLog))
		if createErr != nil { return createErr }

		_, createErr = parent.CreateBucketIfNotExists([]byte(ReplicatedLogWAL))
		if createErr != nil { return createErr }

		_, createErr = parent.CreateBucketIfNotExists([]byte(ReplicatedLogStats))
		if createErr != nil { return createErr }

		_, createErr = parent.CreateBucketIfNotExists([]byte(ReplicatedLogIndex))
		if createErr != nil { return createErr }

		return nil
	}

	newWALErr = db.Update(logTransaction)
	if newWALErr != nil { return nil, newWALErr }

	snapshotTransaction := func(tx *bolt.Tx) error {
		_, createErr := tx.CreateBucketIfNotExists([]byte(Snapshot))
		if createErr != nil { return createErr }
		return nil
	}

	newWALErr = db.Update(snapshotTransaction)
	if newWALErr != nil { return nil, newWALErr }

	sysStatsTransaction := func(tx *bolt.Tx) error {
		_, createErr := tx.CreateBucketIfNotExists([]byte(SystemStats))
		if createErr != nil { return createErr }
		return nil
	}

	newWALErr = db.Update(sysStatsTransaction)
	if newWALErr != nil { return nil, newWALErr }

  return &WAL{ DBFile: dbPath, DB: db }, nil
}
package state

import (
	"io"
	"os"
	"path/filepath"
	bolt "go.etcd.io/bbolt"
	"github.com/sirgallo/utils"
	
	rdbUtils "github.com/sirgallo/rdbv2/internal/utils"
)


//=========================================== Snapshot Utils


/*
	Snapshot State Machine
		1.) generate the name for the snapshot file, which is db name and a uuid 
		2.) open a new file for the snapshot to be written to
		3.) open up a gzip stream to compress the file
		4.) create a bolt transaction to write to the gzip stream
		5.) if successful, return the snapshot path
*/

func (sm *State) SnapshotStat() (string, error) {
	homedir, homeErr := os.UserHomeDir()
	if homeErr != nil { return utils.GetZero[string](), homeErr }

	snapshotFileName, fileNameErr := sm.generateFilename()
	if fileNameErr != nil { return utils.GetZero[string](), fileNameErr }

	snapshotPath := filepath.Join(homedir, SubDirectory, snapshotFileName)
	snapshotFile, fCreateErr := os.Create(snapshotPath)
	if fCreateErr != nil { return utils.GetZero[string](), fCreateErr }
	
	defer snapshotFile.Close()

	transaction := func(tx *bolt.Tx) error {
		_, writeErr := tx.WriteTo(snapshotFile)
		if writeErr != nil { return writeErr }
		return nil
	}

	snapshotErr := sm.DB.View(transaction)
	if snapshotErr != nil { return utils.GetZero[string](), snapshotErr }
	return snapshotPath, nil
}

/*
	Replay Snapshot
		1.) using the filepath for the latest snapshot, open the file
		2.) create a gzip reader
		3.) close the db and remove the original db file
		4.) recreate the same file and read the snapshot
		5.) write the content of the snapshot to the newly created db file
		6.) reopen the db
*/

func (sm *State) ReplaySnapshot(snapshotPath string) error {
	var replayErr error 

	var homedir string
	homedir, replayErr = os.UserHomeDir()
	if replayErr != nil { return replayErr }

	dbPath := filepath.Join(homedir, SubDirectory, DbFileName)

	var snapshotFile *os.File
	snapshotFile, replayErr = os.Open(snapshotPath)
	if replayErr != nil { return replayErr }
	defer snapshotFile.Close()

	replayErr = sm.DB.Close()
	if replayErr != nil { return replayErr }

	replayErr = os.Remove(dbPath)
	if replayErr != nil && ! os.IsNotExist(replayErr) { return replayErr }
	
	var databaseFile *os.File
	databaseFile, replayErr = os.Create(dbPath)
	if replayErr != nil { return replayErr }
	defer databaseFile.Close()

	_, replayErr = io.Copy(databaseFile, snapshotFile)
	if replayErr != nil { return replayErr }

	var db *bolt.DB
	db, replayErr = bolt.Open(dbPath, 0600, nil)
	if replayErr != nil { return replayErr }

	sm.DB = db
	return nil
}

/*
	generate Filename
		--> generate the snapshot name, which is dbname_uniqueID
*/

func (sm *State) generateFilename() (string, error) {
	hash, hashErr := rdbUtils.GenerateRandomSHA256Hash()
	if hashErr != nil { return utils.GetZero[string](), hashErr }

	snapshotName := func() string { return FileNamePrefix + "_" + hash }()
	return snapshotName, nil
}
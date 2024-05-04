package rdbv2

import "github.com/sirgallo/rdbv2/internal/stats"


//=========================================== Raft Utils

/*
	Update RepLog On Startup:
		on system startup or restart replay the WAL
			1.) get the latest log from the WAL on disk
			2.) update commit index to last log index from synced WAL --> WAL only contains committed logs
			3.) update current term to term of last log
*/

func (rdb *RDB) UpdateLogOnStartup() (bool, error) {
	var updateErr error

	snapshotEntry, updateErr := rdb.CurrentSystem.WAL.GetSnapshot()
	if updateErr != nil {
		return false, updateErr 
	} else if snapshotEntry != nil { 
		updateErr = rdb.CurrentSystem.State.ReplaySnapshot(snapshotEntry.SnapshotFilePath) 
		if updateErr != nil { return false, updateErr }
		Log.Info(REPLAY_SNAPSHOT_SUCCESS)
	}

	lastLog, updateErr := rdb.CurrentSystem.WAL.GetLatest()
	if updateErr != nil {
		return false, updateErr
	} else if lastLog != nil {
		rdb.CurrentSystem.CommitIndex = lastLog.Index
		updateErr = rdb.Replication.ApplyLogs()
		if updateErr != nil { return false, updateErr }

		var total int
		total, updateErr = rdb.CurrentSystem.WAL.GetTotal()
		if updateErr != nil { return false , updateErr }
		Log.Info(LOG_ENTRIES_STARTUP, total)
	}

	return true, nil
}

func (rdb *RDB) InitStats() error {
	var initStatsErr error

	var initStatObj *stats.Stats
	initStatObj, initStatsErr = stats.CalculateCurrentStats()
	if initStatsErr != nil {
		Log.Error(CALCULATE_STATS_ERROR, initStatsErr.Error())
		return initStatsErr
	}

	initStatsErr = rdb.CurrentSystem.WAL.SetStat(*initStatObj)
	if initStatsErr != nil {
		Log.Error(SET_STATS_ERROR, initStatsErr.Error())
		return initStatsErr
	}

	return nil
}
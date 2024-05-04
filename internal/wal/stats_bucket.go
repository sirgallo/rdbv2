package wal

import (
	bolt "go.etcd.io/bbolt"

	"github.com/sirgallo/rdbv2/internal/stats"
)


//=========================================== Write Ahead Log Stats Ops


/*
	Set Stat
		set the latest statistic in the time series
			--> keys are iso strings, so keys are stored earliest to latest
*/

func (wal *WAL) SetStat(statObj stats.Stats) error {
	var setErr error

	transaction := func(tx *bolt.Tx) error {
		var value []byte
		var setTxErr error

		bucket := tx.Bucket([]byte(SystemStats))

		value, setTxErr = stats.EncodeStatObjectToBytes(statObj)
		if setTxErr != nil { return setTxErr }

		setTxErr = bucket.Put([]byte(statObj.Timestamp), value)
		if setTxErr != nil { return setTxErr }
		return nil
	}

	setErr = wal.DB.Update(transaction)
	if setErr != nil { return setErr }
	return nil
}

/*
	Get Stats
		Get the current stats in the time series
*/

func (wal *WAL) GetStats() ([]stats.Stats, error) {
	var statsArr []stats.Stats
	var getErr error
	
	transaction := func(tx *bolt.Tx) error {
		bucket := tx.Bucket([]byte(SystemStats))
		cursor := bucket.Cursor()

		for key, val := cursor.First(); key != nil; key, val = cursor.Next() {
			if val != nil {
				statObj, decErr := stats.DecodeBytesToStatObject(val)
				if decErr != nil { return decErr }
				statsArr = append(statsArr, *statObj)
			}
		}

		return nil
	}

	getErr = wal.DB.View(transaction)
	if getErr != nil { return nil, getErr }
	return statsArr, nil
}

/*
	Delete Stats
		stats in the time series are limited to a fixed size, so when the size limit is hit,
		delete the earliest keys up to the limit
*/

func (wal *WAL) DeleteStats() error {	
	var deleteErr error

	transaction := func(tx *bolt.Tx) error {
		bucket := tx.Bucket([]byte(SystemStats))
		cursor := bucket.Cursor()
		keyCount := 0
		
		for key, _ := cursor.First(); key != nil; key, _ = cursor.Next() { keyCount++ }

		numKeysToDelete := keyCount - MAX_STATS
		totalDeleted := 0
		
		for key, _ := cursor.First(); key != nil; key, _ = cursor.Next() {
			if  totalDeleted <= numKeysToDelete {
				bucket.Delete(key)
				totalDeleted++
			}
		}

		return nil
	}

	deleteErr = wal.DB.Update(transaction)
	if deleteErr != nil { return deleteErr }
	return nil
}
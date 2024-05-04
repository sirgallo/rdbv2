package wal

import (
	"bytes"
	bolt "go.etcd.io/bbolt"
	"google.golang.org/protobuf/proto"

	"github.com/sirgallo/rdbv2/internal/replication/replication_proto"
	"github.com/sirgallo/utils"
)

//=========================================== Write Ahead Log ReplicatedLog Bucket Ops

/*
	Append
		create a read-write transaction for the bucket to append a single new entry
			1.) get the current bucket
			2.) transform the entry and key to byte arrays
			3.) put the key and value in the bucket
*/

func (wal *WAL) Append(entry *replication_proto.LogEntry) error {
	transaction := func(tx *bolt.Tx) error {
		var totalBytesAdded, totalKeysAdded int64
		var appendErr error

		bucket := tx.Bucket([]byte(ReplicatedLog))

		latestIndexedLog, getIndexErr := wal.getLatestIndexedEntry(bucket)
		if getIndexErr != nil { return getIndexErr}

		totalBytesAdded, totalKeysAdded, appendErr = wal.appendHelper(bucket, entry)
		if appendErr != nil { return appendErr }

		appendErr = wal.UpdateReplogStats(bucket, totalBytesAdded, totalKeysAdded, ADD)
		if appendErr != nil { return appendErr }

		_, appendErr = wal.setIndexForFirstLogInTerm(bucket, entry, latestIndexedLog)
		if appendErr != nil { return appendErr }

		return nil
	}

	appendErr := wal.DB.Update(transaction)
	if appendErr != nil { return appendErr }
	return nil
}

/*
	Range Append
		create a read-write transaction for the bucket to append a set of new entries
			1.) get the current bucket
			2.) iterate over the new entries and perform the same as single Append
*/

func (wal *WAL) RangeAppend(logs []*replication_proto.LogEntry) error {
	var rangeUpdateErr error

	transaction := func(tx *bolt.Tx) error {
		var newIndexedEntry *replication_proto.LogEntry
		var entrySize, keyToAdd int64
		var appendErr error

		bucket := tx.Bucket([]byte(ReplicatedLog))

		latestIndexedLog, getIndexErr := wal.getLatestIndexedEntry(bucket)
		if getIndexErr != nil { return getIndexErr}

		totalBytesAdded := int64(0)
		totalKeysAdded := int64(0)

		for _, currLog := range logs {
			entrySize, keyToAdd, appendErr = wal.appendHelper(bucket, currLog)
			if appendErr != nil { return appendErr }
			
			totalBytesAdded += entrySize
			totalKeysAdded += keyToAdd

			newIndexedEntry, appendErr = wal.setIndexForFirstLogInTerm(bucket, currLog, latestIndexedLog)
			if appendErr != nil { return appendErr }
			if newIndexedEntry != nil { latestIndexedLog = newIndexedEntry }
		}

		appendErr = wal.UpdateReplogStats(bucket, totalBytesAdded, totalKeysAdded, ADD)
		if appendErr != nil { return appendErr }
		return nil
	}

	rangeUpdateErr = wal.DB.Update(transaction)
	if rangeUpdateErr != nil { return rangeUpdateErr }
	return nil
}

/*
	Read
		create a read transaction for getting a single key-value entry
			1.) get the current bucket
			2.) get the value for the key as bytes
			3.) transform the byte array back to an entry and return
*/

func (wal *WAL) Read(index int64) (*replication_proto.LogEntry, error) {
	var entry *replication_proto.LogEntry
	var readErr error
	
	transaction := func(tx *bolt.Tx) error {
		bucket := tx.Bucket([]byte(ReplicatedLog))
		walBucket := bucket.Bucket([]byte(ReplicatedLogWAL))

		val := walBucket.Get(ConvertIntToBytes(index))
		if val == nil { return nil }

		var decoded replication_proto.LogEntry
		transformErr := proto.Unmarshal(val, &decoded)
		if transformErr != nil { return transformErr }

		entry = &decoded
		return nil
	}

	readErr = wal.DB.View(transaction)
	if readErr != nil { return nil, readErr }
	if entry == nil { return nil, nil }
	
	return entry, nil
}

/*
	Get Range
		create a read transaction for getting a range of entries
			1.) get the current bucket
			2.) create a cursor for the bucket
			3.) seek from the specified start index and iterate until end
			4.) for each value, transform from byte array to entry and append to return array
			5.) return all entries
*/

func (wal *WAL) GetRange(startIndex int64, endIndex int64) ([]*replication_proto.LogEntry, error) {
	var entries []*replication_proto.LogEntry
	var readErr error

	transaction := func(tx *bolt.Tx) error {
		var transformErr error

		transformAndAppend := func(val []byte) error {
			var decoded replication_proto.LogEntry
			transformErr = proto.Unmarshal(val, &decoded)
			if transformErr != nil { return transformErr }
			if &decoded != utils.GetZero[*replication_proto.LogEntry]() { entries = append(entries, &decoded) }

			return nil
		}

		bucket := tx.Bucket([]byte(ReplicatedLog))
		walBucket := bucket.Bucket([]byte(ReplicatedLogWAL))
		cursor := walBucket.Cursor()

		for key, val := cursor.Seek(ConvertIntToBytes(startIndex)); key != nil && bytes.Compare(key, ConvertIntToBytes(endIndex)) <= 0; key, val = cursor.Next() {
			if val != nil { transformAndAppend(val) }
		}

		return nil
	}

	readErr = wal.DB.View(transaction)
	if readErr != nil { return nil, readErr }
	return entries, nil
}

/*
	Get Latest
		create a read transaction for getting the latest entry in the bucket
			1.) get the current bucket
			2.) create a cursor for the bucket and point at the last element in the bucket
			3.) transform the value from byte array to entry and return the entry
*/

func (wal *WAL) GetLatest() (*replication_proto.LogEntry, error) {
	var entry *replication_proto.LogEntry
	var readErr error

	transaction := func(tx *bolt.Tx) error {
		bucket := tx.Bucket([]byte(ReplicatedLog))
		walBucket := bucket.Bucket([]byte(ReplicatedLogWAL))
		cursor := walBucket.Cursor()

		_, val := cursor.Last()
		if val != nil {
			var decoded replication_proto.LogEntry
			transformErr := proto.Unmarshal(val, &decoded)
			if transformErr != nil { return transformErr }
			if &decoded != utils.GetZero[*replication_proto.LogEntry]() { entry = &decoded }
		}

		return nil
	}
	
	readErr = wal.DB.View(transaction)
	if readErr != nil { return nil, readErr }
	if entry == nil { return nil, nil }

	return entry, nil
}

/*
	Get Earliest
		create a read transaction for getting the latest entry in the bucket
			1.) get the current bucket
			2.) create a cursor for the bucket and point at the first element in the bucket
			3.) transform the value from byte array to entry and return the entry
*/

func (wal *WAL) GetEarliest() (*replication_proto.LogEntry, error) {
	var entry *replication_proto.LogEntry
	var readErr error

	transaction := func(tx *bolt.Tx) error {
		bucket := tx.Bucket([]byte(ReplicatedLog))
		walBucket := bucket.Bucket([]byte(ReplicatedLogWAL))
		cursor := walBucket.Cursor()

		_, val := cursor.First()
		if val != nil { 
			var decoded replication_proto.LogEntry
			transformErr := proto.Unmarshal(val, &decoded)
			if transformErr != nil { return transformErr }
			if &decoded != utils.GetZero[*replication_proto.LogEntry]() { entry = &decoded }
		}

		return nil
	}
	
	readErr = wal.DB.View(transaction)
	if readErr != nil { return nil, readErr }
	if entry == nil { return nil, nil }

	return entry, nil
}

/*
	Delete Logs
		create a read-write transaction for log compaction in the bucket
			1.) for all logs up tothe last index, delete the key-value pair
			2.) for each deleted, update the total keys and the total space removed from the log
			3.) get latest log (last applied), and remove all indexes up to it
*/

func (wal *WAL) DeleteLogsUpToLastIncluded(endIndex int64) (int64, int64, error) {
	var currentChunkEndIndex, subTotalBytes, subKeys int64
	var delErr, subErr error

	totalBytesRemoved := int64(0)
	totalKeysRemoved := int64(0)
	chunkSize := 500

	for startIndex := int64(0); startIndex <= endIndex; startIndex += int64(chunkSize) {
		currentChunkEndIndex = startIndex + int64(chunkSize)
		if currentChunkEndIndex > endIndex { currentChunkEndIndex = endIndex }

		transaction := func(tx *bolt.Tx) error {
			bucket := tx.Bucket([]byte(ReplicatedLog))
			subTotalBytes, subKeys, subErr = wal.deleteLogsHelper(bucket, startIndex, currentChunkEndIndex)
			if subErr != nil { return subErr }

			totalBytesRemoved += subTotalBytes
			totalKeysRemoved += subKeys
			return nil
		}

		delErr = wal.DB.Update(transaction)
		if delErr != nil { return totalBytesRemoved, totalKeysRemoved, delErr }
	}

	transaction := func(tx *bolt.Tx) error {
		bucket := tx.Bucket([]byte(ReplicatedLog))
		
		updateErr := wal.UpdateReplogStats(bucket, totalBytesRemoved, totalKeysRemoved, SUB)
		if updateErr != nil { return updateErr }
		return nil
	}

	delErr = wal.DB.Update(transaction) // update statistics
	if delErr != nil { return totalBytesRemoved, totalKeysRemoved, delErr }
	return totalBytesRemoved, totalKeysRemoved, nil
}

func (wal *WAL) RangeDelete(startIndex, endIndex int64) (int64, int64, error) {
	var currentChunkEndIndex, subTotalBytes, subKeys int64
	var rangeDeleteErr, subErr error

	totalBytesRemoved := int64(0)
	totalKeysRemoved := int64(0)
	chunkSize := 500

	for startIndex := int64(startIndex); startIndex <= endIndex; startIndex += int64(chunkSize) {
		currentChunkEndIndex = startIndex + int64(chunkSize)
		if currentChunkEndIndex > endIndex { currentChunkEndIndex = endIndex }

		transaction := func(tx *bolt.Tx) error {
			bucket := tx.Bucket([]byte(ReplicatedLog))
			subTotalBytes, subKeys, subErr = wal.deleteLogsHelper(bucket, startIndex, currentChunkEndIndex)
			if subErr != nil { return subErr }

			totalBytesRemoved += subTotalBytes
			totalKeysRemoved += subKeys
			return nil
		}

		rangeDeleteErr = wal.DB.Update(transaction)
		if rangeDeleteErr != nil { return totalBytesRemoved, totalKeysRemoved, rangeDeleteErr }
	}

	transaction := func(tx *bolt.Tx) error {
		bucket := tx.Bucket([]byte(ReplicatedLog))
		
		updateErr := wal.UpdateReplogStats(bucket, totalBytesRemoved, totalKeysRemoved, SUB)
		if updateErr != nil { return updateErr }
		return nil
	}

	rangeDeleteErr = wal.DB.Update(transaction) // update statistics
	if rangeDeleteErr != nil { return totalBytesRemoved, totalKeysRemoved, rangeDeleteErr }
	return totalBytesRemoved, totalKeysRemoved, nil
} 

/*
	Get Total
		create a read transaction for getting total keys in the bucket
			1.) read from the stats bucket and check the indexed total value
*/

func (wal *WAL) GetTotal() (int, error) {
	var readErr error
	totalKeys := 0

	transaction := func(tx *bolt.Tx) error {
		bucket := tx.Bucket([]byte(ReplicatedLog))
		statsBucket := bucket.Bucket([]byte(ReplicatedLogStats))
		
		val := statsBucket.Get([]byte(ReplogTotalElementsKey))
		if val != nil { totalKeys = int(ConvertBytesToInt(val)) }
		return nil
	}

	readErr = wal.DB.View(transaction)
	if readErr != nil { return 0, readErr }
	return totalKeys, nil
}

/*
	Get Bucket Size In Bytes
		create a read transaction for getting total size of the replicated log in bytes
			1.) read from the stats bucket and check the indexed total size
*/

func (wal *WAL) GetBucketSizeInBytes() (int64, error) {
	var getSizeErr error
	totalSize := int64(0)

	transaction := func(tx *bolt.Tx) error {
		bucket := tx.Bucket([]byte(ReplicatedLog))
		statsBucket := bucket.Bucket([]byte(ReplicatedLogStats))

		val := statsBucket.Get([]byte(ReplogSizeKey))
		if val != nil { totalSize = ConvertBytesToInt(val) }
		return nil
	}

	getSizeErr = wal.DB.View(transaction)
	if getSizeErr != nil { return 0, getSizeErr }
	return totalSize, nil
}

/*
	Update ReplicatedLog Stats
		helper function for updating both the indexes for total keys and total size of the replicated log
*/

func (wal *WAL) UpdateReplogStats(bucket *bolt.Bucket, numUpdatedBytes int64, numUpdatedKeys int64, op StatOP) error {
	var bucketSize, totalKeys, newBucketSize, newTotal int64
	var currStat []byte
	var putErr error

	statsBucket := bucket.Bucket([]byte(ReplicatedLogStats))
	sizeKey := []byte(ReplogSizeKey)
	totalKey := []byte(ReplogTotalElementsKey)
	
	currStat = statsBucket.Get(sizeKey) // size
	if currStat == nil {
		bucketSize = 0
	} else { bucketSize = ConvertBytesToInt(currStat) }

	currStat = statsBucket.Get(totalKey) // total entries
	if currStat == nil {
		totalKeys = 0
	} else { totalKeys = ConvertBytesToInt(currStat) }

	if op == ADD {
		newBucketSize = bucketSize + numUpdatedBytes
		newTotal = totalKeys + numUpdatedKeys
	} else if op == SUB {
		newBucketSize = bucketSize - numUpdatedBytes
		newTotal = totalKeys - numUpdatedKeys
	}

	putErr = statsBucket.Put(sizeKey, ConvertIntToBytes(newBucketSize))
	if putErr != nil { return putErr }

	putErr = statsBucket.Put(totalKey, ConvertIntToBytes(newTotal))
	if putErr != nil { return putErr }
	return nil
}

/*
	Get Indexed Entry For Term
		For the given term, check the indexed value and to get the earliest known entry
*/

func (wal *WAL) GetIndexedEntryForTerm(term int64) (*replication_proto.LogEntry, error) {
	var entry *replication_proto.LogEntry
	var getIndexErr error

	transaction := func(tx *bolt.Tx) error {
		bucket := tx.Bucket([]byte(ReplicatedLog))
		indexBucket := bucket.Bucket([]byte(ReplicatedLogIndex))
		
		val := indexBucket.Get(ConvertIntToBytes(term))
		if val != nil { 
			var decoded replication_proto.LogEntry
			transformErr := proto.Unmarshal(val, &decoded)
			if transformErr != nil { return transformErr }
			if &decoded != utils.GetZero[*replication_proto.LogEntry]() { entry = &decoded }
		}

		return nil
	}

	getIndexErr = wal.DB.View(transaction)
	if getIndexErr != nil { return nil, getIndexErr }
	if entry == nil { return nil, nil }

	return entry, nil
}

/*
	Append Helper
		shared function for appending entries to the replicated log
*/

func (wal *WAL) appendHelper(bucket *bolt.Bucket, entry *replication_proto.LogEntry) (int64, int64, error) {
	var value []byte
	var appendErr error

	walBucket := bucket.Bucket([]byte(ReplicatedLogWAL))
	key := ConvertIntToBytes(entry.Index)
	value, appendErr = proto.Marshal(entry)
	if appendErr != nil { return 0, 0, appendErr }

	appendErr = walBucket.Put(key, value)
	if appendErr != nil { return 0, 0, appendErr }
	return int64(len(key)), int64(len(value)), nil // total bytes, total keys, no err
}

func (wal *WAL) deleteLogsHelper(bucket *bolt.Bucket, startIndex, endIndex int64) (int64, int64, error){
	var entry replication_proto.LogEntry
	var firstTermInBatch int64
	var delErr, transformErr error

	totalBytesRemoved := int64(0)
	totalKeysRemoved := int64(0)

	walBucket := bucket.Bucket([]byte(ReplicatedLogWAL))
	startKey := ConvertIntToBytes(startIndex)
	
	firstEntryToBeDeleted := walBucket.Get(startKey)
	if firstEntryToBeDeleted != nil {
		transformErr = proto.Unmarshal(firstEntryToBeDeleted, &entry)
		if transformErr != nil { return totalBytesRemoved, totalKeysRemoved, transformErr }
		if &entry == utils.GetZero[*replication_proto.LogEntry]() { return 0, 0, nil }
		
		firstTermInBatch = entry.Term
	}

	cursor := walBucket.Cursor()

	for key, val := cursor.Seek(startKey); key != nil && bytes.Compare(key, ConvertIntToBytes(endIndex)) <= 0; key, val = cursor.Next() {
		delErr = bucket.Delete(key)
		if delErr != nil { return totalBytesRemoved, totalKeysRemoved, delErr }

		totalBytesRemoved += int64(len(key)) + int64(len(val))
		totalKeysRemoved++
	}

	keyOfNextLog := ConvertIntToBytes(endIndex + 1)
	val := walBucket.Get(keyOfNextLog)
	if val != nil {
		transformErr = proto.Unmarshal(val, &entry)
		if transformErr != nil { return totalBytesRemoved, totalKeysRemoved, transformErr }
		if &entry == utils.GetZero[*replication_proto.LogEntry]() { return 0, 0, nil }

		indexBucket := bucket.Bucket([]byte(ReplicatedLogIndex))
		indexCursor := indexBucket.Cursor()

		for key, _ := indexCursor.Seek(ConvertIntToBytes(firstTermInBatch)); key != nil && bytes.Compare(key, ConvertIntToBytes(entry.Term)) < 0; key, _ = indexCursor.Next() {
			delErr = indexBucket.Delete(key)
			if delErr != nil { return totalBytesRemoved, totalKeysRemoved, delErr }
		}
	} 

	return totalBytesRemoved, totalKeysRemoved, nil
}

/*
	Get Latest Indexed Entry
		get the earliest known entry for the latest term known in the cluster
*/

func (wal *WAL) getLatestIndexedEntry(bucket *bolt.Bucket) (*replication_proto.LogEntry, error) {
	var entry replication_proto.LogEntry
	var transformErr error

	indexBucket := bucket.Bucket([]byte(ReplicatedLogIndex))
	cursor := indexBucket.Cursor()

	_, val := cursor.Last()
	if val == nil { return &replication_proto.LogEntry{ Index: 0, Term: 0 }, nil }

	transformErr = proto.Unmarshal(val, &entry)
	if transformErr != nil { return nil, transformErr }
	if &entry == utils.GetZero[*replication_proto.LogEntry]() { return nil, nil }

	return &entry, nil
}

/*
	When a higher term than previously known is discovered, update the index to include the first entry associated with term
*/

func (wal *WAL) setIndexForFirstLogInTerm(
	bucket *bolt.Bucket,
	newEntry *replication_proto.LogEntry,
	previousIndexed *replication_proto.LogEntry,
) (*replication_proto.LogEntry, error) {
	if newEntry.Term > previousIndexed.Term {
		var entryAsBytes []byte
		var setErr error

		indexBucket := bucket.Bucket([]byte(ReplicatedLogIndex))
		entryAsBytes, setErr = proto.Marshal(newEntry)
		if setErr != nil { return nil, setErr }
		
		setErr = indexBucket.Put(ConvertIntToBytes(newEntry.Term), entryAsBytes)
		if setErr != nil { return nil, setErr }
		return newEntry, nil
	}

	return nil, nil
}
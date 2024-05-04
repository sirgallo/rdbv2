package state

import (
	"strings"
	bolt "go.etcd.io/bbolt"
	"github.com/sirgallo/utils"
	
	"github.com/sirgallo/rdbv2/internal/replication/replication_proto"
	rdbUtils "github.com/sirgallo/rdbv2/internal/utils"
)


//=========================================== State Machine Operations


//	BulkWrite:
//		operation to apply logs to the state machine and perform operations on it.
//		this is the process of applying log entries from the replicated log performs the operation on the state machine, which will also return a response back to the client that issued the command included in the log entry
//
//		the operation is a struct which contains both the operation to perform and the payload included:
//			--> the payload includes the collection to be operated on as well as the value to update
//
//		PUT:
//			perform an insert for a value in a collection:
//				on inserts, first a hash is generated as the key for the value in the collection. 
//				then, values are inserted into appropriate indexes. 
//				since BoltDb utilizes a B+ tree as its primary data structure, key-value pairs are sorts by default. 
//				RDB utilizes this to create indexes for our collections, where values become the primary key and the value becomes the id of the object in the collection.
//				essentially	can point directly to the location in the collection from a given index
//		DELETE:
//			perform a delete for a value in a collection
//			this involes first doing a lookup on the index for the object to be deleted, and then removing both the original element from the collection and all associated indexes.
//

//		DROP COLLECTION:
//			perform a collection drop:
//				pass the collection to be dropped, and it will be removed from the root database bucket.
//				all associated indexes are removed and the reference to the names of the collection and indexes are removed from the collection and index buckets in root.
func (sm *State) BulkWrite(ops []*replication_proto.LogEntry) ([]*StateResponse, error) {
	responses := []*StateResponse{}

	transaction := func(tx *bolt.Tx) error {
		root := tx.Bucket([]byte(RootBucket))

		for _, op := range ops {
			_, createCollectionErr := sm.createCollection(root, op.Command.Payload.Collection)
			if createCollectionErr != nil { return createCollectionErr }

			switch string(op.Command.Action) {
			case PUT:
				putResp, insertErr := sm.insertIntoCollection(root, op.Command.Payload)
				if insertErr != nil { return insertErr}

				putResp.RequestId = string(op.Command.RequestId)
				responses = append(responses, putResp)
			case DELETE:
				deleteResp, deleteErr := sm.deleteFromCollection(root, op.Command.Payload)
				if deleteErr != nil { return deleteErr }

				deleteResp.RequestId = string(op.Command.RequestId)
				responses = append(responses, deleteResp)
			case DROPCOLLECTION:
				dropResp, dropErr := sm.dropCollection(root, op.Command.Payload)
				if dropErr != nil { return dropErr }

				dropResp.RequestId = string(op.Command.RequestId)
				responses = append(responses, dropResp)
			}
		}

		return nil
	}

	bulkInsertErr := sm.DB.Update(transaction)
	if bulkInsertErr != nil { return nil, bulkInsertErr }

	return responses, nil
}

//	Read:
//		read only operations that do not mutate state.
//
//		GET:
//			perform a lookup on a value:
//			the key does not need to be known, and the value to look for is passed in the payloadhe value is indexed in a separate collection, which points to the key that is associated with the value.
//			keys are dynamically generated on inserts and do not need be known to the user. 
//			the key can be seen more as a unique identifier.
//
//		LIST COLLECTIONS:
//			get all available collections on the state machine:
//				do a lookup on the collection bucket and get all collections names.
func (sm *State) Read(op *StateOperation) (*StateResponse, error) {
	var response *StateResponse

	transaction := func(tx *bolt.Tx) error {
		rootName := []byte(RootBucket)
		root := tx.Bucket(rootName)

		switch op.Action {
		case GET:
			searchResp, searchErr := sm.searchInCollection(root, &replication_proto.CommandPayload{
				Collection: []byte(op.Payload.Collection),
				Value: []byte(op.Payload.Value),
			})

			if searchErr != nil { return searchErr }

			searchResp.RequestId = string(op.RequestId)
			response = searchResp
		case LISTCOLLECTIONS:
			listResp, listErr := sm.listCollections(root, &replication_proto.CommandPayload{
				Collection: []byte(op.Payload.Collection),
				Value: []byte(op.Payload.Value),
			})
			
			if listErr != nil { return listErr }

			listResp.RequestId = string(op.RequestId)
			response = listResp
		}

		return nil
	}

	readErr := sm.DB.View(transaction)
	if readErr != nil { return nil, readErr }

	return response, nil
}


//all functions below are helper functions for each of the above state machine operations


func (sm *State) listCollections(bucket *bolt.Bucket, payload *replication_proto.CommandPayload) (*StateResponse, error) {
	var collections []string

	collectionBucket := bucket.Bucket([]byte(CollectionBucket))
	cursor := collectionBucket.Cursor()

	for key, val := cursor.First(); key != nil; key, val = cursor.Next() {
		collections = append(collections, string(val))
	}

	return &StateResponse{ Value: strings.Join(collections, ", ") }, nil
}

func (sm *State) insertIntoCollection(bucket *bolt.Bucket, payload *replication_proto.CommandPayload) (*StateResponse, error) {
	collection := bucket.Bucket(payload.Collection)

	searchIndexResp, searchErr := sm.searchInIndex(bucket, payload)
	if searchErr != nil { return nil, searchErr }
	if searchIndexResp.Value != utils.GetZero[string]() { return searchIndexResp, nil }

	hash, hashErr := rdbUtils.GenerateRandomSHA256Hash()
	if hashErr != nil { return nil, hashErr }

	generatedKey := []byte(hash)
	putErr := collection.Put(generatedKey, payload.Value)
	if putErr != nil { return nil, putErr }

	insertIndexResp, insertErr := sm.insertIntoIndex(bucket, payload, generatedKey)
	if insertErr != nil { return nil, insertErr }

	return insertIndexResp, nil
}

func (sm *State) searchInCollection(bucket *bolt.Bucket, payload *replication_proto.CommandPayload) (*StateResponse, error) {
	indexResp, searchErr := sm.searchInIndex(bucket, payload)
	if searchErr != nil { return nil, searchErr }

	return indexResp, nil
}

func (sm *State) deleteFromCollection(bucket *bolt.Bucket, payload *replication_proto.CommandPayload) (*StateResponse, error) {
	collection := bucket.Bucket(payload.Collection)

	indexResp, searchErr := sm.searchInIndex(bucket, payload)
	if searchErr != nil { return &StateResponse{ Collection: string(payload.Collection) }, searchErr }

	delErr := collection.Delete([]byte(indexResp.Key))
	if delErr != nil { return nil, delErr }

	indexDelResp, indexDelErr := sm.deleteFromIndex(bucket, payload)
	if indexDelErr != nil { return nil, indexDelErr }

	return indexDelResp, nil
}

func (sm *State) dropCollection(bucket *bolt.Bucket, payload *replication_proto.CommandPayload) (*StateResponse, error) {
	delColErr := bucket.DeleteBucket(payload.Collection)
	if delColErr != nil { return nil, delColErr }

	delIndexErr := bucket.DeleteBucket(append(payload.Collection, []byte(IndexSuffix)...))
	if delIndexErr != nil { return nil, delIndexErr }

	collectionBucket := bucket.Bucket([]byte(CollectionBucket))
	indexBucket := bucket.Bucket([]byte(IndexBucket))

	delFromColBucketErr := collectionBucket.Delete(payload.Collection)
	if delFromColBucketErr != nil { return nil, delFromColBucketErr }

	delFromIndexBucketErr := indexBucket.Delete([]byte(IndexBucket))
	if delFromIndexBucketErr != nil { return nil, delFromIndexBucketErr }

	return &StateResponse{ Collection: string(payload.Collection), Value: "dropped" }, nil
}

func (sm *State) searchInIndex(bucket *bolt.Bucket, payload *replication_proto.CommandPayload) (*StateResponse, error) {
	index := bucket.Bucket(append(payload.Collection, []byte(IndexSuffix)...))
	if index == nil { return &StateResponse{ Collection: string(payload.Collection) }, nil }

	val := index.Get([]byte(payload.Value))
	if val == nil { return &StateResponse{ Collection: string(payload.Collection) }, nil }
	return &StateResponse{ Collection: string(payload.Collection), Key: string(val), Value: string(payload.Value) }, nil
}

func (sm *State) insertIntoIndex(bucket *bolt.Bucket, payload *replication_proto.CommandPayload, colKey []byte) (*StateResponse, error) {
	index := bucket.Bucket(append(payload.Collection, []byte(IndexSuffix)...))
	if index == nil { return &StateResponse{ Collection: string(payload.Collection) }, nil }

	putErr := index.Put([]byte(payload.Value), colKey)
	if putErr != nil { return nil, putErr }
	return &StateResponse{ Collection: string(payload.Collection), Key: string(colKey), Value: string(payload.Value) }, nil
}

func (sm *State) deleteFromIndex(bucket *bolt.Bucket, payload *replication_proto.CommandPayload) (*StateResponse, error) {
	index := bucket.Bucket(append(payload.Collection, []byte(IndexSuffix)...))
	if index == nil { return &StateResponse{ Collection: string(payload.Collection) }, nil }

	indexKey := []byte(payload.Value)
	val := index.Get([]byte(payload.Value))
	if val == nil { return &StateResponse{ Collection: string(payload.Collection) }, nil }

	delErr := index.Delete(indexKey)
	if delErr != nil { return nil, delErr }
	return &StateResponse{ Collection: string(payload.Collection), Key: string(val), Value: string(payload.Value) }, nil
}

func (sm *State) createCollection(bucket *bolt.Bucket, collection []byte) (bool, error) {
	collectionBucket := bucket.Bucket(collection)
	
	if collectionBucket == nil {
		_, createErr := bucket.CreateBucketIfNotExists(collection)
		if createErr != nil { return false, createErr }
	
		indexName, createIndexErr := sm.createIndex(bucket, collection)
		if createIndexErr != nil { return false, createIndexErr }
	
		collectionBucket := bucket.Bucket([]byte(CollectionBucket))
		putCollectionErr := collectionBucket.Put(collection, collection)
		if putCollectionErr != nil { return false, putCollectionErr }
	
		indexBucket := bucket.Bucket([]byte(IndexBucket))
		putIndexErr := indexBucket.Put(indexName, indexName)
		if putIndexErr != nil { return false, putIndexErr }
	}

	return true, nil 
}

func (sm *State) createIndex(bucket *bolt.Bucket, collection []byte) ([]byte, error) {
	indexName := append(collection, []byte(IndexSuffix)...)
	_, createErr := bucket.CreateBucketIfNotExists(indexName)
	if createErr != nil { return nil, createErr }

	return indexName, nil 
}
package state

import (
	"strings"
	bolt "go.etcd.io/bbolt"
	"github.com/sirgallo/utils"
	
	rdbUtils "github.com/sirgallo/rdbv2/internal/utils"
)


//=========================================== State Machine Operations


/*
	Bulk Write:
		operation to apply logs to the state machine and perform operations on it
			--> the process of applying log entries from the replicated log performs the operation on the state machine,
				which will also return a response back to the client that issued the command included in the log entry
			
		The operation is a struct which contains both the operation to perform and the payload included
			--> the payload includes the collection to be operated on as well as the value to update

		PUT
			perform an insert for a value in a collection
			--> on inserts, first a hash is generated as the key for the value in the collection. Then, values are inserted into appropriate
				indexes. Since BoltDb utilizes a B+ tree as its primary data structure, key-value pairs are sorts by default. We can utilize this to
				create indexes for our collections, where values become the primary key and the value becomes the id of the object in the collection,
				so essentially we can point directly to the location in the collection from a given index
		
		DELETE
				perform a delete for a value in a collection
				--> this involes first doing a lookup on the index for the object to be deleted, and then removing both the original element from the
				collection and all associated indexes

		DROP COLLECTION
			perform a collection drop
			--> pass the collection to be dropped, and it will be removed from the root database bucket. All associated indexes are removed and 
			the reference to the names of the collection and indexes are removed from the collection and index buckets in root

	Read:
		read only operations that do not mutate state.

		GET
			perform a lookup on a value. The key does not need to be known, and the value to look for is passed in the payload
			--> the value is indexed in a separate collection, which points to the key that is associated with the value. Keys are dynamically 
					generated on inserts and do not need be known to the user. The key can be seen more as a unique identifier
		
		LIST COLLECTIONS
			get all available collections on the state machine
			--> do a lookup on the collection bucket and get all collections names
*/

func (sm *State) BulkWrite(ops []*StateOperation) ([]*StateResponse, error) {
	responses := []*StateResponse{}

	transaction := func(tx *bolt.Tx) error {
		root := tx.Bucket([]byte(RootBucket))

		for _, op := range ops {
			_, createCollectionErr := sm.createCollection(root, op.Payload.Collection)
			if createCollectionErr != nil { return createCollectionErr }

			switch op.Action {
			case PUT:
				putResp, insertErr := sm.insertIntoCollection(root, &op.Payload)
				if insertErr != nil { return insertErr}

				putResp.RequestID = op.RequestID
				responses = append(responses, putResp)
			case DELETE:
				deleteResp, deleteErr := sm.deleteFromCollection(root, &op.Payload)
				if deleteErr != nil { return deleteErr }

				deleteResp.RequestID = op.RequestID
				responses = append(responses, deleteResp)
			case DROPCOLLECTION:
				dropResp, dropErr := sm.dropCollection(root, &op.Payload)
				if dropErr != nil { return dropErr }

				dropResp.RequestID = op.RequestID
				responses = append(responses, dropResp)
			}
		}

		return nil
	}

	bulkInsertErr := sm.DB.Update(transaction)
	if bulkInsertErr != nil { return nil, bulkInsertErr }

	return responses, nil
}

func (sm *State) Read(op *StateOperation) (*StateResponse, error) {
	var response *StateResponse

	transaction := func(tx *bolt.Tx) error {
		rootName := []byte(RootBucket)
		root := tx.Bucket(rootName)

		switch op.Action {
		case GET:
			searchResp, searchErr := sm.searchInCollection(root, &op.Payload)
			if searchErr != nil { return searchErr }

			searchResp.RequestID = op.RequestID
			response = searchResp
		case LISTCOLLECTIONS:
			listResp, listErr := sm.listCollections(root, &op.Payload)
			if listErr != nil { return listErr }

			listResp.RequestID = op.RequestID
			response = listResp
		}

		return nil
	}

	readErr := sm.DB.View(transaction)
	if readErr != nil { return nil, readErr }

	return response, nil
}

/*
	All functions below are helper functions for each of the above state machine operations
*/

func (sm *State) listCollections(bucket *bolt.Bucket, payload *StateOperationPayload) (*StateResponse, error) {
	var collections []string

	collectionBucketName := []byte(CollectionBucket)
	collectionBucket := bucket.Bucket(collectionBucketName)
	cursor := collectionBucket.Cursor()

	for key, val := cursor.First(); key != nil; key, val = cursor.Next() {
		collections = append(collections, string(val))
	}

	return &StateResponse{ Value: strings.Join(collections, ", ") }, nil
}

func (sm *State) insertIntoCollection(bucket *bolt.Bucket, payload *StateOperationPayload) (*StateResponse, error) {
	collectionName := []byte(payload.Collection)
	collection := bucket.Bucket(collectionName)

	searchIndexResp, searchErr := sm.searchInIndex(bucket, payload)
	if searchErr != nil { return nil, searchErr }
	if searchIndexResp.Value != utils.GetZero[string]() { return searchIndexResp, nil }

	hash, hashErr := rdbUtils.GenerateRandomSHA256Hash()
	if hashErr != nil { return nil, hashErr }

	generatedKey := []byte(hash)
	value := []byte(payload.Value)

	putErr := collection.Put(generatedKey, value)
	if putErr != nil { return nil, putErr }

	insertIndexResp, insertErr := sm.insertIntoIndex(bucket, payload, generatedKey)
	if insertErr != nil { return nil, insertErr }

	return insertIndexResp, nil
}

func (sm *State) searchInCollection(bucket *bolt.Bucket, payload *StateOperationPayload) (*StateResponse, error) {
	indexResp, searchErr := sm.searchInIndex(bucket, payload)
	if searchErr != nil { return nil, searchErr }

	return indexResp, nil
}

func (sm *State) deleteFromCollection(bucket *bolt.Bucket, payload *StateOperationPayload) (*StateResponse, error) {
	collectionName := []byte(payload.Collection)
	collection := bucket.Bucket(collectionName)

	indexResp, searchErr := sm.searchInIndex(bucket, payload)
	if searchErr != nil { return &StateResponse{ Collection: payload.Collection }, searchErr }

	delErr := collection.Delete([]byte(indexResp.Key))
	if delErr != nil { return nil, delErr }

	indexDelResp, indexDelErr := sm.deleteFromIndex(bucket, payload)
	if indexDelErr != nil { return nil, indexDelErr }

	return indexDelResp, nil
}

func (sm *State) dropCollection(bucket *bolt.Bucket, payload *StateOperationPayload) (*StateResponse, error) {
	collectionName := []byte(payload.Collection)

	delColErr := bucket.DeleteBucket(collectionName)
	if delColErr != nil { return nil, delColErr }

	delIndexErr := bucket.DeleteBucket([]byte(payload.Collection + IndexSuffix))
	if delIndexErr != nil { return nil, delIndexErr }

	collectionBucketName := []byte(CollectionBucket)
	collectionBucket := bucket.Bucket(collectionBucketName)
	
	indexBucketName := []byte(IndexBucket)
	indexBucket := bucket.Bucket(indexBucketName)

	delFromColBucketErr := collectionBucket.Delete(collectionName)
	if delFromColBucketErr != nil { return nil, delFromColBucketErr }

	delFromIndexBucketErr := indexBucket.Delete(indexBucketName)
	if delFromIndexBucketErr != nil { return nil, delFromIndexBucketErr }

	return &StateResponse{ Collection: payload.Collection, Value: "dropped" }, nil
}

func (sm *State) searchInIndex(bucket *bolt.Bucket, payload *StateOperationPayload) (*StateResponse, error) {
	index := bucket.Bucket([]byte(payload.Collection + IndexSuffix))
	if index == nil { return &StateResponse{ Collection: payload.Collection }, nil }

	val := index.Get([]byte(payload.Value))
	if val == nil { return &StateResponse{ Collection: payload.Collection }, nil }
	return &StateResponse{ Collection: payload.Collection, Key: string(val), Value: payload.Value }, nil
}

func (sm *State) insertIntoIndex(bucket *bolt.Bucket, payload *StateOperationPayload, colKey []byte) (*StateResponse, error) {
	index := bucket.Bucket([]byte(payload.Collection + IndexSuffix))
	if index == nil { return &StateResponse{ Collection: payload.Collection }, nil }

	putErr := index.Put([]byte(payload.Value), colKey)
	if putErr != nil { return nil, putErr }
	return &StateResponse{ Collection: payload.Collection, Key: string(colKey), Value: payload.Value }, nil
}

func (sm *State) deleteFromIndex(bucket *bolt.Bucket, payload *StateOperationPayload) (*StateResponse, error) {
	index := bucket.Bucket([]byte(payload.Collection + IndexSuffix))
	if index == nil { return &StateResponse{ Collection: payload.Collection }, nil }

	indexKey := []byte(payload.Value)
	val := index.Get([]byte(payload.Value))
	if val == nil { return &StateResponse{ Collection: payload.Collection }, nil }

	delErr := index.Delete(indexKey)
	if delErr != nil { return nil, delErr }
	return &StateResponse{ Collection: payload.Collection, Key: string(val), Value: payload.Value }, nil
}

func (sm *State) createCollection(bucket *bolt.Bucket, collection string) (bool, error) {
	collectionName := []byte(collection)
	collectionBucket := bucket.Bucket([]byte(collection))
	
	if collectionBucket == nil {
		_, createErr := bucket.CreateBucketIfNotExists(collectionName)
		if createErr != nil { return false, createErr }
	
		indexName, createIndexErr := sm.createIndex(bucket, collection)
		if createIndexErr != nil { return false, createIndexErr }
	
		collectionBucket := bucket.Bucket([]byte(CollectionBucket))
		putCollectionErr := collectionBucket.Put(collectionName, collectionName)
		if putCollectionErr != nil { return false, putCollectionErr }
	
		indexBucket := bucket.Bucket([]byte(IndexBucket))
		putIndexErr := indexBucket.Put(indexName, indexName)
		if putIndexErr != nil { return false, putIndexErr }
	}

	return true, nil 
}

func (sm *State) createIndex(bucket *bolt.Bucket, collection string) ([]byte, error) {
	indexName := []byte(collection + IndexSuffix)
	_, createErr := bucket.CreateBucketIfNotExists(indexName)
	if createErr != nil { return nil, createErr }

	return indexName, nil 
}
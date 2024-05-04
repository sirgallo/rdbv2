package state

import (
	"sync"
	bolt "go.etcd.io/bbolt"
)


type Action = string

type StateOperationPayload struct {
	Collection string `json:"collection"`
	Value	string `json:"value"`
}

type StateOperation struct {
	RequestId string `json:"-"`
	Action Action `json:"action"`
	Payload StateOperationPayload `json:"payload"`
}

type StateResponse struct {
	RequestId string `json:"-"`
	Collection string `json:"collection"`
	Key string `json:"key"`
	Value string `json:"value"`
}

type State struct {
	Mutex sync.Mutex
	DBFile string
	DB *bolt.DB
}


const NAME = "State"
const SubDirectory = "rdb/state"
const FileNamePrefix = "state"
const DbFileName = FileNamePrefix + ".db"

const (
	CREATECOLLECTION Action = "create collection"
	DROPCOLLECTION Action = "drop collection"
	LISTCOLLECTIONS Action = "list collections"

	PUT Action = "put"
	GET Action = "get"
	DELETE Action = "delete"
	RANGE Action = "range"
)

const RootBucket = "root"
const CollectionBucket = "collection"
const IndexBucket = "index"

const IndexSuffix = "_index"
package pool

import "sync"


type PoolOpts struct {
	MaxConn int
}

type Pool struct {
	connections sync.Map
	maxConn int
}

type PoolError string


const (
	MAX_CONNECTIONS string = "max connections met"
)
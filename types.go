package rdbv2

import (
	"sync"

	"github.com/sirgallo/rdbv2/internal/campaign"
	"github.com/sirgallo/rdbv2/internal/pool"
	"github.com/sirgallo/rdbv2/internal/replication"
	"github.com/sirgallo/rdbv2/internal/request"
	"github.com/sirgallo/rdbv2/internal/snapshot"
	"github.com/sirgallo/rdbv2/internal/system"
)


type RDBPortOpts struct {
	Campaign int
	Replication int
	Snapshot int
	Request int
}

type RDBOpts struct {
	Protocol string
	Ports RDBPortOpts
	SystemsList []*system.System
	PoolOpts pool.PoolOpts
}

type RDB struct {
	Protocol string
	Ports RDBPortOpts
	CurrentSystem *system.System
	Systems *sync.Map

	Campaign *campaign.CampaignService
	Replication *replication.ReplicationService
	Snapshot *snapshot.SnapshotService
	Request *request.RequestService
}

type rdbInfo string
type rdbError string


const NAME = "Raft"

const (
	DefaultCommitIndex = -1
	DefaultLastApplied = -1
	CommandChannelBuffSize = 100000
)

const (
	LOG_ENTRIES_STARTUP rdbInfo = "total entries on startup"
	REPLAY_SNAPSHOT_SUCCESS rdbInfo = "latest snapshot found and replayed successfully"
)

const (
	CALCULATE_STATS_ERROR rdbError = "unable to get calculate stats for path"
	LISTENING_ERROR rdbError = "service failed to listen"
	SET_STATS_ERROR rdbError = "unable to get set stats in bucket"
)
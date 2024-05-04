package rdbv2

import "os"
import "sync"
import "github.com/sirgallo/logger"

import "github.com/sirgallo/rdbv2/internal/campaign"
import "github.com/sirgallo/rdbv2/internal/pool"
import "github.com/sirgallo/rdbv2/internal/replication"
import "github.com/sirgallo/rdbv2/internal/request"
import "github.com/sirgallo/rdbv2/internal/snapshot"
import "github.com/sirgallo/rdbv2/internal/state"
import "github.com/sirgallo/rdbv2/internal/system"
import "github.com/sirgallo/rdbv2/internal/wal"


//=========================================== Raft Service


var Log = logger.NewCustomLog(NAME)

/*
	initialize sub modules under the same raft service and link together
*/

func NewRDB(opts RDBOpts) *RDB {
	hostname, hostErr := os.Hostname()
	if hostErr != nil { Log.Fatal("unable to get hostname") }

	wal, walErr := wal.NewWAL()
	if walErr != nil { Log.Fatal("unable to create or open WAL") }

	state, sErr := state.NewState()
	if sErr != nil { Log.Fatal("unable to create or open State Machine") }

	currentSystem := &system.System{
		Host: hostname,
		CurrentTerm: 0,
		CommitIndex: DefaultCommitIndex,
		LastApplied: DefaultLastApplied,
		SysStatus: system.Ready,
		WAL: wal,
		State: state,
	}

	rdb := &RDB{ Protocol: opts.Protocol, Systems: &sync.Map{}, CurrentSystem: currentSystem }

	for _, sys := range opts.SystemsList {
		sys.UpdateNextIndex(0)
		sys.SetStatus(system.Ready)
		rdb.Systems.Store(sys.Host, sys)
	}

	leConnPool := pool.NewPool(opts.PoolOpts)
	rlConnPool := pool.NewPool(opts.PoolOpts)
	snpConnPool := pool.NewPool(opts.PoolOpts)

	reqOpts := &request.RequestServiceOpts{ Port: opts.Ports.Request, CurrentSystem: currentSystem }

	leOpts := &campaign.CampaignOpts{
		Port: opts.Ports.Campaign,
		Pool: leConnPool,
		CurrentSystem: currentSystem,
		Systems: rdb.Systems,
	}

	rlOpts := &replication.ReplicationOpts{
		Port: opts.Ports.Replication,
		Pool: rlConnPool,
		CurrentSystem: currentSystem,
		Systems: rdb.Systems,
	}

	snpOpts := &snapshot.SnapshotServiceOpts{
		Port: opts.Ports.Snapshot,
		Pool: snpConnPool,
		CurrentSystem: currentSystem,
		Systems: rdb.Systems,
	}

	httpService := request.NewRequestService(reqOpts)
	leService := campaign.NewCampaignService(leOpts)
	rlService := replication.NewReplicationService(rlOpts)
	snpService := snapshot.NewSnapshotService(snpOpts)

	rdb.Request = httpService
	rdb.Campaign = leService
	rdb.Replication = rlService
	rdb.Snapshot = snpService

	return rdb
}

/*
	Start Raft Service:
		start all sub modules and create go routines to link appropriate channels

		1.) start log apply go routine and update state machine on startup
			go routine:
				on new logs to apply to the state machine, pass through to the state machine
				and once processed, return successful and failed logs to the log commit channel

				--> needs to be started before log updates can be applied

			update replicated logs on startup

		2.) start all sub modules
		3.) start module pass throughs 
*/

func (rdb *RDB) StartRaftService() {
	var updateStateMachineMutex sync.Mutex
	updateStateMachineMutex.Lock()

	_, updateErr := rdb.UpdateLogOnStartup()
	if updateErr != nil { Log.Error("error on log replication:", updateErr.Error()) }

	statsErr := rdb.InitStats()
	if statsErr != nil { Log.Error("error fetching initial stats", statsErr.Error()) }

	updateStateMachineMutex.Unlock()

	rdb.StartModules()
	rdb.StartModulePassThroughs()
	
	select {}
}
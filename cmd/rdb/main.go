package main

import (
	"log"
	"os"
	"github.com/sirgallo/array"
	"github.com/sirgallo/logger"
	
	"github.com/sirgallo/rdbv2"
	"github.com/sirgallo/rdbv2/internal/pool"
	"github.com/sirgallo/rdbv2/internal/system"
)


const NAME = "Main"
var Log = logger.NewCustomLog(NAME)

func main() {
	hostname, hostErr := os.Hostname()
	if hostErr != nil { log.Fatal("unable to get hostname") }

	systemsList := []*system.System{
		{ Host: "rdb_replica_0" },
		{ Host: "rdb_replica_1" },
		{ Host: "rdb_replica_2" },
		{ Host: "rdb_replica_3" },
		{ Host: "rdb_replica_4" },
	}

	sysFilter := func(sys *system.System) bool { return sys.Host != hostname }
	otherSystems := array.Filter[*system.System](systemsList, sysFilter)

	raftOpts := rdbv2.RDBOpts{
		Protocol: "tcp",
		Ports: rdbv2.RDBPortOpts{
			Campaign: 54321,
			Replication: 54322,
			Snapshot: 54323,
			Request: 8080,
		},
		SystemsList: otherSystems,
		PoolOpts: pool.PoolOpts{ MaxConn: 10 },
	}

	raft := rdbv2.NewRDB(raftOpts)
	go raft.StartRaftService()
	
	select{}
}
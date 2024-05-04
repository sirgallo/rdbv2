package rdbv2

import "net"

import "github.com/sirgallo/rdbv2/internal/system"


//=========================================== Raft Modules


/*
	Start Modules
		initialize net listeners and start all sub modules
		modules:
			1. replicated log module
			2. leader election module
			3. snapshot module
			4. http request module
*/

func (rdb *RDB) StartModules() {
	leListener, leErr := net.Listen(rdb.Protocol, rdb.Campaign.Port)
	if leErr != nil { Log.Error(LISTENING_ERROR, leErr.Error()) }

	rlListener, rlErr := net.Listen(rdb.Protocol, rdb.Replication.Port)
	if rlErr != nil { Log.Error(LISTENING_ERROR, rlErr.Error()) }

	snpListener, snpErr := net.Listen(rdb.Protocol, rdb.Snapshot.Port)
	if snpErr != nil { Log.Error(LISTENING_ERROR, snpErr.Error()) }

	go rdb.Replication.StartReplicatedLogService(&rlListener)
	go rdb.Campaign.StartCampaignService(&leListener)
	go rdb.Snapshot.StartSnapshotService(&snpListener)
	go rdb.Request.StartRequestService()
}

/*
	Start Module Pass Throughs
		go routine 1:
			on acknowledged signal from rep log module, attempt reset timeout on
			leader election module
		go routine 2:
			on signal from successful leader election, force heartbeat on log module
		go routine 3:
			on responses from state machine ops, pass to request channel to be sent to
			the client
		go routine 4:
			on signal from the replicated log module that a follower needs the most up
			to date snapshot, signal the snapshot module to send to that follower
*/

func (rdb *RDB) StartModulePassThroughs() {
	go func() {
		for range rdb.Replication.LeaderAcknowledgedSignal {
			rdb.Campaign.ResetTimeoutSignal <- true
		}
	}()

	go func() {
		for range rdb.Campaign.HeartbeatOnElection {
			rdb.Replication.ForceHeartbeatSignal <- true
		}
	}()

	go func() {
		for cmdEntry := range rdb.Request.RequestChannel {
			if rdb.CurrentSystem.SysState == system.Leader {
				rdb.Replication.AppendLogSignal <- cmdEntry
			}
		}
	}()

	go func() {
		for response := range rdb.Replication.StateResponseChannel {
			if rdb.CurrentSystem.SysState == system.Leader {
				rdb.Request.ResponseChannel <- response
			}
		}
	}()

	go func() {
		for host := range rdb.Replication.SendSnapshotToSystemSignal {
			rdb.Replication.SendSnapshotToSystemSignal <- host
		}
	}()
}
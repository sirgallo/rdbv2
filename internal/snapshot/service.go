package snapshot

import (
	"net"
	"time"
	"google.golang.org/grpc"
	"github.com/sirgallo/logger"
	
	"github.com/sirgallo/rdbv2/internal/snapshot/snapshot_proto"
	"github.com/sirgallo/rdbv2/internal/system"
	rdbUtils "github.com/sirgallo/rdbv2/internal/utils"
)


//=========================================== Snapshot Service


//	NewSnapshotService:
//		create a new service instance with passable options.
func NewSnapshotService(opts *SnapshotServiceOpts) *SnapshotService {
	return &SnapshotService{
		Port: rdbUtils.NormalizePort(opts.Port),
		Pool: opts.Pool,
		CurrentSystem: opts.CurrentSystem,
		Systems: opts.Systems,
		SnapshotStartSignal: make(chan bool),
		UpdateSnapshotForSystemSignal: make(chan string),
		Log: *logger.NewCustomLog(NAME),
	}
}

//	StartSnapshotService:
//		start the snapshot service:
//			-->	launch the grpc server for SnapshotRPC
//			--> start the start the snapshot listener
func (snpService *SnapshotService) StartSnapshotService(listener *net.Listener) {
	srv := grpc.NewServer()
	snpService.Log.Info(RPC_SERVER_LISTENING, snpService.Port)
	snapshot_proto.RegisterSnapshotServiceServer(srv, snpService)

	go func() {
		err := srv.Serve(*listener)
		if err != nil { snpService.Log.Error(SYSTEM_ERROR, err.Error()) }
	}()

	snpService.StartSnapshotListener()
}

//	StartSnapshotListener:
//		separate go routines:
//			1.) attempt snapshot timer and wait for timer to drain, signal attempt snapshot, and reset timer
//			2.) snapshot signal
//				-->	if current leader, snapshot the state machine and then signal complete 
//			3.) update snapshot for node
//				--> if leader, send the latest snapshot on the system to the target follower
//			4.) attempt trigger snapshot
//				--> on interval, check if we can begin the snapshot process
func (snpService *SnapshotService) StartSnapshotListener() {
	snpService.AttemptSnapshotTimer = time.NewTimer(AttemptSnapshotInterval)
	timeoutChan := make(chan bool)

	go func() {
		for range snpService.AttemptSnapshotTimer.C {
			timeoutChan <- true
			snpService.resetAttemptSnapshotTimer()
		}
	}()

	go func() {
		var snapshotErr error

		for range snpService.SnapshotStartSignal {
			if snpService.CurrentSystem.SysState == system.Leader { 
				snapshotErr = snpService.Snapshot() 
				if snapshotErr != nil { snpService.Log.Error(UPDATE_SNAPSHOT_ERROR, snapshotErr.Error()) }
			}
		}
	}()

	go func() {
		for host := range snpService.UpdateSnapshotForSystemSignal {
			if snpService.CurrentSystem.SysState == system.Leader {
				go func(host string) { 
					updateErr := snpService.UpdateIndividualSystem(host)
					if updateErr != nil { snpService.Log.Error(UPDATE_SYSTEM_ERROR, updateErr) }
				}(host)
			}
		}
	}()

	go func() {
		for range timeoutChan {
			if snpService.CurrentSystem.SysState == system.Leader && snpService.CurrentSystem.SysStatus != system.Busy {
				_, attemptTriggerErr := snpService.AttemptTriggerSnapshot()
				if attemptTriggerErr != nil { snpService.Log.Error(UPDATE_SNAPSHOT_ERROR, attemptTriggerErr.Error()) }
			}
		}
	}()
}

//	AttemptTriggerSnapshot:
//		if the size of the replicated log has exceeded the threshold determined dynamically by available space in the current mount and the current node is the leader, trigger a snapshot event.
//		this takes a snapshot of the current state to store and broadcast to all followers.
//		also pause the replicated log and let buffer until the snapshot is complete. 
//		if a snapshot is performed, calculate the current system stats to update the dynamic threshold for snapshotting.
func (snpService *SnapshotService) AttemptTriggerSnapshot() (bool, error) {
	bucketSizeInBytes, getSizeErr := snpService.CurrentSystem.WAL.GetBucketSizeInBytes()
	if getSizeErr != nil { 
		snpService.Log.Error(READ_ERROR, getSizeErr.Error())
		return false, getSizeErr
	}

	triggerSnapshot := func() bool { 
		statsArr, getStatsErr := snpService.CurrentSystem.WAL.GetStats()
		if statsArr == nil || getStatsErr != nil { return false }

		latestObj := statsArr[len(statsArr) - 1]
		thresholdInBytes := latestObj.AvailableDiskSpaceInBytes / FractionOfAvailableSizeToTake // let's keep this small for no
		lastAppliedAtThreshold := bucketSizeInBytes >= thresholdInBytes
		systemAbleToSnapshot := snpService.CurrentSystem.SysState == system.Leader && snpService.CurrentSystem.SysStatus != system.Busy
		
		return lastAppliedAtThreshold && systemAbleToSnapshot
	}()

	if triggerSnapshot { 
		snpService.CurrentSystem.SetStatus(system.Busy)
		select {
		case snpService.SnapshotStartSignal <- true:
		default:
		}
	}

	return true, nil
}
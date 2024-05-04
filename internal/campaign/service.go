package campaign

import (
	"net"
	"time"
	"google.golang.org/grpc"
	"github.com/sirgallo/logger"
	
	"github.com/sirgallo/rdbv2/internal/campaign/campaign_proto"
	"github.com/sirgallo/rdbv2/internal/system"
	rdbUtils "github.com/sirgallo/rdbv2/internal/utils"
)

//=========================================== Leader Election Service


/*
	create a new service instance with passable options
	--> initialize state to Follower and initialize a random timeout period for leader election
*/

func NewCampaignService(opts *CampaignOpts) *CampaignService {
	cService := &CampaignService{
		Port: rdbUtils.NormalizePort(opts.Port),
		Pool: opts.Pool,
		CurrentSystem: opts.CurrentSystem,
		Systems: opts.Systems,
		Timeout: initTimeoutOnStartup(),
		ResetTimeoutSignal: make(chan bool),
		HeartbeatOnElection: make(chan bool),
		Log: *logger.NewCustomLog(NAME),
	}

	cService.CurrentSystem.TransitionToFollower(system.StateTransitionOpts{})
	return cService
}

/*
	start the replicated log module/service:
		--> launch the grpc server for AppendEntryRPC
		--> start the leader election timeout
*/

func (cService *CampaignService) StartCampaignService(listener *net.Listener) {
	srv := grpc.NewServer()
	cService.Log.Info(RPC_SERVER_LISTENING, cService.Port)
	campaign_proto.RegisterCampaignServiceServer(srv, cService)

	go func() {
		err := srv.Serve(*listener)
		if err != nil { cService.Log.Error(SYSTEM_ERROR, err.Error()) }
	}()

	cService.StartElectionTimeout()
}

/*
	start the election timeouts:
		1.) if a signal is passed indicating that an AppendEntryRPC has been received from a
			legitimate leader, reset the election timeout
		2.) otherwise, on timeout, start the leader election process
*/

func (cService *CampaignService) StartElectionTimeout() {
	cService.ElectionTimer = time.NewTimer(cService.Timeout)
	timeoutChannel := make(chan bool)

	go func() {
		for {
			select {
			case <- cService.ResetTimeoutSignal:
				cService.resetTimer()
			case <- cService.ElectionTimer.C:
				timeoutChannel <- true
				cService.resetTimer()
			}
		}
	}()

	go func() {
		for range timeoutChannel {
			if cService.CurrentSystem.SysState == system.Follower { cService.Election() }
		}
	}()
}
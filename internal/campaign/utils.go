package campaign

import (
	"math/rand"
	"time"
	
	"github.com/sirgallo/rdbv2/internal/system"
)


//=========================================== Campaign Utils



//	GetAliveSystemsAndMinVotes:
//		helper method for both determining the current alive systems in the cluster and also the minimum votes needed for transitioning to leader.
//
//		in a fault tolerant system like raft, we also need to be aware of the total number of faults that the cluster can withstand before the system can no longer make progress.
//		if the total alive systems are less than the minimum allowed systems for fault tolerance, we need to ensure that these systems do not become leaders and begin replicating in the case that a network partition occurs.
//		this can be calculated by the following:
//			quorum = floor((total systems / 2) + 1)
func (cService *CampaignService) GetAliveSystemsAndMinVotes() ([]*system.System, int64) {
	var aliveSystems []*system.System
	
	totalSystems := int64(1)
	cService.Systems.Range(func(key, value interface{}) bool {
		sys := value.(*system.System)
		if sys.SysStatus != system.Dead { aliveSystems = append(aliveSystems, sys) }
		
		totalSystems++
		return true
	})

	return aliveSystems, int64((totalSystems / 2) + 1)
}

// calculateTimeout
//	initialize the timeout period for the follower node.
//	this implementation has chosen a standard 150-300ms timeout, but a more dynamic approach could be taken to calculate the timeout
func calculateTimeout() time.Duration {
	timeout := rand.Intn(151) + 925
	return time.Duration(timeout) * time.Millisecond
}

func initTimeoutOnStartup() time.Duration {
	timeDuration := calculateTimeout()
	return timeDuration
}

//	resetTimer:
//		1.) Generate a randomized timeout between 150-300ms
//		2.) if unable to stop the timer, drain the timer
//		3.) reset the timer with the new random timeout period
func (cService *CampaignService) resetTimer() {
	reInitTimeout := func() {
		timeoutDuration := calculateTimeout()
		cService.Timeout = timeoutDuration
	}

	reInitTimeout()
	if ! cService.ElectionTimer.Stop() {
    select {
		case <- cService.ElectionTimer.C:
		default:
		}
	}

	cService.ElectionTimer.Reset(cService.Timeout)
}

func (cService *CampaignService) attemptResetTimeoutSignal() {
	select {
	case cService.ResetTimeoutSignal <- true:
	default:
	}
}
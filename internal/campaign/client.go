package campaign

import (
	"context"
	"sync"
	"sync/atomic"
	"google.golang.org/grpc"
	"github.com/sirgallo/utils"

	"github.com/sirgallo/rdbv2/internal/campaign/campaign_proto"
	"github.com/sirgallo/rdbv2/internal/system"
)


//=========================================== Campaign Client


//	Election:
//		when the election timeout is reached, an election occurs:
//			1.) the current system updates itself to candidate state, votes for itself, and updates the term monotonically
//			2.) send RequestVoteRPCs in parallel
//			3.) if the candidate receives the minimum number of votes required to be a leader (so quorum):
//					-->	the leader updates its state to Leader and immediately sends heartbeats to establish authority. 
//						-->	On transition to leader, the new leader will also update the next index of all of the known systems to reflect the last log index on the system
//			4.) if a higher term is discovered, update the current term of the candidate to reflect this and revert back to follower state
//			5.) otherwise, set the system back to Follower, reset the VotedFor field, and reinitialize the leader election timeout, so randomly generate new timeout period for the system
func (cService *CampaignService) Election() error {
	var campaignWG sync.WaitGroup
	var electionErr error

	cService.CurrentSystem.TransitionToCandidate()
	cRespChans := cService.createLERespChannels()

	defer close(cRespChans.VotesChan)
	defer close(cRespChans.HigherTermDiscovered)

	aliveSystems, minimumVotes := cService.GetAliveSystemsAndMinVotes()
	votesGranted := int64(1)
	electionErrChan := make(chan error, 1)

	campaignWG.Add(1)
	go func() {
		defer campaignWG.Done()

		for {
			select {
			case <- cRespChans.BroadcastClose:
				if votesGranted >= int64(minimumVotes) {
					cService.CurrentSystem.TransitionToLeader()
					
					var lastLogIndex int64
					lastLogIndex, _, electionErr = cService.CurrentSystem.DetermineLastLogIdxAndTerm()
					if electionErr != nil { 
						electionErrChan <- electionErr 
						return
					}

					cService.Systems.Range(func(key, value interface{}) bool {
						sys := value.(*system.System)
						sys.UpdateNextIndex(lastLogIndex)
						return true
					})

					cService.HeartbeatOnElection <- true
				} else {
					cService.Log.Warn(MINIMUM_RESPONSES_RECEIVED_ERROR)
					cService.CurrentSystem.TransitionToFollower(system.StateTransitionOpts{})
					cService.attemptResetTimeoutSignal()
				}

				return 
			case <- cRespChans.VotesChan:
				atomic.AddInt64(&votesGranted, 1)
			case term :=<- cRespChans.HigherTermDiscovered:
				cService.Log.Warn(HIGHER_TERM_ERROR)
				cService.CurrentSystem.TransitionToFollower(system.StateTransitionOpts{ CurrentTerm: &term })
				cService.attemptResetTimeoutSignal()
				return
			}
		}
	}()

	campaignWG.Add(1)
	go func() {
		defer campaignWG.Done()
		broadcastErr := cService.broadcastVotes(aliveSystems, cRespChans)
		if broadcastErr != nil { cService.Log.Error(BROADCAST_ERROR, broadcastErr.Error()) }
	}()

	campaignWG.Wait()

	select {
	case incomingErr :=<- electionErrChan:
		return incomingErr
	default:
		return nil
	}
}

//	broadcastVotes:
//		utilized by the Election function
//		
//		RequestVoteRPCs are generated and a go routine is spawned for each system that a request is being sent to. 
//		if a higher term  is discovered, all go routines are signalled to stop broadcasting.
func (cService *CampaignService) broadcastVotes(aliveSystems []*system.System, cRespChans CampaignResponseChannels) error {
	var requestVoteWG sync.WaitGroup
	var broadcastErr error
	
	defer close(cRespChans.BroadcastClose)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var lastLogIndex, lastLogTerm int64
	lastLogIndex, lastLogTerm, broadcastErr = cService.CurrentSystem.DetermineLastLogIdxAndTerm()
	if broadcastErr != nil { return broadcastErr }

	request := &campaign_proto.RequestVote{
		CurrentTerm:  cService.CurrentSystem.CurrentTerm,
		CandidateId:  cService.CurrentSystem.Host,
		LastLogIndex: lastLogIndex,
		LastLogTerm:  lastLogTerm,
	}
 
	for _, sys := range aliveSystems {
		requestVoteWG.Add(1)
		go func(sys *system.System) {
			defer requestVoteWG.Done()

			var conn *grpc.ClientConn
			var requestVoteErr error
			
			conn, requestVoteErr = cService.Pool.GetConnection(sys.Host, cService.Port)
			if requestVoteErr != nil { 
				cService.Log.Error(CONNECTION_POOL_ERROR, sys.Host + ":" + cService.Port, ":", requestVoteErr.Error()) 
				return
			}

			select {
			case <- ctx.Done():
				cService.Pool.PutConnection(sys.Host, conn)
				return
			default:
				conn, requestVoteErr = cService.Pool.GetConnection(sys.Host, cService.Port)
				if requestVoteErr != nil { cService.Log.Error(CONNECTION_POOL_ERROR, sys.Host + ":" + cService.Port, ":", requestVoteErr.Error()) }

				client := campaign_proto.NewCampaignServiceClient(conn)
				requestVoteRPC := func() (*campaign_proto.RequestVoteResponse, error) {
					ctx, cancel := context.WithTimeout(context.Background(), RPCTimeout)
					defer cancel()

					res, err := client.RequestVoteRPC(ctx, request)
					if err != nil { return utils.GetZero[*campaign_proto.RequestVoteResponse](), err }
					return res, nil
				}

				maxRetries := 5
				expOpts := utils.ExpBackoffOpts{ MaxRetries: &maxRetries, TimeoutInNanosecs: 1000000 }
				expBackoff := utils.NewExponentialBackoffStrat[*campaign_proto.RequestVoteResponse](expOpts)

				var res *campaign_proto.RequestVoteResponse
				res, requestVoteErr = expBackoff.PerformBackoff(requestVoteRPC)
				if requestVoteErr != nil { 
					cService.Log.Warn(sys.Host, HOST_UNREACHABLE)
					sys.SetStatus(system.Dead)
					cService.Pool.CloseConnections(sys.Host)
					return 
				}

				if res.VoteGranted { cRespChans.VotesChan <- 1 }
				if res.Term > cService.CurrentSystem.CurrentTerm {
					cRespChans.HigherTermDiscovered <- res.Term
					cancel()
				}
			
				cService.Pool.PutConnection(sys.Host, conn)
			}
		}(sys)
	}

	requestVoteWG.Wait()
	return nil
}

func (cService *CampaignService) createLERespChannels() CampaignResponseChannels {
	broadcastClose := make(chan struct{})
	votesChan := make(chan int)
	higherTermDiscovered := make(chan int64)
	return CampaignResponseChannels { BroadcastClose: broadcastClose, VotesChan: votesChan, HigherTermDiscovered: higherTermDiscovered }
}
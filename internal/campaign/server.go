package campaign

import (
	"context"
	"github.com/sirgallo/utils"
	
	"github.com/sirgallo/rdbv2/internal/campaign/campaign_proto"
	"github.com/sirgallo/rdbv2/internal/system"
)


//=========================================== Leader Election Server


/*
	RequestVoteRPC:
		grpc server implementation

		when a RequestVoteRPC is made to the requestVote server
			1.) load the host from the request into the systems map
			2.) if the incoming request has a higher term than the current system, update the current term and set the system 
				to Follower State
			3.) if the system hasn't voted this term or has already voted for the incoming candidate 
				and the last log index and term	of the request are at least as up to date as what is on the current system
				--> grant the vote to the candidate, reset VotedFor, update the current term to the term of the request, and
					revert back to Follower state
			4.) if the request has a higher term than currently on the system, 
			5.) otherwise, do not grant the vote
*/

func (cService *CampaignService) RequestVoteRPC(ctx context.Context, req *campaign_proto.RequestVote) (*campaign_proto.RequestVoteResponse, error) {
	var requestVoteRPCErr error

	var lastLogIndex, lastLogTerm int64
	lastLogIndex, lastLogTerm, requestVoteRPCErr = cService.CurrentSystem.DetermineLastLogIdxAndTerm()
	if requestVoteRPCErr != nil { return nil, requestVoteRPCErr }

	s, ok := cService.Systems.Load(req.CandidateId)
	if ! ok { 
		sys := &system.System{ Host: req.CandidateId, SysStatus: system.Ready, NextIndex: lastLogIndex }
		cService.Systems.Store(sys.Host, sys)
	} else {
		sys := s.(*system.System)
		sys.SetStatus(system.Ready)
		sys.UpdateNextIndex(lastLogIndex)
	}

	cService.Log.Debug(VOTE_RECEIVED, req.CandidateId)
	cService.Log.Debug("req current term:", req.CurrentTerm, "system current term:", cService.CurrentSystem.CurrentTerm)
	cService.Log.Debug("latest log index:", lastLogIndex, "req last log index:", req.LastLogIndex)
	
	if cService.CurrentSystem.VotedFor == utils.GetZero[string]() || cService.CurrentSystem.VotedFor == req.CandidateId {
		if req.LastLogIndex >= lastLogIndex && req.LastLogTerm >= lastLogTerm {
			cService.CurrentSystem.TransitionToFollower(system.StateTransitionOpts{ CurrentTerm: &req.CurrentTerm, VotedFor: &req.CandidateId })
			cService.attemptResetTimeoutSignal()
			cService.Log.Info(VOTE_GRANTED, req.CandidateId)
			return &campaign_proto.RequestVoteResponse{ Term: cService.CurrentSystem.CurrentTerm, VoteGranted: true }, nil
		}
	}

	if req.CurrentTerm > cService.CurrentSystem.CurrentTerm {
		cService.Log.Warn(HIGHER_TERM_ERROR)
		cService.CurrentSystem.TransitionToFollower(system.StateTransitionOpts{ CurrentTerm: &req.CurrentTerm })
		cService.attemptResetTimeoutSignal()
	}

	return &campaign_proto.RequestVoteResponse{ Term: cService.CurrentSystem.CurrentTerm, VoteGranted: false }, nil
}
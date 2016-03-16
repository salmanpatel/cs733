package main

import "fmt"

type VoteResEv struct {
	term        int64
	voteGranted bool
}

func (sm *StateMachine) VoteResEH(ev VoteResEv) []interface{} {
	var actions []interface{}
	switch sm.state {
	case "Leader":
		return sm.LeaderVoteResEH(ev)
	case "Follower":
		return sm.FollowerVoteResEH(ev)
	case "Candidate":
		return sm.CandidateVoteResEH(ev)
	}
	return actions
}

func (sm *StateMachine) LeaderVoteResEH(ev VoteResEv) []interface{} {
	var actions []interface{}
	return actions
}

func (sm *StateMachine) FollowerVoteResEH(ev VoteResEv) []interface{} {
	var actions []interface{}
	if ev.term > sm.term {
		sm.term = ev.term
		sm.votedFor = 0
		actions = append(actions, StateStoreAc{sm.term, sm.state, sm.votedFor})
	}
	return actions
}

func (sm *StateMachine) CandidateVoteResEH(ev VoteResEv) []interface{} {
	var actions []interface{}
	majority := len(sm.config.peerIds)/2 + 1
	flag := false
	if ev.voteGranted {
		sm.yesVotes += 1
		if sm.yesVotes >= int64(majority) {
			flag = true
			sm.state = "Leader"
			fmt.Printf("Leader elected : Server Id = %v \n", sm.config.serverId)
			for i := 0; i < len(sm.config.peerIds); i++ {
				sm.nextIndex[i] = int64(len(sm.log))
				sm.matchIndex[i] = 0
				actions = append(actions, SendAc{sm.config.peerIds[i], AppendEntriesReqEv{sm.term, sm.config.serverId, int64(len(sm.log) - 1), sm.log[len(sm.log)-1].term, nil, sm.commitIndex}})
			}
			actions = append(actions, AlarmAc{RandInt(75, 150)})
		}
	} else if ev.term > sm.term {
		flag = true
		sm.term = ev.term
		sm.state = "Follower"
		sm.votedFor = 0
		actions = append(actions, AlarmAc{RandInt(150, 300)})
	} else {
		sm.noVotes++
		if sm.noVotes >= int64(majority) {
			flag = true
			sm.state = "Follower"
			actions = append(actions, AlarmAc{150})
		}
	}
	if flag {
		actions = append(actions, StateStoreAc{sm.term, sm.state, sm.votedFor})
	}
	return actions
}

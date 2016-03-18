package main

import "fmt"

type VoteResEv struct {
	Term        int64
	VoteGranted bool
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
	if ev.Term > sm.term {
		sm.term = ev.Term
		sm.votedFor = 0
		actions = append(actions, StateStoreAc{sm.term, sm.state, sm.votedFor})
	}
	return actions
}

func (sm *StateMachine) CandidateVoteResEH(ev VoteResEv) []interface{} {
	var actions []interface{}
	majority := len(sm.config.peerIds)/2 + 1
	flag := false
	if ev.VoteGranted {
		sm.yesVotes += 1
		if sm.yesVotes >= int64(majority) {
			flag = true
			sm.state = "Leader"
			fmt.Printf("%v Elected as a Leader : Term = %v \n", sm.config.serverId, sm.term)
			for i := 0; i < len(sm.config.peerIds); i++ {
				sm.nextIndex[i] = int64(len(sm.log))
				sm.matchIndex[i] = -1
				term := int64(0)
				if len(sm.log) != 0 {
					term = sm.log[len(sm.log)-1].Term
				}
				actions = append(actions, SendAc{sm.config.peerIds[i], AppendEntriesReqEv{sm.term, sm.config.serverId, int64(len(sm.log) - 1), term, nil, sm.commitIndex}})
			}
			actions = append(actions, AlarmAc{sm.heartbeatTO})
		}
	} else if ev.Term > sm.term {
		flag = true
		sm.term = ev.Term
		sm.state = "Follower"
		sm.votedFor = 0
		actions = append(actions, AlarmAc{RandInt(sm.electionTO)})
	} else {
		sm.noVotes++
		if sm.noVotes >= int64(majority) {
			flag = true
			sm.state = "Follower"
			actions = append(actions, AlarmAc{RandInt(sm.electionTO)})
		}
	}
	if flag {
		actions = append(actions, StateStoreAc{sm.term, sm.state, sm.votedFor})
	}
	return actions
}

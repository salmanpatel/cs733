package main

//import "fmt"

type VoteResEv struct {
	term uint64
	voteGranted bool
}

func (sm *StateMachine) VoteResEH(ev VoteResEv) ([]interface{}) {
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

func (sm *StateMachine) LeaderVoteResEH(ev VoteResEv) ([]interface{}) {
	var actions []interface{}
	return actions
}

func (sm *StateMachine) FollowerVoteResEH(ev VoteResEv) ([]interface{}) {
	var actions []interface{}
	if ev.term > sm.term {
		sm.term = ev.term
		sm.votedFor = 0
		actions = append(actions, StateStoreAc{sm.term, sm.state, sm.votedFor})
	}
	return actions
}

func (sm *StateMachine) CandidateVoteResEH(ev VoteResEv) ([]interface{}) {
	var actions []interface{}
	majority := len(sm.config.peerIds)/2 + 1
	flag := false
	if ev.voteGranted {
		sm.yesVotes += 1
		if sm.yesVotes >= uint64(majority) {
			flag = true
			sm.state = "Leader"
			for i:=0; i<len(sm.config.peerIds); i++ {
				sm.nextIndex[i] = uint64(len(sm.log))
				sm.matchIndex[i] = 0
				actions = append(actions, SendAc{sm.config.peerIds[i], AppendEntriesReqEv{sm.term, sm.config.serverId, uint64(len(sm.log)-2), sm.log[len(sm.log)-2].term, nil, sm.commitIndex}})
			}
			actions = append(actions, AlarmAc{150})
		}
	} else if ev.term > sm.term {
		flag = true
		sm.term = ev.term
		sm.state = "Follower"
		sm.votedFor = 0
		actions = append(actions, AlarmAc{150})
	} else {
		flag = true
		sm.noVotes++
		if sm.noVotes >= uint64(majority) {
			sm.state = "Follower"
			actions = append(actions, AlarmAc{150})	
		}
	}
	if flag {
		actions = append(actions, StateStoreAc{sm.term, sm.state, sm.votedFor})
	}
	return actions
}

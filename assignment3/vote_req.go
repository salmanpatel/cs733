package main

import "fmt"

type VoteReqEv struct {
	term         int64
	candidateId  int64
	lastLogIndex int64
	lastLogTerm  int64
}

func (sm *StateMachine) VoteReqEH(ev VoteReqEv) []interface{} {
	var actions []interface{}
	switch sm.state {
	case "Leader":
		actions = sm.LeaderCandidateVoteReqEH(ev)
	case "Follower":
		actions = sm.FollowerVoteReqEH(ev)
	case "Candidate":
		actions = sm.LeaderCandidateVoteReqEH(ev)
	}
	return actions
}

/*func isFollowerUpToDate() bool {

}*/

func (sm *StateMachine) LeaderCandidateVoteReqEH(ev VoteReqEv) []interface{} {
	fmt.Printf("LeaderCandidateVoteReqEH: Server Id = %v \n", sm.config.serverId)
	var actions []interface{}
	if sm.term < ev.term {
		sm.term = ev.term
		sm.votedFor = 0
		sm.state = "Follower"
		actions = append(actions, AlarmAc{RandInt(150, 300)})
		if (sm.log[len(sm.log)-1].term < ev.lastLogTerm) || (sm.log[len(sm.log)-1].term == ev.lastLogTerm && int64(len(sm.log)-1) <= ev.lastLogIndex) {
			actions = append(actions, SendAc{ev.candidateId, VoteResEv{sm.term, true}})
			sm.votedFor = ev.candidateId
		} else {
			actions = append(actions, SendAc{ev.candidateId, VoteResEv{sm.term, false}})
		}
		actions = append(actions, StateStoreAc{sm.term, sm.state, sm.votedFor})
	} else {
		actions = append(actions, SendAc{ev.candidateId, VoteResEv{sm.term, false}})
	}
	return actions
}

func (sm *StateMachine) FollowerVoteReqEH(ev VoteReqEv) []interface{} {
	fmt.Printf("FollowerVoteReqEH: Server Id = %v \n", sm.config.serverId)
	var actions []interface{}
	// votedFor = 0, means it has not voted for this term
	flag := false
	if (sm.term < ev.term) || ((sm.term == ev.term) && (sm.votedFor == 0 || sm.votedFor == ev.candidateId)) {
		if sm.term < ev.term {
			sm.votedFor = 0
			sm.term = ev.term
			flag = true
		}
		if (sm.log[len(sm.log)-1].term < ev.lastLogTerm) || (sm.log[len(sm.log)-1].term == ev.lastLogTerm && int64(len(sm.log)-1) <= ev.lastLogIndex) {
			if sm.term < ev.term {
				flag = true
			}
			sm.votedFor = ev.candidateId
			actions = append(actions, AlarmAc{RandInt(150, 300)})
			actions = append(actions, SendAc{ev.candidateId, VoteResEv{sm.term, true}})
		} else {
			actions = append(actions, SendAc{ev.candidateId, VoteResEv{sm.term, false}})
		}
		if flag {
			actions = append(actions, StateStoreAc{sm.term, sm.state, sm.votedFor})
		}
	} else {
		actions = append(actions, SendAc{ev.candidateId, VoteResEv{sm.term, false}})
	}
	return actions
}

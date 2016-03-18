package main

//import "fmt"

type VoteReqEv struct {
	Term         int64
	CandidateId  int64
	LastLogIndex int64
	LastLogTerm  int64
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
//	fmt.Printf("LeaderCandidateVoteReqEH: Server Id = %v \n", sm.config.serverId)
	var actions []interface{}
	if sm.term < ev.Term {
		sm.term = ev.Term
		sm.votedFor = 0
		sm.state = "Follower"
		actions = append(actions, AlarmAc{RandInt(sm.electionTO)})
		term := int64(0)
		if len(sm.log) != 0 {
			term = sm.log[len(sm.log)-1].Term
		}
		if (term < ev.LastLogTerm) || (term == ev.LastLogTerm && int64(len(sm.log)-1) <= ev.LastLogIndex) {
			actions = append(actions, SendAc{ev.CandidateId, VoteResEv{sm.term, true}})
			sm.votedFor = ev.CandidateId
		} else {
			actions = append(actions, SendAc{ev.CandidateId, VoteResEv{sm.term, false}})
		}
		actions = append(actions, StateStoreAc{sm.term, sm.state, sm.votedFor})
	} else {
		actions = append(actions, SendAc{ev.CandidateId, VoteResEv{sm.term, false}})
	}
	return actions
}

func (sm *StateMachine) FollowerVoteReqEH(ev VoteReqEv) []interface{} {
	// fmt.Printf("FollowerVoteReqEH: Server Id = %v \n", sm.config.serverId)
	var actions []interface{}
	// votedFor = 0, means it has not voted for this term
	flag := false
	if (sm.term < ev.Term) || ((sm.term == ev.Term) && (sm.votedFor == 0 || sm.votedFor == ev.CandidateId)) {
		if sm.term < ev.Term {
			sm.votedFor = 0
			sm.term = ev.Term
			flag = true
		}
		term := int64(0)
		if len(sm.log) != 0 {
			term = sm.log[len(sm.log)-1].Term
		}
		if (term < ev.LastLogTerm) || (term == ev.LastLogTerm && int64(len(sm.log)-1) <= ev.LastLogIndex) {
			if sm.term < ev.Term {
				flag = true
			}
			sm.votedFor = ev.CandidateId
			actions = append(actions, AlarmAc{RandInt(sm.electionTO)})
			actions = append(actions, SendAc{ev.CandidateId, VoteResEv{sm.term, true}})
		} else {
			actions = append(actions, SendAc{ev.CandidateId, VoteResEv{sm.term, false}})
		}
		if flag {
			actions = append(actions, StateStoreAc{sm.term, sm.state, sm.votedFor})
		}
	} else {
		actions = append(actions, SendAc{ev.CandidateId, VoteResEv{sm.term, false}})
	}
	return actions
}
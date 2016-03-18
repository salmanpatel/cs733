package main

//import "fmt"

type TimeoutEv struct {
}

func (sm *StateMachine) TimeoutEH(ev TimeoutEv) []interface{} {
	var actions []interface{}
	switch sm.state {
	case "Leader":
		actions = sm.LeaderTimeoutEH(ev)
	case "Follower":
		actions = sm.FollowerCandidateTimeoutEH(ev)
	case "Candidate":
		actions = sm.FollowerCandidateTimeoutEH(ev)
	}
	return actions
}

func (sm *StateMachine) FollowerCandidateTimeoutEH(ev TimeoutEv) []interface{} {
	// fmt.Printf("%v FollowerCandidateTimeoutEH \n", sm.config.serverId)
	var actions []interface{}
	sm.term++
	if sm.state != "Candidate" {
		sm.state = "Candidate"
	}
	sm.votedFor = sm.config.serverId
	actions = append(actions, StateStoreAc{sm.term, sm.state, sm.votedFor})
	// Setting Election timeout
	actions = append(actions, AlarmAc{RandInt(sm.electionTO)})
	for i := 0; i < len(sm.config.peerIds); i++ {
		term := int64(0)
		if len(sm.log) != 0 {
			term = sm.log[len(sm.log)-1].Term
		}
		//fmt.Printf("%v Generating Vote req event : %v \n", sm.config.serverId, sm.config.peerIds[i])
		actions = append(actions, SendAc{sm.config.peerIds[i], VoteReqEv{sm.term, sm.config.serverId, int64(len(sm.log) - 1), term}})
	}
	sm.yesVotes = 1
	return actions
}

func (sm *StateMachine) LeaderTimeoutEH(ev TimeoutEv) []interface{} {
	var actions []interface{}
	for i := 0; i < len(sm.config.peerIds); i++ {
		prevLogTerm := int64(0)
		if sm.nextIndex[i] != 0 {
			prevLogTerm = sm.log[sm.nextIndex[i]-1].Term
		}
		actions = append(actions, SendAc{sm.config.peerIds[i], AppendEntriesReqEv{sm.term, sm.config.serverId, sm.nextIndex[i] - 1, prevLogTerm, sm.log[sm.nextIndex[i]:], sm.commitIndex}})
	}
	actions = append(actions, AlarmAc{RandInt(sm.electionTO)})
	return actions
}
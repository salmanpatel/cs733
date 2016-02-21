package main

type TimeoutEv struct {
}

func (sm *StateMachine) TimeoutEH(ev TimeoutEv) ([]interface{}) {
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

func (sm *StateMachine) FollowerCandidateTimeoutEH(ev TimeoutEv) ([]interface{}) {
	var actions []interface{}
	sm.term++
	if sm.state != "Candidate" {
		sm.state = "Candidate"
	}
	sm.votedFor = sm.config.serverId
	actions = append(actions, StateStoreAc{sm.term, sm.state, sm.votedFor})
	// Setting Election timeout
	actions = append(actions, AlarmAc{150})
	for i:=0; i<len(sm.config.peerIds); i++ {
		actions = append(actions, SendAc{sm.config.peerIds[i], VoteReqEv{sm.term, sm.config.serverId, uint64(len(sm.log)-1), sm.log[len(sm.log)-1].term}})
	}
	sm.yesVotes = 1
	return actions
}

func (sm *StateMachine) LeaderTimeoutEH(ev TimeoutEv) ([]interface{}) {
	var actions []interface{}
	for i:=0; i<len(sm.config.peerIds); i++ {
		actions = append(actions, SendAc{sm.config.peerIds[i], AppendEntriesReqEv{sm.term, sm.config.serverId, uint64(len(sm.log)-2), sm.log[len(sm.log)-2].term, nil, sm.commitIndex}})
	}
	return actions
}

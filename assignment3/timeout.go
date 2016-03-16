package main

import "fmt"

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
	fmt.Printf("FollowerCandidateTimeoutEH: Server Id = %v \n", sm.config.serverId)
	var actions []interface{}
	sm.term++
	if sm.state != "Candidate" {
		sm.state = "Candidate"
	}
	sm.votedFor = sm.config.serverId
	actions = append(actions, StateStoreAc{sm.term, sm.state, sm.votedFor})
	// Setting Election timeout
	actions = append(actions, AlarmAc{RandInt(150, 300)})
	fmt.Printf("Length : %v \n", len(sm.config.peerIds))
	for i := 0; i < len(sm.config.peerIds); i++ {
		fmt.Printf("Generating Vote req event : %v \n", sm.config.peerIds[i])
		actions = append(actions, SendAc{sm.config.peerIds[i], VoteReqEv{sm.term, sm.config.serverId, uint64(len(sm.log) - 1), sm.log[len(sm.log)-1].term}})
	}
	sm.yesVotes = 1
	return actions
}

func (sm *StateMachine) LeaderTimeoutEH(ev TimeoutEv) []interface{} {
	var actions []interface{}
	for i := 0; i < len(sm.config.peerIds); i++ {
		actions = append(actions, SendAc{sm.config.peerIds[i], AppendEntriesReqEv{sm.term, sm.config.serverId, sm.nextIndex[i] - 1, sm.log[sm.nextIndex[i]-1].term, sm.log[sm.nextIndex[i]:], sm.commitIndex}})
	}
	actions = append(actions, AlarmAc{RandInt(75, 150)})
	return actions
}

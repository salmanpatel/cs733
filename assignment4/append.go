package main

import "errors"

//import "fmt"

type AppendEv struct {
	Data []byte
}

func (sm *StateMachine) AppendEH(ev AppendEv) []interface{} {
	var actions []interface{}
	switch sm.state {
	case "Leader":
		actions = sm.LeaderAppendEH(ev)
	case "Follower":
		actions = sm.FollowerCandidateAppendEH(ev)
	case "Candidate":
		actions = sm.FollowerCandidateAppendEH(ev)
	}
	return actions
}

func (sm *StateMachine) LeaderAppendEH(ev AppendEv) []interface{} {
	// fmt.Printf("%v append called \n", sm.config.serverId)
	var actions []interface{}
	sm.log = append(sm.log, LogEntry{sm.term, ev.Data})
	actions = append(actions, LogStoreAc{int64(len(sm.log) - 1), sm.term, ev.Data})
	for i := 0; i < len(sm.config.peerIds); i++ {
		prevTerm := int64(0)
		if sm.nextIndex[i] != 0 {
			prevTerm = sm.log[sm.nextIndex[i]-1].Term
		}
		actions = append(actions, SendAc{sm.config.peerIds[i], AppendEntriesReqEv{sm.term, sm.config.serverId, sm.nextIndex[i] - 1, prevTerm, sm.log[sm.nextIndex[i]:], sm.commitIndex}})
	}
	return actions
}

func (sm *StateMachine) FollowerCandidateAppendEH(ev AppendEv) []interface{} {
	var actions []interface{}
	actions = append(actions, CommitAc{int64(-1), ev.Data, errors.New("Not a Leader")})
	return actions
}

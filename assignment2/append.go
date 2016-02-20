package main

import "errors"

type AppendEv struct {
	data []byte
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

func (sm *StateMachine) LeaderAppendEH(ev AppendEv) ([]interface{}) {
	var actions []interface{}
	sm.log = append(sm.log, LogEntry{sm.term, ev.data})
	actions = append(actions, LogStoreAc{uint64(len(sm.log)-1), ev.data})
	for i:=0; i<len(sm.config.peerIds); i++ {
		actions = append(actions, SendAc{sm.config.peerIds[i], AppendEntriesReqEv{sm.term, sm.config.serverId, sm.nextIndex[sm.config.peerIds[i]]-1, sm.log[sm.nextIndex[sm.config.peerIds[i]]-1].term, sm.log[sm.nextIndex[sm.config.peerIds[i]]:], sm.commitIndex}})
	}
	return actions
}

func (sm *StateMachine) FollowerCandidateAppendEH(ev AppendEv) ([]interface{}) {
	var actions []interface{}
	actions = append(actions, CommitAc{uint64(len(sm.log)-1), ev.data, errors.New("Not a Leader")})
	return actions
}


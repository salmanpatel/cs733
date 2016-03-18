package main

import "fmt"

type AppendEntriesResEv struct {
	From    int64
	Term    int64
	Success bool
}

func (sm *StateMachine) AppendEntriesResEH(ev AppendEntriesResEv) []interface{} {
	var actions []interface{}
	switch sm.state {
	case "Leader":
		return sm.LeaderAppendEntriesResEH(ev)
	case "Follower":
		return sm.FollowerCandidateAppendEntriesResEH(ev)
	case "Candidate":
		return sm.FollowerCandidateAppendEntriesResEH(ev)
	}
	return actions
}

func (sm *StateMachine) LeaderAppendEntriesResEH(ev AppendEntriesResEv) []interface{} {
	var actions []interface{}
	// Find index of peer
	var fromIndex int64
	for i := 0; i < len(sm.config.peerIds); i++ {
		if sm.config.peerIds[i] == ev.From {
			fromIndex = int64(i)
			break
		}
	}
	fmt.Printf("%v LeaderAppendEntriesResEH: response:%v, from=%v, sm.nextIndex[fromIndex]=%v \n", sm.config.serverId, ev, ev.From, sm.nextIndex[fromIndex])
	// Append Entry Failure
	if !ev.Success {
		if sm.term < ev.Term {
			sm.term = ev.Term
			sm.votedFor = 0
			sm.state = "Follower"
			actions = append(actions, AlarmAc{RandInt(sm.electionTO)})
			actions = append(actions, StateStoreAc{sm.term, sm.state, sm.votedFor})
		} else {
			// Valid Leader - Mismatch in prevIndex entry
			sm.nextIndex[fromIndex]--
			prevTerm := int64(0)
			if sm.nextIndex[fromIndex] != 0 {
				prevTerm = sm.log[sm.nextIndex[fromIndex]-1].Term
			}
			actions = append(actions, SendAc{ev.From, AppendEntriesReqEv{sm.term, sm.config.serverId, sm.nextIndex[fromIndex] - 1, prevTerm, sm.log[sm.nextIndex[fromIndex]:], sm.commitIndex}})
		}
	} else {
		// Update sm.matchIndex[msg.from] to the last replicated index
		sm.matchIndex[fromIndex] = int64(len(sm.log)) - 1
		sm.nextIndex[fromIndex] = int64(len(sm.log))
		maxCommitIndex := sm.commitIndex
		totFol := 1
		majority := len(sm.config.peerIds)/2 + 1
		for i := 0; i < len(sm.config.peerIds); i++ {
			if sm.matchIndex[i] > maxCommitIndex {
				for j := 0; j < len(sm.config.peerIds); j++ {
					if sm.matchIndex[j] >= sm.matchIndex[i] {
						totFol += 1
					}
					if totFol >= majority && sm.matchIndex[i] > maxCommitIndex {
						maxCommitIndex = sm.matchIndex[i]
						break
					}
				}
				totFol = 1
			}
		}
		// Update Commitindex and send commit action to clients
		if maxCommitIndex > sm.commitIndex && sm.log[maxCommitIndex].Term == sm.term {
			for i := sm.commitIndex + 1; i <= maxCommitIndex; i++ {
				actions = append(actions, CommitAc{i, sm.log[i].Data, nil})
			}
			sm.commitIndex = maxCommitIndex
		}
	}
	return actions
}

func (sm *StateMachine) FollowerCandidateAppendEntriesResEH(ev AppendEntriesResEv) []interface{} {
	var actions []interface{}
	if ev.Term > sm.term {
		sm.term = ev.Term
		sm.votedFor = 0
		sm.state = "Follower"
		actions = append(actions, StateStoreAc{sm.term, sm.state, sm.votedFor})
	}
	return actions
}

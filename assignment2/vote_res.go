package main

//import "fmt"

type VoteResEv struct {
	term uint64
	voteGranted bool
}

func (sm *StateMachine) AppendEntriesResEH(ev AppendEntriesResEv) ([]interface{}) {
	var actions []interface{}
	switch sm.state {
		case "Leader":
			return sm.LeaderAppendEntriesResEH(ev)
		case "Follower":
			return sm.FollowerAppendEntriesResEH(ev)
		case "Candidate":
			return sm.CandidateAppendEntriesResEH(ev)
	}
	return actions
}

func (sm *StateMachine) LeaderAppendEntriesResEH(ev AppendEntriesResEv) ([]interface{}) {
	var actions []interface{}
	// Append Entry Failure
	if !ev.success {
		if sm.term < ev.term {
			sm.term = ev.term
			sm.state = "Follower"
		} else {
			// Valid Leader - Mismatch in prevIndex entry
			sm.nextIndex[ev.from]--
			actions = append(actions, SendAc{ev.from, AppendEntriesReqEv{sm.term, sm.config.serverId, sm.nextIndex[ev.from]-1, sm.log[sm.nextIndex[ev.from]-1].term, sm.log[sm.nextIndex[ev.from]:], sm.commitIndex}})
		}
	} else {
		// Update sm.matchIndex[msg.from] to the last replicated index
		sm.nextIndex[ev.from] = uint64(len(sm.log))
		maxCommitIndex := sm.commitIndex
		totFol := 1
		majority := len(sm.config.peerIds)/2 + 1
		for i:=0 ; i<len(sm.config.peerIds); i++ {
			if sm.matchIndex[i] > maxCommitIndex {
				for j:=0; j<len(sm.config.peerIds); j++ { 
					if j!=i && sm.matchIndex[j]>=sm.matchIndex[i] {
						totFol+=1
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
		if maxCommitIndex > sm.commitIndex && sm.log[maxCommitIndex].term == sm.term {
			for i:= sm.commitIndex+1; i<=maxCommitIndex; i++ {
				actions = append(actions, CommitAc{i, sm.log[i].data, nil})
			}
			sm.commitIndex = maxCommitIndex
		}
	}
	return actions
}

func (sm *StateMachine) FollowerAppendEntriesResEH(ev AppendEntriesResEv) ([]interface{}) {
	var actions []interface{}
	if ev.term > sm.term {
		sm.term = ev.term
	}
	return actions
}

func (sm *StateMachine) CandidateAppendEntriesResEH(ev AppendEntriesResEv) ([]interface{}) {
	var actions []interface{}
	if ev.term > sm.term {
		sm.term = ev.term
	}
	return actions
}

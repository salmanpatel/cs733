package main

import "math/rand"

type AppendEntriesReqEv struct {
	term         int64
	leaderId     int64
	prevLogIndex int64
	prevLogTerm  int64
	entries      []LogEntry
	leaderCommit int64
}

func MinInt(a, b int64) int64 {
	if a < b {
		return a
	} else {
		return b
	}
}

func (sm *StateMachine) AppendEntriesReqEH(ev AppendEntriesReqEv) []interface{} {
	var actions []interface{}
	switch sm.state {
	case "Leader":
		actions = sm.LeaderCandidateAppendEntriesReqEH(ev)
	case "Follower":
		actions = sm.FollowerAppendEntriesReqEH(ev)
	case "Candidate":
		actions = sm.LeaderCandidateAppendEntriesReqEH(ev)
	}
	return actions
}

func (sm *StateMachine) FollowerAppendEntriesReqEH(ev AppendEntriesReqEv) []interface{} {
	var actions []interface{}
	if sm.term <= ev.term {
		if sm.term < ev.term {
			sm.votedFor = 0
		}
		sm.term = ev.term
		actions = append(actions, AlarmAc{RandInt(150, 300)})
		actions = append(actions, StateStoreAc{sm.term, sm.state, sm.votedFor})
		if ev.prevLogTerm == 0 {
			sm.log = ev.entries
			for i := 0; i < len(ev.entries); i++ {
				actions = append(actions, LogStoreAc{int64(i), ev.entries[i].term, ev.entries[i].data})
			}
			actions = append(actions, SendAc{ev.leaderId, AppendEntriesResEv{from: sm.config.serverId, term: sm.term, success: true}})
			if ev.leaderCommit > sm.commitIndex {
				newCommitIndex := MinInt(ev.leaderCommit, int64(len(sm.log)-1))
				for i := sm.commitIndex + 1; i <= newCommitIndex; i++ {
					actions = append(actions, CommitAc{i, sm.log[i].data, nil})
				}
				sm.commitIndex = newCommitIndex
			}
		} else if ev.prevLogIndex < int64(len(sm.log)) && sm.log[ev.prevLogIndex].term == ev.prevLogTerm {
			sm.log = sm.log[:ev.prevLogIndex+1]
			sm.log = append(sm.log, ev.entries...)
			for i := 0; i < len(ev.entries); i++ {
				actions = append(actions, LogStoreAc{ev.prevLogIndex + int64(i) + 1, ev.entries[i].term, ev.entries[i].data})
			}
			actions = append(actions, SendAc{ev.leaderId, AppendEntriesResEv{from: sm.config.serverId, term: sm.term, success: true}})
			if ev.leaderCommit > sm.commitIndex {
				newCommitIndex := MinInt(ev.leaderCommit, int64(len(sm.log)-1))
				for i := sm.commitIndex + 1; i <= newCommitIndex; i++ {
					actions = append(actions, CommitAc{i, sm.log[i].data, nil})
				}
				sm.commitIndex = newCommitIndex
			}
		} else {
			actions = append(actions, SendAc{ev.leaderId, AppendEntriesResEv{from: sm.config.serverId, term: sm.term, success: false}})
		}
	} else {
		actions = append(actions, SendAc{ev.leaderId, AppendEntriesResEv{from: sm.config.serverId, term: sm.term, success: false}})
	}
	return actions
}

func (sm *StateMachine) LeaderCandidateAppendEntriesReqEH(ev AppendEntriesReqEv) []interface{} {
	var actions []interface{}
	if sm.term <= ev.term {
		if sm.term < ev.term {
			sm.votedFor = 0
		}
		sm.term = ev.term
		sm.state = "Follower"
		actions = sm.FollowerAppendEntriesReqEH(ev)
	} else {
		actions = append(actions, SendAc{ev.leaderId, AppendEntriesResEv{from: sm.config.serverId, term: sm.term, success: false}})
	}
	return actions
}

func RandInt(min int64, max int64) int64 {
	return min + int64(rand.Int63n(int64(max-min)))
}

package main

import "math/rand"
import "time"

type AppendEntriesReqEv struct {
	Term         int64
	LeaderId     int64
	PrevLogIndex int64
	PrevLogTerm  int64
	Entries      []LogEntry
	LeaderCommit int64
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
	if sm.term <= ev.Term {
		if sm.term < ev.Term {
			sm.votedFor = 0
		}
		sm.term = ev.Term
		actions = append(actions, AlarmAc{RandInt(sm.electionTO)})
		actions = append(actions, StateStoreAc{sm.term, sm.state, sm.votedFor})
		if ev.PrevLogTerm == 0 {
			sm.log = ev.Entries
			for i := 0; i < len(ev.Entries); i++ {
				actions = append(actions, LogStoreAc{int64(i), ev.Entries[i].Term, ev.Entries[i].Data})
			}
			actions = append(actions, SendAc{ev.LeaderId, AppendEntriesResEv{From: sm.config.serverId, Term: sm.term, Success: true}})
			if ev.LeaderCommit > sm.commitIndex {
				newCommitIndex := MinInt(ev.LeaderCommit, int64(len(sm.log)-1))
				for i := sm.commitIndex + 1; i <= newCommitIndex; i++ {
					actions = append(actions, CommitAc{i, sm.log[i].Data, nil})
				}
				sm.commitIndex = newCommitIndex
			}
		} else if ev.PrevLogIndex < int64(len(sm.log)) && sm.log[ev.PrevLogIndex].Term == ev.PrevLogTerm {
			sm.log = sm.log[:ev.PrevLogIndex+1]
			sm.log = append(sm.log, ev.Entries...)
			for i := 0; i < len(ev.Entries); i++ {
				actions = append(actions, LogStoreAc{ev.PrevLogIndex + int64(i) + 1, ev.Entries[i].Term, ev.Entries[i].Data})
			}
			actions = append(actions, SendAc{ev.LeaderId, AppendEntriesResEv{From: sm.config.serverId, Term: sm.term, Success: true}})
			if ev.LeaderCommit > sm.commitIndex {
				newCommitIndex := MinInt(ev.LeaderCommit, int64(len(sm.log)-1))
				for i := sm.commitIndex + 1; i <= newCommitIndex; i++ {
					actions = append(actions, CommitAc{i, sm.log[i].Data, nil})
				}
				sm.commitIndex = newCommitIndex
			}
		} else {
			actions = append(actions, SendAc{ev.LeaderId, AppendEntriesResEv{From: sm.config.serverId, Term: sm.term, Success: false}})
		}
	} else {
		actions = append(actions, SendAc{ev.LeaderId, AppendEntriesResEv{From: sm.config.serverId, Term: sm.term, Success: false}})
	}
	return actions
}

func (sm *StateMachine) LeaderCandidateAppendEntriesReqEH(ev AppendEntriesReqEv) []interface{} {
	var actions []interface{}
	if sm.term <= ev.Term {
		if sm.term < ev.Term {
			sm.votedFor = 0
		}
		sm.term = ev.Term
		sm.state = "Follower"
		actions = sm.FollowerAppendEntriesReqEH(ev)
	} else {
		actions = append(actions, SendAc{ev.LeaderId, AppendEntriesResEv{From: sm.config.serverId, Term: sm.term, Success: false}})
	}
	return actions
}

func RandInt(min int64) int64 {
	rand.Seed(time.Now().UnixNano())
	return min + rand.Int63n(min)
}

package main

//import "time"

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
			i := 0
			for ; i < len(sm.log) && i < len(ev.Entries); i++ {
				if sm.log[i].Term == ev.Entries[i].Term {
					continue
				} else {
					break
				}
			}
			sm.log = sm.log[:i]
			sm.log = append(sm.log, ev.Entries[i:]...)

			for j := i; j < len(ev.Entries); j++ {

				actions = append(actions, LogStoreAc{int64(j), ev.Entries[j].Term, ev.Entries[j].Data})
			}
			actions = append(actions, SendAc{ev.LeaderId, AppendEntriesResEv{From: sm.config.serverId, Term: sm.term, Success: true, ReplicatedLogIndex: int64(len(sm.log) - 1)}})
			if ev.LeaderCommit > sm.commitIndex {
				newCommitIndex := MinInt(ev.LeaderCommit, int64(len(sm.log)-1))
				for i := sm.commitIndex + 1; i <= newCommitIndex; i++ {
					actions = append(actions, CommitAc{i, sm.log[i].Data, nil})
				}
				sm.commitIndex = newCommitIndex
			}
		} else if ev.PrevLogIndex < int64(len(sm.log)) && sm.log[ev.PrevLogIndex].Term == ev.PrevLogTerm {

			// look for an index till log matches by comparing term at each index
			i := 1
			for ; i <= len(ev.Entries) && i+int(ev.PrevLogIndex) < len(sm.log); i++ {
				if sm.log[i+int(ev.PrevLogIndex)].Term == ev.Entries[i-1].Term {
					continue
				}
				break
			}

			sm.log = sm.log[:i+int(ev.PrevLogIndex)]
			sm.log = append(sm.log, ev.Entries[i-1:]...)

			for j := i - 1; j < len(ev.Entries); j++ {
				actions = append(actions, LogStoreAc{int64(j) + ev.PrevLogIndex + 1, ev.Entries[j].Term, ev.Entries[j].Data})
			}

			actions = append(actions, SendAc{ev.LeaderId, AppendEntriesResEv{From: sm.config.serverId, Term: sm.term, Success: true, ReplicatedLogIndex: int64(len(sm.log) - 1)}})
			if ev.LeaderCommit > sm.commitIndex {
				newCommitIndex := MinInt(ev.LeaderCommit, int64(len(sm.log)-1))
				for i := sm.commitIndex + 1; i <= newCommitIndex; i++ {
					actions = append(actions, CommitAc{i, sm.log[i].Data, nil})
				}
				sm.commitIndex = newCommitIndex
			}
		} else {
			actions = append(actions, SendAc{ev.LeaderId, AppendEntriesResEv{From: sm.config.serverId, Term: sm.term, Success: false, ReplicatedLogIndex: int64(len(sm.log) - 1)}})
		}
	} else {
		actions = append(actions, SendAc{ev.LeaderId, AppendEntriesResEv{From: sm.config.serverId, Term: sm.term, Success: false, ReplicatedLogIndex: int64(len(sm.log) - 1)}})
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
		var outstandingCmdAc []interface{}
		if sm.state == "Leader" {
			outstandingCmdAc = sm.HandleOutstandingCmd()
		}
		sm.state = "Follower"
		actions = sm.FollowerAppendEntriesReqEH(ev)
		actions = append(actions, outstandingCmdAc...)
	} else {
		actions = append(actions, SendAc{ev.LeaderId, AppendEntriesResEv{From: sm.config.serverId, Term: sm.term, Success: false, ReplicatedLogIndex: int64(len(sm.log) - 1)}})
	}
	return actions
}

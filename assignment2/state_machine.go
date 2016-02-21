package main

type LogEntry struct {
	term uint64
	data []byte
}

type StateMachine struct {
	config Config
	term uint64
	votedFor uint64
	log []LogEntry
	state string
	commitIndex uint64
	nextIndex []uint64
	matchIndex []uint64
	yesVotes uint64
	noVotes uint64
}

func (sm *StateMachine) ProcessEvent (ev interface{}) []interface{} {
	var actions []interface{}
	switch ev.(type) {
	case AppendEv:
		cmd := ev.(AppendEv)
		actions = sm.AppendEH(cmd)
	case AppendEntriesReqEv:
		cmd := ev.(AppendEntriesReqEv)
		actions = sm.AppendEntriesReqEH(cmd)
	case AppendEntriesResEv:
		cmd := ev.(AppendEntriesResEv)
		actions = sm.AppendEntriesResEH(cmd)
	case TimeoutEv:
		cmd := ev.(TimeoutEv)
		actions = sm.TimeoutEH(cmd)
	case VoteReqEv:
		cmd := ev.(VoteReqEv)
		actions = sm.VoteReqEH(cmd)
	case VoteResEv:
		cmd := ev.(VoteResEv)
		actions = sm.VoteResEH(cmd)
	default: println ("Unrecognized")
	}
	return actions
}

/*
func main() {
	var sm StateMachine
	sm.state = "Leader"
	sm.ProcessEvent(AppendEntriesReqEv{term : 10, prevLogIndex: 100, prevLogTerm: 3})
}
*/

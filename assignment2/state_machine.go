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

func (sm *StateMachine) ProcessEvent (ev interface{}) {
	switch ev.(type) {
	case AppendEntriesReqEv:
		cmd := ev.(AppendEntriesReqEv)
		sm.AppendEntriesReqEH(cmd)
	case AppendEntriesResEv:
		cmd := ev.(AppendEntriesResEv)
		sm.AppendEntriesResEH(cmd)
	case TimeoutEv:
		cmd := ev.(TimeoutEv)
		sm.TimeoutEH(cmd)
//	case VoteReqEv:
//		cmd := ev.(VoteReqEv)
//		fmt.Printf("%v\n", cmd)
	default: println ("Unrecognized")
	}
}

func main() {
	var sm StateMachine
	sm.state = "Leader"
	sm.ProcessEvent(AppendEntriesReqEv{term : 10, prevLogIndex: 100, prevLogTerm: 3})
}

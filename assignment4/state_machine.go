package main

import (
	"errors"
	_ "fmt"
	"math/rand"
)

//import "time"

type LogEntry struct {
	Term int64
	Data []byte
}

type StateMachine struct {
	config      Config
	term        int64
	votedFor    int64
	log         []LogEntry
	state       string
	commitIndex int64
	nextIndex   []int64
	matchIndex  []int64
	yesVotes    int64
	noVotes     int64
	heartbeatTO int64
	electionTO  int64
}

func (sm *StateMachine) ProcessEvent(ev interface{}) []interface{} {
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
	default:
		println("Unrecognized")
	}
	return actions
}

func RandInt(min int64) int64 {
	//rand.Seed(time.Now().UnixNano())
	return min + rand.Int63n(min)
	//	return min
}

func (sm *StateMachine) HandleOutstandingCmd() []interface{} {
	//fmt.Printf("%v outstanding writes from index %v to %v\n", sm.config.serverId, sm.commitIndex+1, len(sm.log))
	var actions []interface{}
	for i := int(sm.commitIndex) + 1; i < len(sm.log); i++ {
		//fmt.Printf("%v redirection for index %v\n",sm.config.serverId, i)
		// actions := append(actions, C)
		actions = append(actions, CommitAc{int64(-1), sm.log[i].Data, errors.New("Not a Leader")})
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

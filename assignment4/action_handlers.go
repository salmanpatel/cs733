package main

import (
	// "fmt"
	"github.com/cs733-iitb/cluster"
	"github.com/cs733-iitb/log"
	//	"reflect"
	"time"
)

// Process Alarm action - by generating timer
func (rn *RaftNode) ProcessAlarmAc(action AlarmAc) {
	// fmt.Printf("%v %v Timer Reset %v \n", rn.Id() ,rn.sm.state, action.time)
	rn.timer.Reset(time.Duration(action.time) * time.Millisecond)
}

func (rn *RaftNode) ProcessSendAc(action SendAc) {
	//if rn.sm.state == "Leader" {
	//		fmt.Printf("%v Sent: %v%v \n", rn.Id(), reflect.TypeOf(action.event), action)
	//}
	switch action.event.(type) {
	case AppendEntriesReqEv:
		rn.nwHandler.Outbox() <- &cluster.Envelope{Pid: int(action.peerId), Msg: action.event.(AppendEntriesReqEv)}
	case AppendEntriesResEv:
		rn.nwHandler.Outbox() <- &cluster.Envelope{Pid: int(action.peerId), Msg: action.event.(AppendEntriesResEv)}
	case VoteReqEv:
		rn.nwHandler.Outbox() <- &cluster.Envelope{Pid: int(action.peerId), Msg: action.event.(VoteReqEv)}
	case VoteResEv:
		rn.nwHandler.Outbox() <- &cluster.Envelope{Pid: int(action.peerId), Msg: action.event.(VoteResEv)}
	default:
		println("Unrecognized Event")
	}
}

func (rn *RaftNode) ProcessCommitAc(action CommitAc) {
	// fmt.Printf("%v ProcessCommitAc \n", rn.Id())
	var ci CommitInfo
	ci.index = action.index
	ci.data = action.data
	ci.err = action.err
	rn.commitCh <- ci
}

func (rn *RaftNode) ProcessLogStoreAc(action LogStoreAc) {
	// fmt.Printf("%v ProcessLogStoreAc \n", rn.Id())
	logFP, err := log.Open(rn.logDir + "/" + LogFile)
	logFP.RegisterSampleEntry(LogEntry{})
	assert(err == nil)
	defer logFP.Close()
	assert(int64(logFP.GetLastIndex()+1) >= action.index)
	logFP.TruncateToEnd(int64(action.index))
	logFP.Append(LogEntry{action.term, action.data})
}

func (rn *RaftNode) ProcessStateStoreAc(action StateStoreAc) {
	// fmt.Printf("%v ProcessStateStoreAc \n", rn.Id())
	stateAttrsFP, err := log.Open(rn.logDir + "/" + StateFile)
	stateAttrsFP.RegisterSampleEntry(PersistentStateAttrs{})
	assert(err == nil)
	defer stateAttrsFP.Close()
	stateAttrsFP.TruncateToEnd(0) // Flush previous state
	stateAttrsFP.Append(PersistentStateAttrs{action.term, action.state, action.votedFor})
}

func assert(val bool) {
	if !val {
		panic("Assertion Failed")
	}
}

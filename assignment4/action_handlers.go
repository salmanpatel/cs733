package main

import (
	"fmt"
	"github.com/cs733-iitb/cluster"
	"github.com/cs733-iitb/log"
	"os"
	//	"reflect"
	"bufio"
	"strconv"
	"time"
	//"log"
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
	//	fmt.Printf("%v ProcessLogStoreAc \n", rn.Id())
	//fmt.Printf("log store start: %v\n",time.Now())
	logFP, err := log.Open(rn.logDir + "/" + LogFile)
	logFP.RegisterSampleEntry(LogEntry{})
	assert(err == nil)
	defer logFP.Close()
	assert(int64(logFP.GetLastIndex()+1) >= action.index)
	logFP.TruncateToEnd(int64(action.index))
	logFP.Append(LogEntry{action.term, action.data})
	//fmt.Printf("log store end: %v\n",time.Now())
}

func (rn *RaftNode) ProcessStateStoreAc(action StateStoreAc) {
	// fmt.Printf("%v ProcessStateStoreAc \n", rn.Id())

	/* Old state store handling
	stateAttrsFP, err := log.Open(rn.logDir + "/" + StateFile)
	stateAttrsFP.RegisterSampleEntry(PersistentStateAttrs{})
	assert(err == nil)
	defer stateAttrsFP.Close()
	stateAttrsFP.TruncateToEnd(0) // Flush previous state
	stateAttrsFP.Append(PersistentStateAttrs{action.term, action.state, action.votedFor})
	*/
	//fmt.Printf("start of state store %v \n", time.Now())
	f, err := os.OpenFile("state"+rn.logDir[3:], os.O_WRONLY, 0666)
	checkErr(err, "opening state file in state action handler")
	defer f.Close()
	b := bufio.NewWriter(f)
	defer func() {
		if err = b.Flush(); err != nil {
			//log.Fatal(err)
			fmt.Println("Error: ProcessStateStoreAc - flush()")
			os.Exit(1)
		}
	}()
	_, err = fmt.Fprintf(b, "%s %s %s\n", strconv.FormatInt(action.term, 10), action.state, strconv.FormatInt(action.votedFor, 10))
	checkErr(err, "writing to state file in state action handler")
	//fmt.Printf("start of state store %v \n", time.Now())
}

func assert(val bool) {
	if !val {
		fmt.Println("Assertion Failed")
		os.Exit(1)
	}
}

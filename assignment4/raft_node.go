package main

import (
	"encoding/gob"
	"errors"
	"fmt"
	"github.com/cs733-iitb/cluster"
	"github.com/cs733-iitb/log"
	"os"
	//	"reflect"
	"bufio"
	"math/rand"
	"strconv"
	"time"
)

const LogFile = "log"
const StateFile = "state"

// Raft Node Structure
type RaftNode struct { // implements Node interface
	sm      StateMachine
	eventCh chan interface{}
	// timeoutCh chan bool
	commitCh    chan CommitInfo
	shutdownSig chan bool
	nwHandler   cluster.Server
	timer       *time.Timer
	logDir      string
	stateFP     *os.File
	logFP       *log.Log
}

type NetConfig struct {
	id   int64
	host string
	port int
}

// Raft Node Configuration
type RaftNodeConfig struct {
	cluster     []NetConfig // Information about all servers, including this.
	id          int64       // this node's id. One of the cluster's entries should match.
	logDir      string      // Log file directory for this node
	electionTO  int64
	heartbeatTO int64
}

type CommitInfo struct {
	index int64
	data  []byte
	err   error
}

type PersistentStateAttrs struct {
	Term     int64
	State    string
	VotedFor int64
}

func initRaftNode(id int64, peers []NetConfig, jsonFile string) RaftNode {
	// peers := prepareRaftNodeConfigObj()
	//initRaftStateFile("PersistentData_" + strconv.Itoa((i+1)*100))
	rn := New(RaftNodeConfig{peers, id, "dir" + strconv.FormatInt(id, 10), 6000, 500}, jsonFile)
	return rn
}

func New(rnConfig RaftNodeConfig, jsonFile string) RaftNode {
	var rn RaftNode
	rn.eventCh = make(chan interface{}, 100000)
	rn.shutdownSig = make(chan bool)
	rn.commitCh = make(chan CommitInfo, 5000)
	rn.logDir = rnConfig.logDir

	// open log file and initialize logfp variable
	var err error
	rn.logFP, err = log.Open(rnConfig.logDir + "/" + LogFile)
	rn.logFP.RegisterSampleEntry(LogEntry{})
	assert(err == nil)

	rn.initializeStateMachine(rnConfig)

	rn.nwHandler, err = cluster.New(int(rnConfig.id), jsonFile)
	assert(err == nil)

	// Register various types to be send on outbox and receive in inbox
	gob.Register(VoteReqEv{})
	gob.Register(VoteResEv{})
	gob.Register(AppendEntriesReqEv{})
	gob.Register(AppendEntriesResEv{})

	// Set initial election timeout
	/*go func() {
		time.Sleep(time.Millisecond * time.Duration(RandInt(rn.sm.electionTO)))
		rn.timeoutCh <- true
	}()*/
	rn.timer = time.NewTimer(time.Duration(RandInt(rnConfig.electionTO)) * time.Millisecond)
	rand.Seed(time.Now().UnixNano())

	return rn
}

// A channel for client to listen on. What goes into Append must come out of here at some point.
func (rn *RaftNode) CommitChannel() <-chan CommitInfo {
	return rn.commitCh
}

// Last known committed index in the log. This could be -1 until the system stabilizes.
func (rn *RaftNode) CommittedIndex() int64 {
	return rn.sm.commitIndex
}

// Returns the data at a log index, or an error.
func (rn *RaftNode) Get(index int) (error, []byte) {
	if index >= len(rn.sm.log) {
		return errors.New("Invalid Index"), nil
	}
	return nil, rn.sm.log[index].Data
}

// Node's id
func (rn *RaftNode) Id() int64 {
	return rn.sm.config.serverId
}

// Id of leader. -1 if unknown
func (rn *RaftNode) LeaderId() int64 {
	return rn.sm.votedFor
}

// Signal to shut down all goroutines, stop sockets, flush log and close it, cancel timers.
func (rn *RaftNode) Shutdown() {
	rn.shutdownSig <- true
	rn.nwHandler.Close()
	//defer rn.Close()
}

func (rn *RaftNode) initializeLog(rnConfig RaftNodeConfig) int64 {
	//logFP, err := log.Open(rnConfig.logDir + "/" + LogFile)
	//logFP.RegisterSampleEntry(LogEntry{})
	//assert(err == nil)
	//defer logFP.Close()
	totLogEntrs := rn.logFP.GetLastIndex() // should return 1
	if totLogEntrs > 0 {
		for j := 0; j < int(totLogEntrs); j++ {
			res, err := rn.logFP.Get(int64(j))
			assert(err == nil)
			logEntry, ok := res.(LogEntry)
			assert(ok)
			rn.sm.log = append(rn.sm.log, logEntry)
		}
	}
	return totLogEntrs
}

func (rn *RaftNode) initializeStateMachine(rnConfig RaftNodeConfig) {
	totLogEntrs := rn.initializeLog(rnConfig)
	rn.sm.commitIndex = -1
	rn.sm.config.serverId = rnConfig.id
	// fmt.Printf("length of cluseter = %v \n", len(rnConfig.cluster))
	for _, nodeConfig := range rnConfig.cluster {
		if nodeConfig.id != rnConfig.id {
			rn.sm.config.peerIds = append(rn.sm.config.peerIds, nodeConfig.id)
			rn.sm.nextIndex = append(rn.sm.nextIndex, int64(totLogEntrs))
			rn.sm.matchIndex = append(rn.sm.matchIndex, 0)
		}
	}
	rn.sm.yesVotes = 0
	rn.sm.noVotes = 0
	rn.sm.electionTO = rnConfig.electionTO
	rn.sm.heartbeatTO = rnConfig.heartbeatTO
	// State preserving file does not exist
	/*	if _, err := os.Stat(rnConfig.logDir + "/" + StateFile); os.IsNotExist(err) {
		//		rmlog(rnConfig.logDir)
		rn.sm.state = "Follower"
		rn.sm.term = 0
		rn.sm.votedFor = 0
	} else {*/
	// read from a file
	/* old
	stateAttrsFP, err := log.Open(rnConfig.logDir + "/" + StateFile)
	stateAttrsFP.RegisterSampleEntry(PersistentStateAttrs{})
	assert(err == nil)
	defer stateAttrsFP.Close()
	i := stateAttrsFP.GetLastIndex() // should return 0
	// fmt.Printf("initializeStateMachine: last index = %v\n", i)
	assert(i == 0)
	res, err := stateAttrsFP.Get(0)
	fmt.Println("reading state file")
	assert(err == nil)
	stateAttrs, ok := res.(PersistentStateAttrs)
	assert(ok)
	*/
	f, err := os.OpenFile("state"+rnConfig.logDir[3:], os.O_RDWR|os.O_CREATE, 0666)
	checkErr(err, "opening state file : initializeStateMachine")
	rn.stateFP = f

	//defer f.Close()
	b := bufio.NewReader(f)
	var stateStr string
	var termStr string
	var vfStr string
	_, err = fmt.Fscanf(b, "%s %s %s\n", &termStr, &stateStr, &vfStr)
	//fmt.Println(err)
	checkErr(err, "reading to state file : initializeStateMachine")

	/* old
	rn.sm.state = stateAttrs.State
	rn.sm.term = stateAttrs.Term
	rn.sm.votedFor = stateAttrs.VotedFor
	*/

	rn.sm.state = stateStr
	rn.sm.term, err = strconv.ParseInt(termStr, 10, 64)
	checkErr(err, "initializeStateMachine: string to int conversion")
	rn.sm.votedFor, err = strconv.ParseInt(vfStr, 10, 64)
	checkErr(err, "initializeStateMachine: string to int conversion")
}

// Process Append request from client
func (rn *RaftNode) Append(data []byte) {
	// fmt.Println("Append Called")
	rn.eventCh <- AppendEv{data}
	// fmt.Println("appended to channel")
}

// Process all Events on State Machine
func (rn *RaftNode) processEvents() {
	for {
		var ev interface{}
		select {
		case <-rn.shutdownSig:
			{
				return
			}
		case ev = <-rn.eventCh:
		case <-rn.timer.C:
			//if rn.sm.state == "Follower" {
			//	fmt.Printf("%v %v Timeout\n", rn.Id(), rn.sm.state)
			//}
			ev = TimeoutEv{}
		case inboxEv := <-rn.nwHandler.Inbox():
			//if rn.sm.state == "Leader" {
			//	fmt.Printf("%v Received: %v%v \n", rn.Id(), reflect.TypeOf(inboxEv.Msg), inboxEv.Msg)
			//}
			switch inboxEv.Msg.(type) {
			case AppendEntriesReqEv:
				//if rn.sm.state == "Follower" && rn.Id()==200 {
				//	fmt.Println(inboxEv.Msg.(AppendEntriesReqEv))
				//}
				rn.eventCh <- inboxEv.Msg.(AppendEntriesReqEv)
			case AppendEntriesResEv:
				rn.eventCh <- inboxEv.Msg.(AppendEntriesResEv)
			case VoteReqEv:
				rn.eventCh <- inboxEv.Msg.(VoteReqEv)
			case VoteResEv:
				rn.eventCh <- inboxEv.Msg.(VoteResEv)
			}
			continue
		}
		actions := rn.sm.ProcessEvent(ev)
		// fmt.Printf("%v event: %v, actions: %v \n", rn.Id(), reflect.TypeOf(ev),actions)
		rn.doActions(actions)
	}
}

// Process all Actions generated due to processing of an event
func (rn *RaftNode) doActions(actions []interface{}) {
	// fmt.Printf("%v actions called %v \n", rn.Id(), actions)
	for _, action := range actions {
		switch action.(type) {
		case AlarmAc:
			cmd := action.(AlarmAc)
			go rn.ProcessAlarmAc(cmd)
		case SendAc:
			cmd := action.(SendAc)
			rn.ProcessSendAc(cmd)
		case CommitAc:
			cmd := action.(CommitAc)
			rn.ProcessCommitAc(cmd)
		case LogStoreAc:
			cmd := action.(LogStoreAc)
			rn.ProcessLogStoreAc(cmd)
		case StateStoreAc:
			cmd := action.(StateStoreAc)
			rn.ProcessStateStoreAc(cmd)
		default:
			println("ERROR : Invalid Action Type")
		}
	}
}

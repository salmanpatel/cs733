package main

import (
	"encoding/gob"
	"github.com/cs733-iitb/cluster"
	"github.com/cs733-iitb/log"
	"os"
	"time"
)

const logFile = "log"
const stateFile = "state"

// Raft Node Structure
type RaftNode struct { // implements Node interface
	sm        StateMachine
	eventCh   chan interface{}
	timeoutCh chan bool
	commitCh  chan CommitInfo
	nwHandler cluster.Server
	parTOs    int
}

type NetConfig struct {
	id   uint64
	host string
	port int
}

// Raft Node Configuration
type RaftNodeConfig struct {
	cluster     []NetConfig // Information about all servers, including this.
	id          uint64      // this node's id. One of the cluster's entries should match.
	logDir      string      // Log file directory for this node
	electionTO  int
	heartbeatTO int
}

type CommitInfo struct {
	index uint64
	data  []byte
	err   error
}

type PersistentStateAttrs struct {
	term     uint64
	state    string
	votedFor uint64
}

func New(rnConfig RaftNodeConfig, jsonFile string) RaftNode {
	var rn RaftNode
	rn.eventCh = make(chan interface{}, 100)
	rn.timeoutCh = make(chan bool)
	rn.commitCh = make(chan CommitInfo, 100)
	rn.parTOs = 0

	rn.initializeStateMachine(rnConfig)

	var err error
	rn.nwHandler, err = cluster.New(int(rnConfig.id), jsonFile)
	assert(err == nil)

	// Register various types to be send on outbox and receive in inbox
	gob.Register(VoteReqEv{})
	gob.Register(VoteResEv{})
	gob.Register(AppendEntriesReqEv{})
	gob.Register(AppendEntriesResEv{})

	return rn
}

func (rn *RaftNode) initializeLog(rnConfig RaftNodeConfig) int64 {
	logFP, err := log.Open(rnConfig.logDir + "/" + logFile)
	logFP.RegisterSampleEntry(LogEntry{})
	assert(err == nil)
	defer logFP.Close()
	totLogEntrs := logFP.GetLastIndex() // should return 1
	if totLogEntrs > 0 {
		for j := 0; j < int(totLogEntrs); j++ {
			res, err := logFP.Get(int64(j))
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
	rn.sm.commitIndex = 0
	rn.sm.config.serverId = rnConfig.id
	for index, nodeConfig := range rnConfig.cluster {
		if nodeConfig.id != rnConfig.id {
			rn.sm.config.peerIds = append(rn.sm.config.peerIds, nodeConfig.id)
			rn.sm.nextIndex[index] = uint64(totLogEntrs)
			rn.sm.matchIndex[index] = 0
		}
	}
	rn.sm.yesVotes = 0
	rn.sm.noVotes = 0
	// State preserving file does not exist
	if _, err := os.Stat(rnConfig.logDir + "/" + stateFile); os.IsNotExist(err) {
		//		rmlog(rnConfig.logDir)
		rn.sm.state = "Follower"
		rn.sm.term = 0
		rn.sm.votedFor = 0
	} else {
		// read from a file
		stateAttrsFP, err := log.Open(rnConfig.logDir + "/" + stateFile)
		stateAttrsFP.RegisterSampleEntry(PersistentStateAttrs{})
		assert(err == nil)
		defer stateAttrsFP.Close()
		i := stateAttrsFP.GetLastIndex() // should return 1
		assert(i == 1)
		res, err := stateAttrsFP.Get(i)
		assert(err == nil)
		stateAttrs, ok := res.(PersistentStateAttrs)
		assert(ok)
		rn.sm.state = stateAttrs.state
		rn.sm.term = stateAttrs.term
		rn.sm.votedFor = stateAttrs.votedFor
	}
}

// Process Append request from client
func (rn *RaftNode) Append(data []byte) {
	rn.eventCh <- AppendEv{data}
}

// Process all Events on State Machine
func (rn *RaftNode) processEvents(logDir string) {
	for {
		var ev interface{}
		select {
		case ev = <-rn.eventCh:
		case <-rn.timeoutCh:
			ev = TimeoutEv{}
		case inboxEv := <-rn.nwHandler.Inbox():
			switch inboxEv.Msg.(type) {
			case AppendEntriesReqEv:
				rn.eventCh <- inboxEv.Msg.(AppendEntriesReqEv)
			case AppendEntriesResEv:
				rn.eventCh <- inboxEv.Msg.(AppendEntriesResEv)
			case VoteReqEv:
				rn.eventCh <- inboxEv.Msg.(VoteReqEv)
			case VoteResEv:
				rn.eventCh <- inboxEv.Msg.(VoteResEv)
			}
			continue
		default:
			println("Unrecognized Event")
		}
		actions := rn.sm.ProcessEvent(ev)
		rn.doActions(actions, logDir)
	}
}

// Process all Actions generated due to processing of an event
func (rn *RaftNode) doActions(actions []interface{}, logDir string) {
	for _, action := range actions {
		switch action.(type) {
		case AlarmAc:
			rn.parTOs += 1
			cmd := action.(AlarmAc)
			rn.processAlarmAc(cmd)
		case SendAc:
			cmd := action.(SendAc)
			rn.processSendAc(cmd)
		case CommitAc:
			cmd := action.(CommitAc)
			rn.processCommitAc(cmd)
		case LogStoreAc:
			cmd := action.(LogStoreAc)
			rn.processLogStoreAc(cmd, logDir)
		case StateStoreAc:
			cmd := action.(StateStoreAc)
			rn.processStateStoreAc(cmd, logDir)
		default:
			println("ERROR : Invalid Action Type")
		}
	}
}

// Process Alarm action - by generating timer
func (rn *RaftNode) processAlarmAc(action AlarmAc) {
	beforeParTOs := rn.parTOs
	time.Sleep(time.Millisecond * time.Duration(action.time))
	// No timer reset
	if beforeParTOs == rn.parTOs {
		rn.timeoutCh <- true
	}
}

func (rn *RaftNode) processSendAc(action SendAc) {
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

func (rn *RaftNode) processCommitAc(action CommitAc) {
	var ci CommitInfo
	ci.index = action.index
	ci.data = action.data
	ci.err = action.err
	rn.commitCh <- ci
}

func (rn *RaftNode) processLogStoreAc(action LogStoreAc, logDir string) {
	logFP, err := log.Open(logDir + "/" + logFile)
	logFP.RegisterSampleEntry(LogEntry{})
	assert(err == nil)
	defer logFP.Close()
	assert(uint64(logFP.GetLastIndex()+1) >= action.index)
	logFP.TruncateToEnd(int64(action.index))
	logFP.Append(LogEntry{action.term, action.data})
}

func (rn *RaftNode) processStateStoreAc(action StateStoreAc, logDir string) {
	stateAttrsFP, err := log.Open(logDir + "/" + stateFile)
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

package main

import "time"

// Raft Node Structure
type RaftNode struct { // implements Node interface
	sm        StateMachine
	eventCh   chan interface{}
	timeoutCh chan bool
	commitCh  chan CommitInfo
}

type CommitInfo struct {
	data  []byte
	index int64
	err   error
}

func New(serverId int, jsonFile string) RaftNode {
	var sm StateMachine
	sm.state = "Follower"
}

// Process Append request from client
func (rn *RaftNode) Append(data []byte) {
	rn.eventCh <- AppendEv{data}
}

// Process all Events on State Machine
func (rn *RaftNode) processEvents() {
	for {
		var ev interface{}
		select {
		case ev = <-rn.eventCh:
		case <-rn.timeoutCh:
			{
				ev = TimeoutEv{}
			}
		}
		actions := rn.sm.ProcessEvent(ev)
		rn.doActions(actions)
	}
}

// Process all Actions generated due to processing of an event
func (rn *RaftNode) doActions(actions []interface{}) {
	for _, action := range actions {
		switch action.(type) {
		case AlarmAc:
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
			rn.processLogStoreAc(cmd)
		case StateStoreAc:
			cmd := action.(StateStoreAc)
			rn.processStateStoreAc(cmd)
		default:
			println("ERROR : Invalid Action Type")
		}
	}
}

// Process Alarm action - by generating timer
func (rn *RaftNode) processAlarmAc(action AlarmAc) {
	time.Sleep(time.Millisecond * time.Duration(action.time))
	rn.timeoutCh <- true
}

func (rn *RaftNode) processSendAc(action SendAc) {

}

func (rn *RaftNode) processCommitAc(action CommitAc) {
	rn.commitCh <- CommitInfo(action)
}

func (rn *RaftNode) processLogStoreAc(action LogStoreAc) {

}

func (rn *RaftNode) processStateStoreAc(action StateStoreAc) {

}

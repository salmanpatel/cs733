package main

import (
	"errors"
	"fmt"
	"reflect"
	"testing"
)

/*
func expectSM(t *testing.T, actualSM *StateMachine, expectedSM *StateMachine, errStr string) {
	if !reflect.DeepEqual(actualSM, expectedSM) {
		t.Error(fmt.Sprintf("Failed : %v\n",errStr))
	}
}
*/

func expectSM(t *testing.T, responsesm *StateMachine, expectedsm *StateMachine, errstr string) {
	ok := true
	if responsesm.state != expectedsm.state {
		ok = false
		errstr += fmt.Sprintf("State mismatch\n")
	}
	if responsesm.term != expectedsm.term {
		ok = false
		errstr += fmt.Sprintf("Term mismatch\n")
	}
	if responsesm.votedFor != expectedsm.votedFor {
		ok = false
		errstr += fmt.Sprintf("VotedFor mismatch\n")
	}
	if !reflect.DeepEqual(responsesm.log, expectedsm.log) {
		ok = false
		errstr += fmt.Sprintf("Log mismatch\n")
	}
	if responsesm.commitIndex != expectedsm.commitIndex {
		ok = false
		errstr += fmt.Sprintf("logCommitIndex mismatch\n")
	}
	if !reflect.DeepEqual(responsesm.nextIndex, expectedsm.nextIndex) {
		ok = false
		errstr += fmt.Sprintf("NextIndex mismatch\n")
	}
	if !reflect.DeepEqual(responsesm.matchIndex, expectedsm.matchIndex) {
		ok = false
		errstr += fmt.Sprintf("MatchIndex mismatch\n")
	}
	if responsesm.yesVotes != expectedsm.yesVotes {
		ok = false
		errstr += fmt.Sprintf("yesVotesNum mismatch\n")
	}
	if responsesm.noVotes != expectedsm.noVotes {
		ok = false
		errstr += fmt.Sprintf("noVotesNum mismatch\n")
	}
	if !ok {
		t.Fatal(errstr)
	}
}

func expectActions(t *testing.T, actualAc []interface{}, expectedAc []interface{}, errStr string) {
	errStr = "Failed : " + errStr + " "
	if len(actualAc) != len(expectedAc) {
		t.Fatal(fmt.Sprintf("%v :Length Mismatch: Expected - %v , Actual - %v\n", errStr, len(expectedAc), len(actualAc)))
	}
	expMap := make(map[reflect.Type]int)
	actMap := make(map[reflect.Type]int)
	for _, val := range actualAc {
		actMap[reflect.TypeOf(val)] += 1
	}
	for _, val := range expectedAc {
		expMap[reflect.TypeOf(val)] += 1
	}
	if !reflect.DeepEqual(expMap, actMap) {
		fmt.Printf("%v\n%v\n", actMap, expMap)
		t.Fatal(fmt.Sprintf("%v :Actions Mismatch in Map:\n", errStr))
	}
	if !actionsEquality(actualAc, expectedAc) || !actionsEquality(expectedAc, actualAc) {
		t.Fatal(fmt.Sprintf("%v :Actions Mismatch 2:\n", errStr))
	}
}

func actionsEquality(actualAc []interface{}, expectedAc []interface{}) bool {
	flag := false
	for _, actVal := range actualAc {
		if reflect.TypeOf(actVal) == reflect.TypeOf(AlarmAc{}) {
			continue
		}
		for _, expVal := range expectedAc {
			if reflect.TypeOf(actVal) == reflect.TypeOf(expVal) && reflect.DeepEqual(actVal, expVal) {
				flag = true
				break
			}
		}
		if !flag {
			fmt.Printf("%v\n", actVal)
			return false
		}
		flag = false
	}
	return true
}

func TestFollowerTO(t *testing.T) {
	sm := &StateMachine{term: 3, state: "Follower", config: Config{999, []uint64{995, 996, 997, 998}}, votedFor: 3, log: []LogEntry{LogEntry{1, []byte{'a', 'b', 'c'}}}}
	outputAc := sm.ProcessEvent(TimeoutEv{})
	expectSM(t, sm, &StateMachine{state: "Candidate", term: 4, log: []LogEntry{LogEntry{1, []byte{'a', 'b', 'c'}}}, votedFor: 999, config: Config{999, []uint64{995, 996, 997, 998}}, yesVotes: 1}, "Follower TO")
	expectActions(t, outputAc, []interface{}{StateStoreAc{4, "Candidate", 999}, AlarmAc{150}, SendAc{995, VoteReqEv{4, 999, 0, 1}}, SendAc{996, VoteReqEv{4, 999, 0, 1}}, SendAc{997, VoteReqEv{4, 999, 0, 1}}, SendAc{998, VoteReqEv{4, 999, 0, 1}}}, "Follower TO")
}

func TestLeaderTO(t *testing.T) {
	sm := &StateMachine{term: 3, state: "Leader", config: Config{999, []uint64{995, 996, 997, 998}}, nextIndex: []uint64{1, 3, 2, 1}, votedFor: 3, log: []LogEntry{LogEntry{1, []byte("abc")}, LogEntry{2, []byte("def")}, LogEntry{2, []byte("ghijk")}, LogEntry{3, []byte("lm")}}, commitIndex: 1}
	outputAc := sm.ProcessEvent(TimeoutEv{})
	expectSM(t, sm, &StateMachine{term: 3, state: "Leader", config: Config{999, []uint64{995, 996, 997, 998}}, nextIndex: []uint64{1, 3, 2, 1}, votedFor: 3, log: []LogEntry{LogEntry{1, []byte("abc")}, LogEntry{2, []byte("def")}, LogEntry{2, []byte("ghijk")}, LogEntry{3, []byte("lm")}}, commitIndex: 1}, "Leader TO")
	expectActions(t, outputAc, []interface{}{AlarmAc{150}, SendAc{995, AppendEntriesReqEv{3, 999, 0, 1, []LogEntry{LogEntry{2, []byte("def")}, LogEntry{2, []byte("ghijk")}, LogEntry{3, []byte("lm")}}, 1}}, SendAc{996, AppendEntriesReqEv{3, 999, 2, 2, []LogEntry{LogEntry{3, []byte("lm")}}, 1}}, SendAc{997, AppendEntriesReqEv{3, 999, 1, 2, []LogEntry{LogEntry{2, []byte("ghijk")}, LogEntry{3, []byte("lm")}}, 1}}, SendAc{998, AppendEntriesReqEv{3, 999, 0, 1, []LogEntry{LogEntry{2, []byte("def")}, LogEntry{2, []byte("ghijk")}, LogEntry{3, []byte("lm")}}, 1}}}, "Leader TO")
}

func TestLeaderAppend(t *testing.T) {
	sm := &StateMachine{state: "Leader", term: 3, log: []LogEntry{LogEntry{1, []byte("abc")}, LogEntry{2, []byte("def")}, LogEntry{2, []byte("ghijk")}}, config: Config{999, []uint64{995, 996, 997, 998}}, nextIndex: []uint64{1, 3, 2, 1}, commitIndex: 0}
	outputAc := sm.ProcessEvent(AppendEv{[]byte("lm")})
	expectSM(t, sm, &StateMachine{state: "Leader", log: []LogEntry{LogEntry{1, []byte("abc")}, LogEntry{2, []byte("def")}, LogEntry{2, []byte("ghijk")}, LogEntry{3, []byte("lm")}}, term: 3, config: Config{999, []uint64{995, 996, 997, 998}}, nextIndex: []uint64{1, 3, 2, 1}, commitIndex: 0}, "Leader Append")
	expectActions(t, outputAc, []interface{}{LogStoreAc{3, 3, []byte("lm")}, SendAc{995, AppendEntriesReqEv{3, 999, 0, 1, []LogEntry{LogEntry{2, []byte("def")}, LogEntry{2, []byte("ghijk")}, LogEntry{3, []byte("lm")}}, 0}}, SendAc{996, AppendEntriesReqEv{3, 999, 2, 2, []LogEntry{LogEntry{3, []byte("lm")}}, 0}}, SendAc{997, AppendEntriesReqEv{3, 999, 1, 2, []LogEntry{LogEntry{2, []byte("ghijk")}, LogEntry{3, []byte("lm")}}, 0}}, SendAc{998, AppendEntriesReqEv{3, 999, 0, 1, []LogEntry{LogEntry{2, []byte("def")}, LogEntry{2, []byte("ghijk")}, LogEntry{3, []byte("lm")}}, 0}}}, "Follower TO")
}

func TestFollowerAppend(t *testing.T) {
	sm := &StateMachine{state: "Follower", log: []LogEntry{LogEntry{1, []byte("abc")}, LogEntry{2, []byte("def")}, LogEntry{2, []byte("ghijk")}, LogEntry{3, []byte("lm")}}}
	outputAc := sm.ProcessEvent(AppendEv{[]byte("nop")})
	expectSM(t, sm, &StateMachine{state: "Follower", log: []LogEntry{LogEntry{1, []byte("abc")}, LogEntry{2, []byte("def")}, LogEntry{2, []byte("ghijk")}, LogEntry{3, []byte("lm")}}}, "Follower Append")
	expectActions(t, outputAc, []interface{}{CommitAc{3, []byte("nop"), errors.New("Not a Leader")}}, "Follower TO")
}

func TestFollowerAppendEntriesReqSuccess(t *testing.T) {
	sm := &StateMachine{state: "Follower", term: 4, votedFor: 995, log: []LogEntry{LogEntry{1, []byte("abc")}, LogEntry{2, []byte("def")}, LogEntry{2, []byte("ghijk")}}, config: Config{995, []uint64{999, 996, 997, 998}}, commitIndex: 2}
	outputAc := sm.ProcessEvent(AppendEntriesReqEv{5, 999, 1, 2, []LogEntry{LogEntry{3, []byte("lm")}, LogEntry{4, []byte("four")}}, 3})
	expectSM(t, sm, &StateMachine{state: "Follower", term: 5, votedFor: 0, log: []LogEntry{LogEntry{1, []byte("abc")}, LogEntry{2, []byte("def")}, LogEntry{3, []byte("lm")}, LogEntry{4, []byte("four")}}, config: Config{995, []uint64{999, 996, 997, 998}}, commitIndex: 3}, "Follower AppendEntriesReq with Success response")
	expectActions(t, outputAc, []interface{}{AlarmAc{150}, CommitAc{3, []byte("four"), nil}, StateStoreAc{5, "Follower", 0}, LogStoreAc{2, 3, []byte("lm")}, LogStoreAc{3, 4, []byte("four")}, SendAc{999, AppendEntriesResEv{995, 5, true}}}, "Follower AppendEntriesReq with Success response")
}

func TestFollowerAppendEntriesReqLogMismatch(t *testing.T) {
	sm := &StateMachine{state: "Follower", term: 4, votedFor: 995, log: []LogEntry{LogEntry{1, []byte("abc")}, LogEntry{1, []byte("def")}, LogEntry{2, []byte("ghijk")}}, config: Config{995, []uint64{999, 996, 997, 998}}, commitIndex: 0}
	outputAc := sm.ProcessEvent(AppendEntriesReqEv{5, 999, 1, 2, []LogEntry{LogEntry{3, []byte("lm")}, LogEntry{4, []byte("four")}}, 3})
	expectSM(t, sm, &StateMachine{state: "Follower", term: 5, votedFor: 0, log: []LogEntry{LogEntry{1, []byte("abc")}, LogEntry{1, []byte("def")}, LogEntry{2, []byte("ghijk")}}, config: Config{995, []uint64{999, 996, 997, 998}}, commitIndex: 0}, "Follower AppendEntriesReq - Log Mismatch")
	expectActions(t, outputAc, []interface{}{AlarmAc{150}, StateStoreAc{5, "Follower", 0}, SendAc{999, AppendEntriesResEv{995, 5, false}}}, "Follower AppendEntriesReq - Log Mismatch")
}

func TestFollowerAppendEntriesReqFromLowerTerm(t *testing.T) {
	sm := &StateMachine{state: "Follower", term: 4, votedFor: 995, log: []LogEntry{LogEntry{1, []byte("abc")}, LogEntry{2, []byte("def")}, LogEntry{2, []byte("ghijk")}}, config: Config{995, []uint64{999, 996, 997, 998}}, commitIndex: 2}
	outputAc := sm.ProcessEvent(AppendEntriesReqEv{3, 999, 1, 2, []LogEntry{LogEntry{3, []byte("lm")}, LogEntry{4, []byte("four")}}, 3})
	expectSM(t, sm, &StateMachine{state: "Follower", term: 4, votedFor: 995, log: []LogEntry{LogEntry{1, []byte("abc")}, LogEntry{2, []byte("def")}, LogEntry{2, []byte("ghijk")}}, config: Config{995, []uint64{999, 996, 997, 998}}, commitIndex: 2}, "Follower AppendEntriesReq - From Lower Term")
	expectActions(t, outputAc, []interface{}{SendAc{999, AppendEntriesResEv{995, 4, false}}}, "Follower AppendEntriesReq - From Lower Term")
}

func TestFollowerAppendEntriesReqEmptyLog(t *testing.T) {
	sm := &StateMachine{state: "Follower", term: 4, votedFor: 995, log: []LogEntry{LogEntry{1, []byte("abc")}, LogEntry{2, []byte("def")}, LogEntry{2, []byte("ghijk")}}, config: Config{995, []uint64{999, 996, 997, 998}}, commitIndex: 0}
	outputAc := sm.ProcessEvent(AppendEntriesReqEv{5, 999, 0, 0, []LogEntry{LogEntry{3, []byte("lm")}, LogEntry{4, []byte("four")}}, 1})
	expectSM(t, sm, &StateMachine{state: "Follower", term: 5, votedFor: 0, log: []LogEntry{LogEntry{3, []byte("lm")}, LogEntry{4, []byte("four")}}, config: Config{995, []uint64{999, 996, 997, 998}}, commitIndex: 1}, "Follower AppendEntriesReq - Empty Log")
	expectActions(t, outputAc, []interface{}{AlarmAc{150}, CommitAc{1, []byte("four"), nil}, StateStoreAc{5, "Follower", 0}, LogStoreAc{0, 3, []byte("lm")}, LogStoreAc{1, 4, []byte("four")}, SendAc{999, AppendEntriesResEv{995, 5, true}}}, "Follower AppendEntriesReq - Empty Log")
}

func TestLeaderAppendEntriesReqSuccess(t *testing.T) {
	sm := &StateMachine{state: "Leader", term: 4, votedFor: 995, log: []LogEntry{LogEntry{1, []byte("abc")}, LogEntry{2, []byte("def")}, LogEntry{2, []byte("ghijk")}}, config: Config{995, []uint64{999, 996, 997, 998}}, commitIndex: 2}
	outputAc := sm.ProcessEvent(AppendEntriesReqEv{5, 999, 1, 2, []LogEntry{LogEntry{3, []byte("lm")}, LogEntry{4, []byte("four")}}, 3})
	expectSM(t, sm, &StateMachine{state: "Follower", term: 5, votedFor: 0, log: []LogEntry{LogEntry{1, []byte("abc")}, LogEntry{2, []byte("def")}, LogEntry{3, []byte("lm")}, LogEntry{4, []byte("four")}}, config: Config{995, []uint64{999, 996, 997, 998}}, commitIndex: 3}, "Leader AppendEntriesReq with Success response")
	expectActions(t, outputAc, []interface{}{AlarmAc{150}, CommitAc{3, []byte("four"), nil}, StateStoreAc{5, "Follower", 0}, LogStoreAc{2, 3, []byte("lm")}, LogStoreAc{3, 4, []byte("four")}, SendAc{999, AppendEntriesResEv{995, 5, true}}}, "Leader AppendEntriesReq with Success response")
}

func TestCandidateAppendEntriesReqFromLowerTerm(t *testing.T) {
	sm := &StateMachine{state: "Candidate", term: 4, votedFor: 995, log: []LogEntry{LogEntry{1, []byte("abc")}, LogEntry{2, []byte("def")}, LogEntry{2, []byte("ghijk")}}, config: Config{995, []uint64{999, 996, 997, 998}}, commitIndex: 2}
	outputAc := sm.ProcessEvent(AppendEntriesReqEv{3, 999, 1, 2, []LogEntry{LogEntry{3, []byte("lm")}, LogEntry{4, []byte("four")}}, 3})
	expectSM(t, sm, &StateMachine{state: "Candidate", term: 4, votedFor: 995, log: []LogEntry{LogEntry{1, []byte("abc")}, LogEntry{2, []byte("def")}, LogEntry{2, []byte("ghijk")}}, config: Config{995, []uint64{999, 996, 997, 998}}, commitIndex: 2}, "Candidate AppendEntriesReq - From Lower Term")
	expectActions(t, outputAc, []interface{}{SendAc{999, AppendEntriesResEv{995, 4, false}}}, "Candidate AppendEntriesReq - From Lower Term")
}

func TestLeaderAppendEntriesResFailure(t *testing.T) {
	sm := &StateMachine{state: "Leader", term: 4, config: Config{999, []uint64{995, 996, 997, 998}}}
	outputAc := sm.ProcessEvent(AppendEntriesResEv{995, 5, false})
	expectSM(t, sm, &StateMachine{state: "Follower", term: 5, votedFor: 0, config: Config{995, []uint64{999, 996, 997, 998}}}, "Leader AppendEntriesRes with Fail response")
	expectActions(t, outputAc, []interface{}{AlarmAc{150}, StateStoreAc{5, "Follower", 0}}, "Leader AppendEntriesRes with Fail response")
}

func TestLeaderAppendEntriesResLogMismatch(t *testing.T) {
	sm := &StateMachine{state: "Leader", term: 4, nextIndex: []uint64{0, 1, 2, 1}, log: []LogEntry{LogEntry{1, []byte("abc")}, LogEntry{2, []byte("def")}, LogEntry{2, []byte("ghijk")}}, config: Config{999, []uint64{995, 996, 997, 998}}, commitIndex: 1}
	outputAc := sm.ProcessEvent(AppendEntriesResEv{997, 4, false})
	expectSM(t, sm, &StateMachine{state: "Leader", term: 4, nextIndex: []uint64{0, 1, 1, 1}, log: []LogEntry{LogEntry{1, []byte("abc")}, LogEntry{2, []byte("def")}, LogEntry{2, []byte("ghijk")}}, config: Config{999, []uint64{995, 996, 997, 998}}, commitIndex: 1}, "Leader AppendEntriesRes with Log Mismatch")
	expectActions(t, outputAc, []interface{}{SendAc{997, AppendEntriesReqEv{4, 999, 0, 1, []LogEntry{LogEntry{2, []byte("def")}, LogEntry{2, []byte("ghijk")}}, 1}}}, "Leader AppendEntriesRes with Log Mismatch")
}

func TestLeaderAppendEntriesResSuccess(t *testing.T) {
	sm := &StateMachine{state: "Leader", term: 2, nextIndex: []uint64{2, 0, 1, 1}, matchIndex: []uint64{2, 0, 1, 0}, log: []LogEntry{LogEntry{1, []byte("abc")}, LogEntry{2, []byte("def")}, LogEntry{2, []byte("ghijk")}}, config: Config{999, []uint64{995, 996, 997, 998}}, commitIndex: 1}
	outputAc := sm.ProcessEvent(AppendEntriesResEv{997, 2, true})
	expectSM(t, sm, &StateMachine{state: "Leader", term: 2, nextIndex: []uint64{2, 0, 3, 1}, matchIndex: []uint64{2, 0, 2, 0}, log: []LogEntry{LogEntry{1, []byte("abc")}, LogEntry{2, []byte("def")}, LogEntry{2, []byte("ghijk")}}, config: Config{999, []uint64{995, 996, 997, 998}}, commitIndex: 2}, "Leader AppendEntriesRes Success Commit Update")
	expectActions(t, outputAc, []interface{}{CommitAc{2, []byte("ghijk"), nil}}, "Leader AppendEntriesRes Success Commit Update")
}

func TestCandidateAppendEntriesRes(t *testing.T) {
	sm := &StateMachine{state: "Candidate", term: 4}
	outputAc := sm.ProcessEvent(AppendEntriesResEv{997, 5, false})
	expectSM(t, sm, &StateMachine{state: "Follower", term: 5, votedFor: 0}, "Candidate AppendEntriesRes with higher term response")
	expectActions(t, outputAc, []interface{}{StateStoreAc{5, "Follower", 0}}, "Candidate AppendEntriesRes with higher term response")
}

func TestVoteResCandidateToLeader(t *testing.T) {
	sm := &StateMachine{state: "Candidate", term: 5, yesVotes: 2, log: []LogEntry{LogEntry{1, []byte("abc")}, LogEntry{2, []byte("def")}, LogEntry{2, []byte("ghijk")}}, config: Config{999, []uint64{995, 996, 997, 998}}, commitIndex: 1, nextIndex: []uint64{1, 1, 1, 1}, matchIndex: []uint64{0, 0, 0, 0}}
	outputAc := sm.ProcessEvent(VoteResEv{5, true})
	expectSM(t, sm, &StateMachine{state: "Leader", term: 5, yesVotes: 3, nextIndex: []uint64{3, 3, 3, 3}, matchIndex: []uint64{0, 0, 0, 0}, log: []LogEntry{LogEntry{1, []byte("abc")}, LogEntry{2, []byte("def")}, LogEntry{2, []byte("ghijk")}}, config: Config{999, []uint64{995, 996, 997, 998}}, commitIndex: 1}, "Vote Response - Candidate to Leader")
	expectActions(t, outputAc, []interface{}{AlarmAc{150}, SendAc{995, AppendEntriesReqEv{5, 999, 2, 2, nil, 1}}, SendAc{996, AppendEntriesReqEv{5, 999, 2, 2, nil, 1}}, SendAc{997, AppendEntriesReqEv{5, 999, 2, 2, nil, 1}}, SendAc{998, AppendEntriesReqEv{5, 999, 2, 2, nil, 1}}, StateStoreAc{5, "Leader", 0}}, "Vote Response - Candidate to Leader")
}

func TestVoteResCandidateBackToFollower(t *testing.T) {
	sm := &StateMachine{state: "Candidate", term: 5, noVotes: 2, votedFor: 999}
	outputAc := sm.ProcessEvent(VoteResEv{5, false})
	expectSM(t, sm, &StateMachine{state: "Follower", term: 5, noVotes: 3, votedFor: 999}, "Vote Response - Candidate Back to Follower")
	expectActions(t, outputAc, []interface{}{AlarmAc{150}, StateStoreAc{5, "Follower", 999}}, "Vote Response - Candidate Back to Follower")
}

func TestVoteResCandidateHigherTerm(t *testing.T) {
	sm := &StateMachine{state: "Candidate", term: 4, votedFor: 999}
	outputAc := sm.ProcessEvent(VoteResEv{5, false})
	expectSM(t, sm, &StateMachine{state: "Follower", term: 5, votedFor: 0}, "Vote Response - Candidate - Higher term")
	expectActions(t, outputAc, []interface{}{AlarmAc{150}, StateStoreAc{5, "Follower", 0}}, "Vote Response - Candidate - Higher term")
}

func TestVoteResLeader(t *testing.T) {
	sm := &StateMachine{state: "Leader"}
	outputAc := sm.ProcessEvent(VoteResEv{5, false})
	expectSM(t, sm, &StateMachine{state: "Leader"}, "Vote Response - Leader")
	expectActions(t, outputAc, nil, "Vote Response - Leader")
}

func TestVoteResFollower(t *testing.T) {
	sm := &StateMachine{state: "Follower", term: 4, votedFor: 999}
	outputAc := sm.ProcessEvent(VoteResEv{5, false})
	expectSM(t, sm, &StateMachine{state: "Follower", term: 5, votedFor: 0}, "Vote Response - Follower")
	expectActions(t, outputAc, []interface{}{StateStoreAc{5, "Follower", 0}}, "Vote Response - Follower")
}

func TestVoteReqFollower(t *testing.T) {
	sm := &StateMachine{state: "Follower", term: 5, votedFor: 999, log: []LogEntry{LogEntry{1, []byte("abc")}, LogEntry{2, []byte("def")}, LogEntry{3, []byte("ghi")}}, config: Config{999, []uint64{995, 996, 997, 998}}}
	outputAc := sm.ProcessEvent(VoteReqEv{5, 999, 3, 3})
	expectSM(t, sm, &StateMachine{state: "Follower", term: 5, votedFor: 999, log: []LogEntry{LogEntry{1, []byte("abc")}, LogEntry{2, []byte("def")}, LogEntry{3, []byte("ghi")}}, config: Config{999, []uint64{995, 996, 997, 998}}}, "Vote Request - Follower - Vote Granted")
	expectActions(t, outputAc, []interface{}{AlarmAc{150}, SendAc{999, VoteResEv{5, true}}}, "Vote Request - Follower - Vote Granted")
}

func TestVoteReqFollowerNotUptoDate(t *testing.T) {
	sm := &StateMachine{state: "Follower", term: 5, votedFor: 999, log: []LogEntry{LogEntry{1, []byte("abc")}, LogEntry{2, []byte("def")}, LogEntry{3, []byte("ghi")}}, config: Config{999, []uint64{995, 996, 997, 998}}}
	outputAc := sm.ProcessEvent(VoteReqEv{5, 999, 1, 3})
	expectSM(t, sm, &StateMachine{state: "Follower", term: 5, votedFor: 999, log: []LogEntry{LogEntry{1, []byte("abc")}, LogEntry{2, []byte("def")}, LogEntry{3, []byte("ghi")}}, config: Config{999, []uint64{995, 996, 997, 998}}}, "Vote Request - Follower - Log Not Upto Date")
	expectActions(t, outputAc, []interface{}{SendAc{999, VoteResEv{5, false}}}, "Vote Request - Follower - Log Not Upto Date")
}

func TestVoteReqLeader(t *testing.T) {
	sm := &StateMachine{state: "Leader", term: 4, votedFor: 997, log: []LogEntry{LogEntry{1, []byte("abc")}, LogEntry{2, []byte("def")}, LogEntry{3, []byte("ghi")}}, config: Config{999, []uint64{995, 996, 997, 998}}}
	outputAc := sm.ProcessEvent(VoteReqEv{5, 999, 2, 3})
	expectSM(t, sm, &StateMachine{state: "Follower", term: 5, votedFor: 999, log: []LogEntry{LogEntry{1, []byte("abc")}, LogEntry{2, []byte("def")}, LogEntry{3, []byte("ghi")}}, config: Config{999, []uint64{995, 996, 997, 998}}}, "Vote Request - Leader")
	expectActions(t, outputAc, []interface{}{AlarmAc{150}, SendAc{999, VoteResEv{5, true}}, StateStoreAc{5, "Follower", 999}}, "Vote Request - Follower - Leader")
}

func TestVoteReqCandidateLowerTerm(t *testing.T) {
	sm := &StateMachine{state: "Candidate", term: 4, votedFor: 997, log: []LogEntry{LogEntry{1, []byte("abc")}, LogEntry{2, []byte("def")}, LogEntry{3, []byte("ghi")}}, config: Config{999, []uint64{995, 996, 997, 998}}}
	outputAc := sm.ProcessEvent(VoteReqEv{3, 999, 2, 3})
	expectSM(t, sm, &StateMachine{state: "Candidate", term: 4, votedFor: 997, log: []LogEntry{LogEntry{1, []byte("abc")}, LogEntry{2, []byte("def")}, LogEntry{3, []byte("ghi")}}, config: Config{999, []uint64{995, 996, 997, 998}}}, "Vote Request - Candidate - Lower term request")
	expectActions(t, outputAc, []interface{}{SendAc{999, VoteResEv{4, false}}}, "Vote Request - Follower - Candidate - Lower term request")
}

func TestVoteReqCandidateLeaderNotUptoDate(t *testing.T) {
	sm := &StateMachine{state: "Candidate", term: 4, votedFor: 997, log: []LogEntry{LogEntry{1, []byte("abc")}, LogEntry{2, []byte("def")}, LogEntry{3, []byte("ghi")}}, config: Config{999, []uint64{995, 996, 997, 998}}}
	outputAc := sm.ProcessEvent(VoteReqEv{5, 999, 2, 2})
	expectSM(t, sm, &StateMachine{state: "Follower", term: 5, votedFor: 0, log: []LogEntry{LogEntry{1, []byte("abc")}, LogEntry{2, []byte("def")}, LogEntry{3, []byte("ghi")}}, config: Config{999, []uint64{995, 996, 997, 998}}}, "Vote Request - Candidate - Leader not upto date")
	expectActions(t, outputAc, []interface{}{AlarmAc{150}, SendAc{999, VoteResEv{5, false}}, StateStoreAc{5, "Follower", 0}}, "Vote Request - Candidate - Leader not upto date")
}

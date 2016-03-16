package main

import (
	"github.com/cs733-iitb/log"
	"strconv"
	"testing"
	"time"
//	"fmt"
)

// Number of replicated nodes
const totRaftNodes = 5
const jsonFile = "config.json"

var peers []NetConfig

func prepareRaftNodeConfigObj() {
	peers = []NetConfig{NetConfig{100, "localhost", 8001}, NetConfig{200, "localhost", 8002}, NetConfig{300, "localhost", 8003}, NetConfig{400, "localhost", 8004}, NetConfig{500, "localhost", 8005}}
}

func TestRaftNodeBasic(t *testing.T) {
	prepareRaftNodeConfigObj()
	rnArr := makeRafts()
	//for i:=0; i<totRaftNodes; i++ {
	//	go rnArr[i].processEvents()
	//}
	// make sure you wait before leader is getting selected
	time.Sleep(1 * time.Second)
	// get leader id from a stable system
	_ = getLeaderById(rnArr[0].LeaderId(), rnArr)
	// fmt.Printf("Leader Id = %v \n", ldr)
	/* ldr.Append([]byte("foo"))
	time.Sleep(2 * time.Second)

	for _, rn := range rnArr {
		select {
		case ci := <-rn.CommitChannel():
			if ci.err != nil {
				t.Fatal(ci.err)
			}
			if string(ci.data) != "foo" {
				t.Fatal("Got different data")
			}
		default:
			t.Fatal("Expected message on all nodes")
		}
	}*/
	//	fmt.Println("test case executed")
}

func makeRafts() []RaftNode {
	//_ = RaftNodeConfig{peers, 100, "PersistentData", RandInt(150, 300), RandInt(75, 150)}
	rnArr := make([]RaftNode, totRaftNodes)
	for i := 0; i < totRaftNodes; i++ {
		initRaftStateFile("PersistentData_" + strconv.Itoa((i+1)*100))
		rnArr[i] = New(RaftNodeConfig{peers, uint64((i + 1) * 100), "PersistentData_" + strconv.Itoa((i+1)*100), RandInt(150, 300), 100}, jsonFile)
		go rnArr[i].processEvents()
	}
	return rnArr
}

func getLeaderById(ldrId uint64, rnArr []RaftNode) *RaftNode {
	for index, rn := range rnArr {
		if rn.Id() == ldrId {
			return &rnArr[index]
		}
	}
	return nil
}

func initRaftStateFile(logDir string) {
	// fmt.Printf("init raft state file : logDir - %v \n", logDir)
	stateAttrsFP, err := log.Open(logDir + "/" + StateFile)
	stateAttrsFP.RegisterSampleEntry(PersistentStateAttrs{})
	stateAttrsFP.SetCacheSize(1)
	assert(err == nil)
	defer stateAttrsFP.Close()
	stateAttrsFP.TruncateToEnd(0)
	err1 := stateAttrsFP.Append(PersistentStateAttrs{0, "Follower", 0})
	// fmt.Println(err1)
	assert(err1 == nil)
	// fmt.Println("file created successfully")
}

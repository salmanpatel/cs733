/*

Raft Node Initializer
functions to be used by file server to build raft node underneath

*/

package main

import (
	"encoding/json"
	"fmt"
	//"github.com/cs733-iitb/log"
	"io/ioutil"
	"os"
	//"runtime"
	"strconv"
	"strings"
	//"testing"
	//"time"
)

const totRaftNodes = 5

type PeerSpec struct {
	Id        int64
	Address   string
	FSAddress string
}

type PeerSpecArr struct {
	Peers []PeerSpec
}

func checkErr(err error, errMsg string) {
	if err != nil {
		fmt.Printf("Error: %v \n", errMsg)
		os.Exit(1)
	}
}

func prepareRaftNodeConfigObj(jsonFile string) []NetConfig {
	data, err := ioutil.ReadFile(jsonFile)
	checkErr(err, "reading JSON file")
	var peerSpecArr PeerSpecArr
	err = json.Unmarshal(data, &peerSpecArr)
	checkErr(err, "decoding JSON file")
	peers := make([]NetConfig, len(peerSpecArr.Peers))
	for i, peer := range peerSpecArr.Peers {
		hostPort := strings.Split(peer.Address, ":")
		port, err := strconv.Atoi(hostPort[1])
		checkErr(err, "string to int conversion for port")
		peers[i] = NetConfig{peer.Id, hostPort[0], port}
	}
	return peers
}

func prepareFSConfigObj(jsonFile string) []NetConfig {
	data, err := ioutil.ReadFile(jsonFile)
	checkErr(err, "reading JSON file")
	var peerSpecArr PeerSpecArr
	err = json.Unmarshal(data, &peerSpecArr)
	checkErr(err, "decoding JSON file")
	peers := make([]NetConfig, len(peerSpecArr.Peers))
	for i, peer := range peerSpecArr.Peers {
		hostPort := strings.Split(peer.FSAddress, ":")
		port, err := strconv.Atoi(hostPort[1])
		checkErr(err, "string to int conversion for port")
		peers[i] = NetConfig{peer.Id, hostPort[0], port}
	}
	return peers
}

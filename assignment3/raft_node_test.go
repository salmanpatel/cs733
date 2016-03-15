package main

import (
	"github.com/cs733-iitb/cluster"
	"os"
	"testing"
)

func TestRaftNodeBasic(t *testing.T) {
	//	srv, err := cluster.New(1, "config.json")
	cluster.New(1, "config.json")
}

func rmlog(logDir string) {
	os.RemoveAll(logDir + "/" + logFile)
}

package main

import "github.com/cs733-iitb/cluster"
import "testing"

func TestRaftNodeBasic(t *testing.T) {
//	srv, err := cluster.New(1, "config.json")
	cluster.New(1, "config.json")
}

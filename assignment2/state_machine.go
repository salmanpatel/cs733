package main

/*import (
	"fmt"
)*/

type logEntry struct {
	term uint64
	data []byte
}

type ServerStatePers struct {
	currentTerm uint64
	votedFor bool
	log []logEntry
	state string
}

type ServerStateNonPers struct {
	commitIndex uint64	
	nextIndex []uint64
	matchIndex []uint64
}

func main() {
}

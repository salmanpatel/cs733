package main

type Node interface {
	// Client's message to Raft node
	Append([]byte)

	// A channel for client to listen on. What goes into Append must come out of here at some point.
	CommitChannel() <-chan CommitInfo

	// Last known committed index in the log. This could be -1 until the system stabilizes.
	CommittedIndex() uint64

	// Returns the data at a log index, or an error.
	Get(index int) (error, []byte)

	// Node's id
	Id() uint64

	// Id of leader. -1 if unknown
	LeaderId() uint64

	// Signal to shut down all goroutines, stop sockets, flush log and close it, cancel timers.
	Shutdown()
}

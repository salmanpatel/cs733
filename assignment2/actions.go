package main

type SendAc struct {
	peerId uint64
	event interface{}
}

type CommitAc struct {
	index uint64
	data []byte
	err error
}

type AlarmAc struct {
	time uint64
}

type LogStoreAc struct {
	index uint64
	term uint64
	data []byte
}

type StateStoreAc struct {
	term uint64
	state string
	votedFor uint64
}

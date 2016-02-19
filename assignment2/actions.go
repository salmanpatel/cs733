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

type LogStore struct {
	index uint64
	data []byte
}

type StateStore struct {
	term uint64
	votedFor uint64
	state uint64
}

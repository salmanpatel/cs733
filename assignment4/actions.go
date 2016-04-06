package main

type SendAc struct {
	peerId int64
	event  interface{}
}

type CommitAc struct {
	index int64
	data  []byte
	err   error
}

type AlarmAc struct {
	time int64
}

type LogStoreAc struct {
	index int64
	term  int64
	data  []byte
}

type StateStoreAc struct {
	term     int64
	state    string
	votedFor int64
}

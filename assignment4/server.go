package main

import (
	"bufio"
	"bytes"
	"encoding/gob"
	"fmt"
	"github.com/salmanpatel/cs733/assignment4/fs"
	"math"
	"net"
	"os"
	"strconv"
)

var crlf = []byte{'\r', '\n'}

type MsgStruct struct {
	Data fs.Msg
}

type Response struct {
	resp *fs.Msg
	err  error
}

func check(obj interface{}) {
	if obj != nil {
		fmt.Println(obj)
		os.Exit(1)
	}
}

func reply(conn *net.TCPConn, msg *fs.Msg) bool {
	var err error
	write := func(data []byte) {
		if err != nil {
			return
		}
		_, err = conn.Write(data)
	}
	var resp string
	switch msg.Kind {
	case 'C': // read response
		resp = fmt.Sprintf("CONTENTS %d %d %d", msg.Version, msg.Numbytes, msg.Exptime)
	case 'O':
		resp = "OK "
		if msg.Version > 0 {
			resp += strconv.Itoa(msg.Version)
		}
	case 'F':
		resp = "ERR_FILE_NOT_FOUND"
	case 'V':
		resp = "ERR_VERSION " + strconv.Itoa(msg.Version)
	case 'M':
		resp = "ERR_CMD_ERR"
	case 'I':
		resp = "ERR_INTERNAL"
	case 'R':
		resp = "ERR_REDIRECT " + string(msg.Contents[:])
	default:
		fmt.Printf("Unknown response kind '%c'", msg.Kind)
		return false
	}
	resp += "\r\n"
	write([]byte(resp))
	if msg.Kind == 'C' {
		write(msg.Contents)
		write(crlf)
	}
	return err == nil
}

func serve(conn *net.TCPConn, clientId int64, cmdChan chan *Response, rn RaftNode, fsStruct *fs.FS) {

	reader := bufio.NewReader(conn)

	for {

		msg, msgerr, fatalerr := fs.GetMsg(reader)
		if fatalerr != nil {
			reply(conn, &fs.Msg{Kind: 'M'})
			conn.Close()
			break
		}
		if msgerr != nil {
			if (!reply(conn, &fs.Msg{Kind: 'M'})) {
				conn.Close()
				break
			}
			continue
		}

		var respMsg *fs.Msg

		// replicate command to all other servers
		// reads need not be replicated

		if msg.Kind != 'r' {

			// set client id to identify client channel to whom commit channel should forward
			msg.ClientId = clientId

			// replicate command before processing
			msgBytes, err := encode(*msg)

			// server is not able to encode
			if err != nil {
				if (!reply(conn, &fs.Msg{Kind: 'I'})) {
					conn.Close()
					break
				}
				continue
			}

			// replicate this command to all other raft nodes
			rn.Append(msgBytes)

			// wait on channel untill it has been replicated on majority of raft nodes
			respStrVar := <-cmdChan
			errVal := respStrVar.err
			respMsg = respStrVar.resp
			if errVal != nil {
				// set message kind to ERR_REDIRECT
				cont := []byte(getConnStringById(rn.LeaderId(), jsonFile))
				reply(conn, &fs.Msg{Kind: 'R', Contents: cont})
				conn.Close()
				break
			}
		} else {
			respMsg = fs.ProcessMsg(msg, fsStruct)
		}

		// response := fs.ProcessMsg(msg)
		if !reply(conn, respMsg) {
			conn.Close()
			break
		}
	}
}

func getConnStringById(id int64, jsonFile string) string {
	fsConfigObj := prepareFSConfigObj(jsonFile)
	for _, fsc := range fsConfigObj {
		if fsc.id == id {
			return fsc.host + ":" + strconv.Itoa(fsc.port)
		}
	}
	// Ideally server id should match with one of the id's from config object
	// otherwise returning empty string
	return ""
}

func serverMain(id int64, peers []NetConfig, jsonFile string) {

	// Initialize raft node and spawn independent go routine
	rn := initRaftNode(id, peers, jsonFile)
	go rn.processEvents()

	var fsStruct = &fs.FS{Dir: make(map[string]*fs.FileInfo, 1000)}

	// open listen port for client connections
	connString := getConnStringById(id, jsonFile)
	tcpaddr, err := net.ResolveTCPAddr("tcp", connString)
	check(err)
	tcp_acceptor, err := net.ListenTCP("tcp", tcpaddr)
	check(err)

	// register MsgStruct for encoding and decoding of Msg structure to binary data
	gob.Register(MsgStruct{})

	// used as an indentified for different client connections
	var clientId int64 = 0

	// manage index that is processed last
	var lastIndexPrcsd int64 = -1

	// map to maintain client id to channel mapping, so that after reading command
	// from commit channel it can be put to appropriate client channel
	clientIdToChanMap := make(map[int64]chan *Response)

	// go routine to listen on commit channel from a raft node
	go func() {
		for {
			cmtInfo := <-rn.CommitChannel()
			// we get index as -1 when append has been requested on follower
			// in that case - we do not replicate command but we do get commitInfo on commit channel
			if cmtInfo.index != -1 {
				// check for missing indexes
				if cmtInfo.index != lastIndexPrcsd+1 {
					// handleMissingCmnds()
					for i := lastIndexPrcsd + 1; i < cmtInfo.index; i++ {
						fmt.Printf("Processing missing indexes: %v\n", i)
						err, msngMsg := rn.Get(int(i))
						if err != nil {
							// invalid index has been requested
							fmt.Println("Error: invalid index requested")
						}
						msngMsgDecoded, err := decode(msngMsg)
						if err != nil {
							// server facing problem with message decoding
							fmt.Println("Error: decoding message after replication")
						} else {
							response := fs.ProcessMsg(&msngMsgDecoded, fsStruct)
							clientIdToChanMap[msngMsgDecoded.ClientId] <- &Response{response, nil}
						}
					}
				}
			}
			msg, err := decode(cmtInfo.data)
			if err != nil {
				// server facing problem with messafe decoding
				fmt.Println("Error: decoding message after replication")
			} else {
				response := fs.ProcessMsg(&msg, fsStruct)
				clientIdToChanMap[msg.ClientId] <- &Response{response, cmtInfo.err}
			}
			lastIndexPrcsd = cmtInfo.index
		}
	}()

	for {
		tcp_conn, err := tcp_acceptor.AcceptTCP()
		check(err)
		clientId = (clientId + 1) % math.MaxInt64
		clientIdToChanMap[clientId] = make(chan *Response)
		go serve(tcp_conn, clientId, clientIdToChanMap[clientId], rn, fsStruct)
	}
}

func encode(msg fs.Msg) ([]byte, error) {
	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)
	le := MsgStruct{Data: msg}
	err := enc.Encode(le)
	return buf.Bytes(), err
}

func decode(msgBytes []byte) (fs.Msg, error) {
	buf := bytes.NewBuffer(msgBytes)
	enc := gob.NewDecoder(buf)
	var le MsgStruct
	err := enc.Decode(&le)
	return le.Data, err
}

package main

import (
	//"encoding/json"
	"fmt"
	//"github.com/cs733-iitb/log"
	//"io/ioutil"
	"os"
	"os/exec"
	"sync"
	//"runtime"
	"strconv"
	"strings"
	"testing"
	"time"
	// "github.com/salmanpatel/cs733/assignment4/fs"
	"bufio"
	"bytes"
	"errors"
	"log"
	"net"
	// "reflect"
)

const jsonFile = "config.json"

var fsProcesses map[int64]*exec.Cmd

type Msg struct {
	// Kind = the first character of the command. For errors, it
	// is the first letter after "ERR_", ('V' for ERR_VERSION, for
	// example), except for "ERR_CMD_ERR", for which the kind is 'M'
	Kind     byte
	Filename string
	Contents []byte
	Numbytes int
	Exptime  int // expiry time in seconds
	Version  int
}

func cleanup(logDir string) {
	os.RemoveAll(logDir)
	os.Remove("state" + logDir[3:])
}

/*
procedure to cleanup previous logs &
initialize state variable file with default values
*/

func initRaftStateFile(logDir string) {
	// fmt.Printf("init raft state file : logDir - %v \n", logDir)
	cleanup(logDir)

	/* Old state file creation
	stateAttrsFP, err := log.Open(logDir + "/" + StateFile)
	stateAttrsFP.RegisterSampleEntry(PersistentStateAttrs{})
	stateAttrsFP.SetCacheSize(1)
	assert(err == nil)
	defer stateAttrsFP.Close()
	stateAttrsFP.TruncateToEnd(0)
	err1 := stateAttrsFP.Append(PersistentStateAttrs{0, "Follower", 0})
	// fmt.Println(err1)
	assert(err1 == nil)
	*/

	f, err := os.OpenFile("state"+logDir[3:], os.O_CREATE|os.O_WRONLY, 0666)
	//fmt.Println(err)
	checkErr(err, "opening state file : initRaftStateFile ")
	defer f.Close()
	b := bufio.NewWriter(f)
	defer func() {
		if err = b.Flush(); err != nil {
			log.Fatal(err)
		}
	}()

	//var n int
	_, err = fmt.Fprintf(b, "%s %s %s\n", "0", "Follower", "0")
	//fmt.Println(n)
	checkErr(err, "writing to state file : initRaftStateFile ")
	//fmt.Println("file created successfully")
}

func InitServer() {
	peers := prepareRaftNodeConfigObj(jsonFile)
	// Clear Previous Logs
	// Initialize State File with Initial Values
	for _, peer := range peers {
		initRaftStateFile("dir" + strconv.FormatInt(peer.id, 10))
		go serverMain(peer.id, peers, jsonFile)
	}
	time.Sleep(5 * time.Second)
}

func startAllServers() {
	fsProcesses = make(map[int64]*exec.Cmd)
	peers := prepareRaftNodeConfigObj(jsonFile)
	for i := 0; i < len(peers); i++ {
		initRaftStateFile("dir" + strconv.FormatInt(peers[i].id, 10))
		cmd := exec.Command("./assignment4", strconv.FormatInt(peers[i].id, 10), jsonFile)
		cmd.Stdout = os.Stdout
		cmd.Stdin = os.Stdin
		cmd.Start()
		fsProcesses[peers[i].id] = cmd
	}
	time.Sleep(5 * time.Second)
}

func startServer(serverId int64) {
	cmd := exec.Command("./assignment4", strconv.FormatInt(serverId, 10), jsonFile)
	cmd.Stdout = os.Stdout
	cmd.Stdin = os.Stdin
	cmd.Start()
	fsProcesses[serverId] = cmd
}

func TestSimple(t *testing.T) {
	// fmt.Println("Starting file server")
	//InitServer()
	startAllServers()
}

func expect(t *testing.T, response *Msg, expected *Msg, errstr string, err error) {
	if err != nil {
		t.Fatal("Unexpected error: " + err.Error())
	}
	ok := true
	if response.Kind != expected.Kind {
		ok = false
		errstr += fmt.Sprintf(" Got kind='%c', expected '%c'", response.Kind, expected.Kind)
	}
	if expected.Version > 0 && expected.Version != response.Version {
		ok = false
		errstr += " Version mismatch"
	}
	if response.Kind == 'C' {
		if expected.Contents != nil &&
			bytes.Compare(response.Contents, expected.Contents) != 0 {
			ok = false
		}
	}
	if !ok {
		t.Fatal("Expected " + errstr)
	}
}

func TestRPC_BasicSequential(t *testing.T) {
	cl := mkClient(t, "localhost:9001")
	defer cl.close()
	//fmt.Println("BasicSequential started")
	// Read non-existent file cs733net
	m, err := cl.read("cs733net")
	expect(t, m, &Msg{Kind: 'F'}, "file not found", err)
	//fmt.Println("cl.read(cs733net) pass")

	// Read non-existent file cs733net
	cl, m, err = cl.sendDeleteCommand(t, "cs733net")
	expect(t, m, &Msg{Kind: 'F'}, "file not found", err)
	//fmt.Println("sendDeleteCommand(t,cs733net) pass")
	// fmt.Printf("conn value after delete: %v \n", cl)

	// Write file cs733net
	data := "Cloud fun"
	cl, m, err = cl.sendWriteCommand(t, "cs733net", data, 0)
	expect(t, m, &Msg{Kind: 'O'}, "write success", err)
	//fmt.Println("cl.sendWriteCommand(t,cs733net, data, 0) pass")

	// Expect to read it back
	m, err = cl.read("cs733net")
	expect(t, m, &Msg{Kind: 'C', Contents: []byte(data)}, "read my write", err)
	//fmt.Println("cl.read(cs733net) pass")

	// CAS in new value
	version1 := m.Version
	data2 := "Cloud fun 2"
	// Cas new value
	cl, m, err = cl.sendCasCommand(t, "cs733net", version1, data2, 0)
	expect(t, m, &Msg{Kind: 'O'}, "cas success", err)
	//fmt.Println("sendCasCommand(t, cs733net, version1, data2, 0) pass")

	// Expect to read it back
	m, err = cl.read("cs733net")
	expect(t, m, &Msg{Kind: 'C', Contents: []byte(data2)}, "read my cas", err)
	//fmt.Println("read(cs733net) pass")

	// Expect Cas to fail with old version
	cl, m, err = cl.sendCasCommand(t, "cs733net", version1, data, 0)
	expect(t, m, &Msg{Kind: 'V'}, "cas version mismatch", err)
	//fmt.Println("sendCasCommand(t, cs733net, version1, data, 0)")

	// Expect a failed cas to not have succeeded. Read should return data2.
	m, err = cl.read("cs733net")
	expect(t, m, &Msg{Kind: 'C', Contents: []byte(data2)}, "failed cas to not have succeeded", err)
	//fmt.Println("read(cs733net)")

	// sendDeleteCommand
	cl, m, err = cl.sendDeleteCommand(t, "cs733net")
	expect(t, m, &Msg{Kind: 'O'}, "delete success", err)
	//fmt.Printf("sendDeleteCommand(t,cs733net)")

	// Expect to not find the file
	m, err = cl.read("cs733net")
	expect(t, m, &Msg{Kind: 'F'}, "file not found", err)
	//fmt.Println("read(cs733net)")

	//fmt.Println("TestRPC_BasicSequential Pass")
}

func TestRPC_Binary(t *testing.T) {
	cl := mkClient(t, "localhost:9003")
	defer cl.close()

	// Write binary contents
	data := "\x00\x01\r\n\x03" // some non-ascii, some crlf chars
	cl, m, err := cl.sendWriteCommand(t, "binfile", data, 0)
	expect(t, m, &Msg{Kind: 'O'}, "write success", err)

	// Expect to read it back
	m, err = cl.read("binfile")
	expect(t, m, &Msg{Kind: 'C', Contents: []byte(data)}, "read my write", err)

	//fmt.Println("TestRPC_Binary Pass")
}

func TestRPC_BasicTimer(t *testing.T) {
	//fmt.Println("Testing TestRPC_BasicTimer")

	cl := mkClient(t, "localhost:9001")
	defer cl.close()

	// Write file cs733, with expiry time of 2 seconds
	str := "Cloud fun"
	cl, m, err := cl.sendWriteCommand(t, "cs733", str, 2)
	expect(t, m, &Msg{Kind: 'O'}, "write success", err)

	// Expect to read it back immediately.
	m, err = cl.read("cs733")
	expect(t, m, &Msg{Kind: 'C', Contents: []byte(str)}, "read my cas", err)

	time.Sleep(3 * time.Second)

	// Expect to not find the file after expiry
	m, err = cl.read("cs733")
	expect(t, m, &Msg{Kind: 'F'}, "file not found", err)

	// Recreate the file with expiry time of 1 second
	cl, m, err = cl.sendWriteCommand(t, "cs733", str, 1)
	expect(t, m, &Msg{Kind: 'O'}, "file recreated", err)

	// Overwrite the file with expiry time of 4. This should be the new time.
	cl, m, err = cl.sendWriteCommand(t, "cs733", str, 3)
	expect(t, m, &Msg{Kind: 'O'}, "file overwriten with exptime=4", err)

	// The last expiry time was 3 seconds. We should expect the file to still be around 2 seconds later
	time.Sleep(2 * time.Second)

	// Expect the file to not have expired.
	m, err = cl.read("cs733")
	expect(t, m, &Msg{Kind: 'C', Contents: []byte(str)}, "file to not expire until 4 sec", err)

	time.Sleep(3 * time.Second)
	// 5 seconds since the last write. Expect the file to have expired
	m, err = cl.read("cs733")
	expect(t, m, &Msg{Kind: 'F'}, "file not found after 4 sec", err)

	// Create the file with an expiry time of 1 sec. We're going to delete it
	// then immediately create it. The new file better not get deleted.
	cl, m, err = cl.sendWriteCommand(t, "cs733", str, 5)
	expect(t, m, &Msg{Kind: 'O'}, "file created for delete", err)

	cl, m, err = cl.sendDeleteCommand(t, "cs733")
	expect(t, m, &Msg{Kind: 'O'}, "deleted ok", err)

	cl, m, err = cl.sendWriteCommand(t, "cs733", str, 0) // No expiry
	expect(t, m, &Msg{Kind: 'O'}, "file recreated", err)

	time.Sleep(1100 * time.Millisecond) // A little more than 1 sec
	m, err = cl.read("cs733")
	expect(t, m, &Msg{Kind: 'C'}, "file should not be deleted", err)

	//fmt.Println("TestRPC_BasicTimer Pass")
}

// nclients write to the same file. At the end the file should be
// any one clients' last write

func TestRPC_ConcurrentWrites(t *testing.T) {
	nclients := 35
	niters := 5
	clients := make([]*Client, nclients)
	for i := 0; i < nclients; i++ {
		cl := mkClient(t, "localhost:9001")
		if cl == nil {
			t.Fatalf("Unable to create client #%d", i)
		}
		defer cl.close()
		clients[i] = cl
	}

	errCh := make(chan error, nclients)
	var sem sync.WaitGroup // Used as a semaphore to coordinate goroutines to begin concurrently
	sem.Add(1)
	ch := make(chan *Msg, nclients*niters) // channel for all replies
	for i := 0; i < nclients; i++ {
		go func(i int, cl *Client) {
			sem.Wait()
			for j := 0; j < niters; j++ {
				str := fmt.Sprintf("cl %d %d", i, j)
				var m *Msg
				var err error
				clients[i], m, err = clients[i].sendWriteCommand(t, "concWrite", str, 0)
				// fmt.Printf("write cl %d %d successfull\n", i,j)
				if err != nil {
					errCh <- err
					break
				} else {
					ch <- m
				}
			}
		}(i, clients[i])
	}
	time.Sleep(100 * time.Millisecond) // give goroutines a chance
	sem.Done()                         // Go!

	// There should be no errors
	for i := 0; i < nclients*niters; i++ {
		select {
		case m := <-ch:
			if m.Kind != 'O' {
				t.Fatalf("Concurrent write failed with kind=%c", m.Kind)
			}
		case err := <-errCh:
			t.Fatal(err)
		}
	}
	m, _ := clients[0].read("concWrite")
	// Ensure the contents are of the form "cl <i> 9"
	// The last write of any client ends with " 9"
	if !(m.Kind == 'C' && strings.HasSuffix(string(m.Contents), " 4")) {
		t.Fatalf("Expected to be able to read after 1000 writes. Got msg = %v", m)
	}
	//fmt.Println("ConcurrentWrite Pass")
}

// nclients cas to the same file. At the end the file should be any one clients' last write.
// The only difference between this test and the ConcurrentWrite test above is that each
// client loops around until each CAS succeeds. The number of concurrent clients has been
// reduced to keep the testing time within limits.
func TestRPC_ConcurrentCas(t *testing.T) {
	nclients := 10
	niters := 2

	clients := make([]*Client, nclients)
	for i := 0; i < nclients; i++ {
		cl := mkClient(t, "localhost:9001")
		if cl == nil {
			t.Fatalf("Unable to create client #%d", i)
		}
		defer cl.close()
		clients[i] = cl
	}

	var sem sync.WaitGroup // Used as a semaphore to coordinate goroutines to *begin* concurrently
	sem.Add(1)

	var m *Msg
	clients[0], m, _ = clients[0].sendWriteCommand(t, "concCas", "first", 0)
	ver := m.Version
	if m.Kind != 'O' || ver == 0 {
		t.Fatalf("Expected write to succeed and return version")
	}

	var wg sync.WaitGroup
	wg.Add(nclients)

	errorCh := make(chan error, nclients)

	for i := 0; i < nclients; i++ {
		go func(i int, ver int, cl *Client) {
			sem.Wait()
			defer wg.Done()
			for j := 0; j < niters; j++ {
				str := fmt.Sprintf("cl %d %d", i, j)
				for {
					var err error
					var m *Msg
					cl, m, err = cl.sendCasCommand(t, "concCas", ver, str, 0)
					if err != nil {
						errorCh <- err
						return
					} else if m.Kind == 'O' {
						// fmt.Printf("cas %d %d successfull\n", i,j)
						break
					} else if m.Kind != 'V' {
						errorCh <- errors.New(fmt.Sprintf("Expected 'V' msg, got %c", m.Kind))
						return
					}
					ver = m.Version // retry with latest version
				}
			}
		}(i, ver, clients[i])
	}

	time.Sleep(100 * time.Millisecond) // give goroutines a chance
	sem.Done()                         // Start goroutines
	wg.Wait()                          // Wait for them to finish
	select {
	case e := <-errorCh:
		t.Fatalf("Error received while doing cas: %v", e)
	default: // no errors
	}
	m, _ = clients[0].read("concCas")
	if !(m.Kind == 'C' && strings.HasSuffix(string(m.Contents), " 1")) {
		t.Fatalf("Expected to be able to read after 1000 writes. Got msg.Kind = %d, msg.Contents=%s", m.Kind, m.Contents)
	}
	//fmt.Println("TestRPC_ConcurrentCas Pass")
}

func TestFollowerCrash(t *testing.T) {
	connString := "localhost:9005"

	cl := mkClient(t, connString)
	defer cl.close()
	//fmt.Println("BasicSequential started")
	// Read non-existent file cs733net
	m, err := cl.read("F_Crash")
	expect(t, m, &Msg{Kind: 'F'}, "file not found", err)
	//fmt.Println("cl.read(cs733net) pass")

	// Write file cs733net
	data := "Cloud fun"
	cl, m, err = cl.sendWriteCommand(t, "F_Crash", data, 0)
	expect(t, m, &Msg{Kind: 'O'}, "write success", err)
	//fmt.Println("cl.sendWriteCommand(t,cs733net, data, 0) pass")

	leaderId := identifyLeader(t, []int64{})
	//fmt.Printf("leader id: %v \n",leaderId)
	var followerId int64
	fsPeers := prepareFSConfigObj(jsonFile)
	for _, peer := range fsPeers {
		if peer.id != leaderId {
			followerId = peer.id
			break
		}
	}
	//fmt.Printf("follower Id: %v \n",followerId)

	// kill follower
	err = fsProcesses[followerId].Process.Kill()
	checkErr(err, "killing follower")

	data = "file created in absence of one follower"
	cl, m, err = cl.sendWriteCommand(t, "F_Crash_2", data, 0)
	expect(t, m, &Msg{Kind: 'O'}, "write success", err)
	//fmt.Println("F_Crash_2 success")

	cl, m, err = cl.sendDeleteCommand(t, "F_Crash")
	expect(t, m, &Msg{Kind: 'O'}, "delete success", err)

	// start follower back
	startServer(followerId)

	// wait for replication
	time.Sleep(3 * time.Second)

	// read it from follower which was crashed
	cl.close()
	connString = idToConnString(followerId)
	//fmt.Printf("connecting to %v\n", connString)
	cl = mkClient(t, connString)
	m, err = cl.read("F_Crash_2")
	expect(t, m, &Msg{Kind: 'C', Contents: []byte(data)}, "read F_Crash_2", err)
	m, err = cl.read("F_Crash")
	expect(t, m, &Msg{Kind: 'F', Contents: []byte(data)}, "read F_Crash", err)
	//fmt.Println("cl.read(cs733net) pass")
	//fmt.Println("TestFollowerCrash Pass")
}

func TestLeaderCrash(t *testing.T) {
	//fmt.Println("Leader Crash Started")
	connString := "localhost:9005"

	cl := mkClient(t, connString)
	defer cl.close()

	data := "Cloud fun 1"
	cl, m, err := cl.sendWriteCommand(t, "L_Crash_1", data, 0)
	expect(t, m, &Msg{Kind: 'O'}, "write success 1", err)
	//fmt.Println("cl.sendWriteCommand(t,cs733net, data, 0) pass")

	data = "Cloud fun 2"
	cl, m, err = cl.sendWriteCommand(t, "L_Crash_2", data, 0)
	expect(t, m, &Msg{Kind: 'O'}, "write success 2", err)
	//fmt.Println("cl.sendWriteCommand(t,cs733net, data, 0) pass")

	// give some time to leader to replicate it's log to follower
	time.Sleep(3 * time.Second)

	// kill leader
	oldLeaderId := identifyLeader(t, []int64{})
	err = fsProcesses[oldLeaderId].Process.Kill()
	checkErr(err, "killing leader")

	// give some time to elect new leader
	time.Sleep(10 * time.Second)

	// make connection to new leader
	newLeaderId := identifyLeader(t, []int64{oldLeaderId})
	connString = idToConnString(newLeaderId)
	//fmt.Printf("connecting to %v\n", connString)
	cl = mkClient(t, connString)

	cl, m, err = cl.sendDeleteCommand(t, "L_Crash_1")
	expect(t, m, &Msg{Kind: 'O'}, "delete success 1", err)

	data = "file created on new leader"
	cl, m, err = cl.sendWriteCommand(t, "F_Crash_3", data, 0)
	expect(t, m, &Msg{Kind: 'O'}, "write success 3", err)
	//fmt.Println("F_Crash_2 success")

	// Now start old leader
	startServer(oldLeaderId)

	// wait for replication
	time.Sleep(3 * time.Second)

	// read it from old leader
	cl.close()
	connString = idToConnString(oldLeaderId)
	//fmt.Printf("connecting to %v\n", connString)
	cl = mkClient(t, connString)
	m, err = cl.read("F_Crash_1")
	expect(t, m, &Msg{Kind: 'F', Contents: []byte(data)}, "read F_Crash_1", err)
	m, err = cl.read("F_Crash_3")
	expect(t, m, &Msg{Kind: 'C', Contents: []byte(data)}, "read F_Crash_3", err)
	//fmt.Println("cl.read(cs733net) pass")
}

func getFollowerId(leaderId int64) int64 {
	followerId := int64(100)
	fsPeers := prepareFSConfigObj(jsonFile)
	for _, peer := range fsPeers {
		if peer.id != leaderId {
			followerId = peer.id
			break
		}
	}
	return followerId
}

func idToConnString(id int64) string {
	fsPeers := prepareFSConfigObj(jsonFile)
	for _, peer := range fsPeers {
		if peer.id == id {
			return peer.host + ":" + strconv.Itoa(peer.port)
		}
	}
	return ""
}

func identifyLeader(t *testing.T, skipList []int64) int64 {
	//fmt.Printf("skip list %v \n", skipList)
	fsPeers := prepareFSConfigObj(jsonFile)
	for _, peer := range fsPeers {
		if !contains(skipList, peer.id) {
			//fmt.Printf("checking for %v id\n", peer.id)
			cl := mkClient(t, peer.host+":"+strconv.Itoa(peer.port))
			msg, err := cl.delete("SomeNonExistingFile")
			// fmt.Printf("sendWriteCommand: %v\n", msg)
			if err != nil {
				t.Fatal("Unexpected error: " + err.Error())
			}
			if msg != nil && msg.Kind != 'R' {
				return peer.id
			}
		}
	}
	return 100
}

func contains(s []int64, e int64) bool {
	for _, a := range s {
		if a == e {
			return true
		}
	}
	return false
}

func mkClient(t *testing.T, connString string) *Client {
	var client *Client
	raddr, err := net.ResolveTCPAddr("tcp", connString)
	if err == nil {
		conn, err := net.DialTCP("tcp", nil, raddr)
		if err == nil {
			client = &Client{conn: conn, reader: bufio.NewReader(conn)}
		} else {
			client = nil
			fmt.Println("Error in make client")
		}
	}
	if err != nil {
		t.Fatal(err)
	}
	return client
}

type Client struct {
	conn   *net.TCPConn
	reader *bufio.Reader // a bufio Reader wrapper over conn
}

func (cl *Client) close() {
	if cl != nil && cl.conn != nil {
		cl.conn.Close()
		cl.conn = nil
	}
}

var errNoConn = errors.New("Connection is closed")

func (cl *Client) send(str string) error {
	if cl.conn == nil {
		return errNoConn
	}
	_, err := cl.conn.Write([]byte(str))
	if err != nil {
		err = fmt.Errorf("Write error in SendRaw: %v", err)
		cl.conn.Close()
		cl.conn = nil
	}
	return err
}

func (cl *Client) read(filename string) (*Msg, error) {
	cmd := "read " + filename + "\r\n"
	return cl.sendRcv(cmd)
}

func (cl *Client) write(filename string, contents string, exptime int) (*Msg, error) {
	var cmd string
	if exptime == 0 {
		cmd = fmt.Sprintf("write %s %d\r\n", filename, len(contents))
	} else {
		cmd = fmt.Sprintf("write %s %d %d\r\n", filename, len(contents), exptime)
	}
	cmd += contents + "\r\n"
	// fmt.Printf("write: %v", cmd)
	return cl.sendRcv(cmd)
}

func (cl *Client) rcv() (msg *Msg, err error) {
	// we will assume no errors in server side formatting
	line, err := cl.reader.ReadString('\n')
	if err == nil {
		msg, err = parseFirst(line)
		if err != nil {
			return nil, err
		}
		if msg.Kind == 'C' {
			contents := make([]byte, msg.Numbytes)
			var c byte
			for i := 0; i < msg.Numbytes; i++ {
				if c, err = cl.reader.ReadByte(); err != nil {
					break
				}
				contents[i] = c
			}
			if err == nil {
				msg.Contents = contents
				cl.reader.ReadByte() // \r
				cl.reader.ReadByte() // \n
			}
		}
	}
	if err != nil {
		cl.close()
	}
	return msg, err
}

func (cl *Client) sendRcv(str string) (msg *Msg, err error) {
	if cl.conn == nil {
		//fmt.Println("sendRcv: connection closed")
		return nil, errNoConn
	}
	err = cl.send(str)
	if err == nil {
		msg, err = cl.rcv()
		//fmt.Printf("sendRcv: %v %v \n", msg, err)
	}
	return msg, err
}

func (cl *Client) cas(filename string, version int, contents string, exptime int) (*Msg, error) {
	var cmd string
	if exptime == 0 {
		cmd = fmt.Sprintf("cas %s %d %d\r\n", filename, version, len(contents))
	} else {
		cmd = fmt.Sprintf("cas %s %d %d %d\r\n", filename, version, len(contents), exptime)
	}
	cmd += contents + "\r\n"
	return cl.sendRcv(cmd)
}

func (cl *Client) delete(filename string) (*Msg, error) {
	cmd := "delete " + filename + "\r\n"
	return cl.sendRcv(cmd)
}

func parseFirst(line string) (msg *Msg, err error) {
	fields := strings.Fields(line)
	msg = &Msg{}

	// Utility function fieldNum to int
	toInt := func(fieldNum int) int {
		var i int
		if err == nil {
			if fieldNum >= len(fields) {
				err = errors.New(fmt.Sprintf("Not enough fields. Expected field #%d in %s\n", fieldNum, line))
				return 0
			}
			i, err = strconv.Atoi(fields[fieldNum])
		}
		return i
	}

	if len(fields) == 0 {
		return nil, errors.New("Empty line. The previous command is likely at fault")
	}
	switch fields[0] {
	case "OK": // OK [version]
		msg.Kind = 'O'
		if len(fields) > 1 {
			msg.Version = toInt(1)
		}
	case "CONTENTS": // CONTENTS <version> <numbytes> <exptime> \r\n
		msg.Kind = 'C'
		msg.Version = toInt(1)
		msg.Numbytes = toInt(2)
		msg.Exptime = toInt(3)
	case "ERR_VERSION":
		msg.Kind = 'V'
		msg.Version = toInt(1)
	case "ERR_FILE_NOT_FOUND":
		msg.Kind = 'F'
	case "ERR_CMD_ERR":
		msg.Kind = 'M'
	case "ERR_INTERNAL":
		msg.Kind = 'I'
	case "ERR_REDIRECT":
		msg.Kind = 'R'
		if len(fields) < 2 {
			msg.Contents = []byte("localhost:9001")
		} else {
			msg.Contents = []byte(fields[1])
		}
	default:
		err = errors.New("Unknown response " + fields[0])
	}
	if err != nil {
		return nil, err
	} else {
		return msg, nil
	}
}

func (cl *Client) sendDeleteCommand(t *testing.T, filename string) (c *Client, msg *Msg, err error) {
	flag := 0
	for flag == 0 {
		msg, err = cl.delete(filename)
		if err != nil {
			t.Fatal("Unexpected error: " + err.Error())
		}
		if msg.Kind != 'R' {
			flag = 1
			return cl, msg, err
		}
		cl.close()
		//fmt.Printf("Redirection: %v\n", string(msg.Contents[:]))
		cl = mkClient(t, string(msg.Contents[:]))
	}
	return cl, msg, err
}

func (cl *Client) sendWriteCommand(t *testing.T, filename string, contents string, exptime int) (c *Client, msg *Msg, err error) {
	flag := 0
	for flag == 0 {
		msg, err = cl.write(filename, contents, exptime)
		// fmt.Printf("sendWriteCommand: %v\n", msg)
		if err != nil {
			t.Fatal("Unexpected error: " + err.Error())
		}
		if msg != nil && msg.Kind != 'R' {
			flag = 1
			return cl, msg, err
		}
		cl.close()
		//fmt.Printf("Redirection: %v\n", string(msg.Contents[:]))
		cl = mkClient(t, string(msg.Contents[:]))
	}
	return cl, msg, err
}

func (cl *Client) sendCasCommand(t *testing.T, filename string, version int, contents string, exptime int) (c *Client, msg *Msg, err error) {
	flag := 0
	for flag == 0 {
		msg, err = cl.cas(filename, version, contents, exptime)
		if err != nil {
			t.Fatal("Unexpected error: " + err.Error())
		}
		if msg.Kind != 'R' {
			flag = 1
			return cl, msg, err
		}
		cl.close()
		//mt.Printf("Redirection: %v\n", string(msg.Contents[:]))
		cl = mkClient(t, string(msg.Contents[:]))
	}
	return cl, msg, err
}

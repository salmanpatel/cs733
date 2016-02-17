package main

import (
	"bufio"
	"fmt"
	"net"
	"strconv"
	"strings"
	"testing"
	"time"
	"runtime"
)

const noOfClients int = 100
const totWrite int = 10
var c = make(chan bool)
var changeCtrl = make(chan bool)
var version2 string
var contents2 string = "test2"
var name2 string = "f.txt"
var exptime2 string = "10"

func TestConcurrency(t *testing.T) {
	runtime.GOMAXPROCS(1010)
	go serverMain()
        time.Sleep(1 * time.Second) // one second is enough time for the server to start
//	noOfClients := 1000
	for i:=0 ; i<noOfClients; i++ {
		go sendMessage(t, i)
	}
	for k:=0; k<noOfClients; k++ {
		<- c
	}
//	fmt.Println("All go routine finished")
	conn, err := net.DialTimeout("tcp", "localhost:8080", 1 * time.Second)
	if err != nil {
		t.Error(err.Error()) // report error through testing framework
	}
	scanner := bufio.NewScanner(conn)
	name := "hi.txt"
	fmt.Fprintf(conn, "read %v\r\n", name) // try a read now
        scanner.Scan()
        arr := strings.Split(scanner.Text(), " ")
        expect(t, arr[0], "CONTENTS")
//	version := noOfClients*totWrite
//        expect(t, arr[1], fmt.Sprintf("%v", version)) // expect only accepts strings, convert int version to string
//        expect(t, arr[2], fmt.Sprintf("%v", len(contents)))     
        scanner.Scan()
        expectContent(t, scanner.Text())
}


func TestTCPSimple(t *testing.T) {
//	go serverMain()
//        time.Sleep(1 * time.Second) // one second is enough time for the server to start
	name := "hi.txt"
	contents := "bye"
	exptime := 300000
	conn, err := net.Dial("tcp", "localhost:8080")
	if err != nil {
		t.Error(err.Error()) // report error through testing framework
	}

	scanner := bufio.NewScanner(conn)

	// Write a file
	fmt.Fprintf(conn, "write %v %v %v\r\n%v\r\n", name, len(contents), exptime, contents)
	scanner.Scan() // read first line
	resp := scanner.Text() // extract the text from the buffer
	arr := strings.Split(resp, " ") // split into OK and <version>
	expect(t, arr[0], "OK")
	version, err := strconv.ParseInt(arr[1], 10, 64) // parse version as number
	if err != nil {
		t.Error("Non-numeric version found")
	}

	fmt.Fprintf(conn, "read %v\r\n", name) // try a read now
	scanner.Scan()

	arr = strings.Split(scanner.Text(), " ")
	expect(t, arr[0], "CONTENTS")
	expect(t, arr[1], fmt.Sprintf("%v", version)) // expect only accepts strings, convert int version to string
	expect(t, arr[2], fmt.Sprintf("%v", len(contents)))	
	scanner.Scan()
	expect(t, contents, scanner.Text())
}

// Simple serial check of getting and setting
func sendMessage(t *testing.T, clientNo int) {
	name := "hi.txt"
//	contents := ""
	exptime := 300000
	conn, err := net.DialTimeout("tcp", "localhost:8080", 1 * time.Second)
	if err != nil {
		t.Error(err.Error()) // report error through testing framework
	}
	scanner := bufio.NewScanner(conn)
	for j:=0; j<totWrite; j++ {
		contents := strconv.FormatInt(int64(totWrite*clientNo+j),10)
		fmt.Fprintf(conn, "write %v %v %v\r\n%v\r\n", name, len(contents), exptime, contents)
		scanner.Scan() // read first line
		arr := strings.Split(scanner.Text(), " ") // extract the text from the buffer
		expect(t, arr[0], "OK")
		_, err := strconv.ParseInt(arr[1], 10, 64) // parse version as number
		if err != nil {
			t.Error("Non-numeric version found")
		}
	}
	c <- true
}

func TestExpiry(t *testing.T) {
	go writeMessage(t)
	go readMessage(t)
        <- c
	<- c
}

func readMessage(t *testing.T) {
	conn, err := net.DialTimeout("tcp", "localhost:8080", 1 * time.Second)
	if err != nil {
		t.Error(err.Error()) // report error through testing framework
	}
	scanner := bufio.NewScanner(conn)
	<- changeCtrl
	fmt.Fprintf(conn, "read %v\r\n", name2) // try a read now
        scanner.Scan()
        arr := strings.Split(scanner.Text(), " ")
        expect(t, arr[0], "CONTENTS")
        expect(t, arr[1], fmt.Sprintf("%v", version2)) // expect only accepts strings, convert int version to string
        expect(t, arr[2], fmt.Sprintf("%v", len(contents2)))
        expect(t, arr[3], "5")
        scanner.Scan()
        expect(t, scanner.Text(), contents2)
	<- changeCtrl
        fmt.Fprintf(conn, "read %v\r\n", name2) // try a read now
        scanner.Scan()
        expect(t, scanner.Text(), "ERR_FILE_NOT_FOUND")
	c <- true
}

// Simple serial check of getting and setting
func writeMessage(t *testing.T) {
	conn, err := net.DialTimeout("tcp", "localhost:8080", 1 * time.Second)
	if err != nil {
		t.Error(err.Error()) // report error through testing framework
	}
	scanner := bufio.NewScanner(conn)
	fmt.Fprintf(conn, "write %v %v %v\r\n%v\r\n", name2, len(contents2), exptime2, contents2)
	scanner.Scan() // read first line
	arr := strings.Split(scanner.Text(), " ") // extract the text from the buffer
	expect(t, arr[0], "OK")
	version2 = arr[1]
	_, err = strconv.ParseInt(arr[1], 10, 64) // parse version as number
	if err != nil {
		t.Error("Non-numeric version found")
	}
	time.Sleep(5*time.Second)
	changeCtrl <- true
	time.Sleep(6*time.Second)
	changeCtrl <- true
	c <- true
}

func TestWriteCas(t *testing.T) {
        conn, err := net.DialTimeout("tcp", "localhost:8080", 1 * time.Second)
        if err != nil {
                t.Error(err.Error()) // report error through testing framework
        }
        scanner := bufio.NewScanner(conn)
        name3 := "f.txt"
        contents3 := "begin test3"
        fmt.Fprintf(conn, "write %v %v\r\n%v\r\n", name3, len(contents3), contents3)
        scanner.Scan() // read first line
        arr := strings.Split(scanner.Text(), " ") // extract the text from the buffer
        expect(t, arr[0], "OK")
        version3 := arr[1]
        _, err = strconv.ParseInt(arr[1], 10, 64) // parse version as number
        if err != nil {
                t.Error("Non-numeric version found")
        }
	go write3(t, &version3, name3)
	go cas3(t, &version3, name3)
	<- c 
	<- c
        fmt.Fprintf(conn, "read %v\r\n", name3) // try a read now
        scanner.Scan()
        arr = strings.Split(scanner.Text(), " ")
        expect(t, arr[0], "CONTENTS")
//        expect(t, arr[1], fmt.Sprintf("%v", version3)) // expect only accepts strings, convert int version to string
        expect(t, arr[2], fmt.Sprintf("%v", len("write3")))
	scanner.Scan()
	expect(t, "write3", scanner.Text())
}

func write3(t *testing.T, version3 *string, name3 string) {
//	fmt.Println("running write3")
        conn, err := net.DialTimeout("tcp", "localhost:8080", 1 * time.Second)
        if err != nil {
                t.Error(err.Error()) // report error through testing framework
        }
        scanner := bufio.NewScanner(conn)
	contents3 := "write3"
        fmt.Fprintf(conn, "write %v %v\r\n%v\r\n", name3, len(contents3), contents3)
        scanner.Scan() // read first line
        arr := strings.Split(scanner.Text(), " ") // extract the text from the buffer
        expect(t, arr[0], "OK")
	*version3 = arr[1]
        _, err = strconv.ParseInt(arr[1], 10, 64) // parse version as number
        if err != nil {
                t.Error("Non-numeric version found")
        }
//	fmt.Println("write3 done")
	c <- true
}

func cas3(t *testing.T, version3 *string, name3 string) {
        conn, err := net.DialTimeout("tcp", "localhost:8080", 1 * time.Second)
        if err != nil {
                t.Error(err.Error()) // report error through testing framework
        }
        scanner := bufio.NewScanner(conn)
        contents3 := "cas3"
        fmt.Fprintf(conn, "cas %v %v %v\r\n%v\r\n", name3, *version3, len(contents3), contents3)
        scanner.Scan() // read first line
/*        arr := strings.Split(scanner.Text(), " ") // extract the text from the buffer
        expect(t, arr[0], "OK")
	*version3 = arr[1]
        _, err = strconv.ParseInt(arr[1], 10, 64) // parse version as number
        if err != nil {
                t.Error("Non-numeric version found")
        }*/
	c <- true
}

func TestCas(t *testing.T) {
	name4 := "f2.txt"
	conn, err := net.DialTimeout("tcp", "localhost:8080", 1 * time.Second)
        if err != nil {
                t.Error(err.Error()) // report error through testing framework
        }
        scanner := bufio.NewScanner(conn)
        contents4 := "begin test4"
        fmt.Fprintf(conn, "write %v %v\r\n%v\r\n", name4, len(contents4), contents4)
        scanner.Scan() // read first line
        arr := strings.Split(scanner.Text(), " ") // extract the text from the buffer
        expect(t, arr[0], "OK")
        _, err = strconv.ParseInt(arr[1], 10, 64) // parse version as number
        if err != nil {
                t.Error("Non-numeric version found")
        }
        for i:=0 ; i<noOfClients; i++ {
                go sendMultipleCas(t, i, name4)
        }
        for k:=0; k<noOfClients; k++ {
                <- c
        }
//      fmt.Println("All go routine finished")
        fmt.Fprintf(conn, "read %v\r\n", name4) // try a read now
        scanner.Scan()
        arr = strings.Split(scanner.Text(), " ")
        expect(t, arr[0], "CONTENTS")
        scanner.Scan()
        expectContent(t, scanner.Text())
}

func sendMultipleCas(t *testing.T, clientNo int, name4 string) {
        conn, err := net.DialTimeout("tcp", "localhost:8080", 1 * time.Second)
        if err != nil {
                t.Error(err.Error()) // report error through testing framework
        }
        scanner := bufio.NewScanner(conn)
        for j:=0; j<totWrite; j++ {
		fmt.Fprintf(conn, "read %v\r\n", name4) // try a read now
	        scanner.Scan()
		arr := strings.Split(scanner.Text(), " ")
	        expect(t, arr[0], "CONTENTS")
		version := arr[1]
		scanner.Scan()
	        scanner.Text()
                contents := strconv.FormatInt(int64(totWrite*clientNo+j),10)
                fmt.Fprintf(conn, "cas %v %v %v\r\n%v\r\n", name4, version, len(contents), contents)
                scanner.Scan() // read first line
                arr = strings.Split(scanner.Text(), " ") // extract the text from the buffer
//                expect(t, arr[0], "OK")
                _, err := strconv.ParseInt(arr[1], 10, 64) // parse version as number
                if err != nil {
                        t.Error("Non-numeric version found")
                }
        }
        c <- true
}

// Useful testing function
func expect(t *testing.T, a string, b string) {
	if a != b {
		t.Error(fmt.Sprintf("Expected %v, found %v", b, a)) // t.Error is visible when running `go test -verbose`
	}
}

func expectContent(t *testing.T, contents string) {
	flag := false
	for i:=0; i<noOfClients; i++ {
		if contents == strconv.FormatInt(int64(totWrite*i+totWrite-1),10) {
			flag = true
		}
	}
	if !flag {
		t.Error(fmt.Sprintf("Found %v", contents)) // t.Error is visible when running `go test -verbose`
	}
}

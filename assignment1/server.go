package main

import (
	"fmt"
	"net"
	"bufio"
	"strings"
	"sync"
	"strconv"
	"io"
	"time"
	"math"
	"sync/atomic"
)

type File struct {
	fileName string
	fileSize uint64
	fileVersionNo uint64
	fileExpTime uint64
	fileContent []byte
}

var psuedoTime uint64 = 0
const initialVersionNo uint64 = 1
const maxCmdSize uint64 = 2
var mapLock sync.RWMutex
var fileSystem map[string]File

func writeFile(reader *bufio.Reader, conn net.Conn ,cmdTokens []string) {
//	fmt.Println("Inside write file")
//	conn.Write([]byte(""))
	fileSize, err := strconv.ParseUint(cmdTokens[2],10,64)
	if err != nil {
		conn.Write([]byte("ERR_CMD_ERR\r\n"))
		conn.Close()
		return
	}
	var contentBuffer []byte = make([]byte,fileSize)
	c := make(chan bool)
	go func(){
		select{
                        case <-time.After(60 * time.Second):
                                conn.Write([]byte("ERR_INTERNAL\r\n"))
                                conn.Close()
                                return
                        case <-c:
                }
        }()
	_, err = io.ReadFull(reader, contentBuffer)
	c <- true
	byte1,err1 := reader.ReadByte()
	byte2,err2 := reader.ReadByte()
//	fmt.Println(contentBuffer)
//	fmt.Println(n)
	if byte1!='\r' || byte2!='\n' || err != nil || err1 !=nil || err2!=nil {
		conn.Write([]byte("ERR_INTERNAL\r\n"))
		conn.Close()
		return
	}
	// Reading and discarding "\r\n"
//	bufio.NewReader(conn).ReadByte()
//	contentBuffer = nil
	var expTimeUser uint64
	var expTime uint64 = math.MaxUint64
	if(len(cmdTokens)==4) {
		expTimeUser, err = strconv.ParseUint(cmdTokens[3],10,64)
		if err != nil {
			conn.Write([]byte("ERR_CMD_ERR\r\n"))
			return
		}
		expTime = psuedoTime + expTimeUser
	}
	var versionNo uint64 = initialVersionNo
	mapLock.Lock()
	if fileObj, ok := fileSystem[cmdTokens[1]]; ok  {
		fileObj.fileContent = contentBuffer
		fileObj.fileVersionNo += 1
		fileObj.fileSize = fileSize
		fileObj.fileExpTime = expTime
		fileSystem[cmdTokens[1]] = fileObj
		versionNo = fileObj.fileVersionNo
	} else {
		fileSystem[cmdTokens[1]] = File{cmdTokens[1],fileSize, initialVersionNo, expTime, contentBuffer}
	}
	mapLock.Unlock()
	conn.Write([]byte("OK " + strconv.FormatUint(versionNo,10) + "\r\n"))
}

func readFile(conn net.Conn, cmdTokens []string) {
//	fmt.Println("Inside read file")
	mapLock.RLock()
	fileObj, ok := fileSystem[cmdTokens[1]]
	mapLock.RUnlock()
	if !ok || fileObj.fileExpTime < psuedoTime {
		conn.Write([]byte("ERR_FILE_NOT_FOUND\r\n"))
		return
        } else {
		expTime := ""
		if fileObj.fileExpTime != math.MaxUint64 {
			expTime = " " + strconv.FormatUint(fileObj.fileExpTime-psuedoTime,10)
		}
		returnVal := "CONTENTS " + strconv.FormatUint(fileObj.fileVersionNo, 10) + " " + strconv.FormatUint(fileObj.fileSize, 10) + expTime + "\r\n"
		returnVal2 := append([]byte(returnVal), fileObj.fileContent...)
		conn.Write(append(returnVal2, []byte("\r\n")...))
	}
}

func casFile(reader *bufio.Reader, conn net.Conn, cmdTokens []string) {
//	fmt.Println("Inside CAS file")
//	conn.Write([]byte(""))
	fileSize, err1 := strconv.ParseUint(cmdTokens[3],10,64)
	if err1 != nil  {
		conn.Write([]byte("ERR_CMD_ERR\r\n"))
		conn.Close()
		return
	}
	var contentBuffer []byte = make([]byte,fileSize)
	c := make(chan bool)
	go func(){
                select{
                        case <-time.After(60 * time.Second):
                                conn.Write([]byte("ERR_INTERNAL\r\n"))
                                conn.Close()
                                return
                        case <-c:
                }
        }()
	_, readErr := io.ReadFull(reader, contentBuffer)
	c <- true
	byte1, readErr1 := reader.ReadByte()
	byte2, readErr2 := reader.ReadByte()
//	fmt.Println(contentBuffer)
//	fmt.Println(n)
	if byte1!='\r' || byte2!='\n' || readErr != nil || readErr1 !=nil || readErr2!=nil {
		conn.Write([]byte("ERR_INTERNAL\r\n"))
		conn.Close()
		return
	}
//	fmt.Println(contentBuffer)
//	fmt.Println(n)
//	fmt.Println("no error in readinf buff")
	// Reading and discarding "\r\n"
//	fmt.Println("read slash ")
	versionNo, err2 := strconv.ParseUint(cmdTokens[2],10,64)
	if err2 != nil {
		conn.Write([]byte("ERR_CMD_ERR\r\n"))
		return
	}
//	fmt.Println("version no read")
	var expTimeUser uint64
	var err3 error
	var expTime uint64 = math.MaxUint64
	if(len(cmdTokens)==5) {
		expTimeUser, err3 = strconv.ParseUint(cmdTokens[4],10,64)
		if err3 != nil {
			conn.Write([]byte("ERR_CMD_ERR\r\n"))
			return
		}
		expTime = psuedoTime + expTimeUser
//	fmt.Println("exp time calculated")
	}
	var fileObj File
	var ok bool
	mapLock.Lock()
//	fmt.Println("lock aquired")
	if fileObj, ok = fileSystem[cmdTokens[1]]; !ok  {
		mapLock.Unlock()
		conn.Write([]byte("ERR_FILE_NOT_FOUND\r\n"))
		return
        }
//	fmt.Println("file found")
	if versionNo != fileObj.fileVersionNo {
		newVersion := fileObj.fileVersionNo
		mapLock.Unlock()
		conn.Write([]byte("ERR_VERSION " + strconv.FormatUint(newVersion,10) + "\r\n"))
		return
	}
//	fmt.Println("version matched")
	fileObj.fileVersionNo += 1
	fileObj.fileContent = contentBuffer
	fileObj.fileSize = fileSize
	fileObj.fileExpTime = expTime
	fileSystem[cmdTokens[1]] = fileObj
	mapLock.Unlock()
	conn.Write([]byte("OK " + strconv.FormatUint(fileObj.fileVersionNo,10) + "\r\n"))
}

func deleteFile(conn net.Conn, cmdTokens []string) {
//	fmt.Println("Inside delete file")
	mapLock.Lock()
	if fileObj, ok := fileSystem[cmdTokens[1]]; !ok || fileObj.fileExpTime < psuedoTime  {
		conn.Write([]byte("ERR_FILE_NOT_FOUND\r\n"))
		mapLock.Unlock()
		return
        }
	delete(fileSystem, cmdTokens[1])
	mapLock.Unlock()
	conn.Write([]byte("OK\r\n"))
}

func processCommand(reader *bufio.Reader, conn net.Conn, command string) {
	tokens := strings.Fields(command)
	if(len(tokens) == 0) {
		conn.Write([]byte("ERR_CMD_ERR\r\n"))
		return
	}
//	fmt.Printf("tokens %v\n", tokens)
//	fmt.Printf("length of tokens %v\n", len(tokens))
	switch tokens[0] {
		 case "write":
			if len(tokens)==3 || len(tokens)==4 {
				writeFile(reader, conn, tokens)
			} else {
				conn.Write([]byte("ERR_CMD_ERR\r\n"))
			}
		 case "read":
			if len(tokens)==2 {
				readFile(conn, tokens)
			} else {
				conn.Write([]byte("ERR_CMD_ERR\r\n"))
			}
		 case "cas":
			if len(tokens)==4 || len(tokens)==5 {
				casFile(reader, conn, tokens)
			} else {
				conn.Write([]byte("ERR_CMD_ERR\r\n"))
			}
		 case "delete":
			if len(tokens)==2 {
				deleteFile(conn, tokens)
			} else {
				conn.Write([]byte("ERR_CMD_ERR\r\n"))
			}
		 default:
//			fmt.Println("Bad Command")
			conn.Write([]byte("ERR_CMD_ERR\r\n"))
			conn.Close()
	}
}

func handleClient(c net.Conn) {
//	fmt.Printf("Client %v connected.\n", c.RemoteAddr())
	reader := bufio.NewReader(c)
	buffer := make([]byte, maxCmdSize)
	var isPrefix bool
	var err error
	for {
//		fmt.Println("Waiting for command")
		buffer,isPrefix,err = reader.ReadLine()
//		fmt.Println(isPrefix)
		if isPrefix || err!=nil {
                        c.Write([]byte("ERR_CMD_ERR\r\n"))
                        c.Close()
			return
		}
//		fmt.Printf("Message Read : %s\n", buffer)
		processCommand(reader, c, string(buffer))
//		fmt.Println("Command processed")
	}
//	fmt.Printf("Client connection closed from %v.\n", c.RemoteAddr())
}

func serverMain() {
	go incTimer()
	fileSystem = make(map[string]File)
	listenPort := "8080"
	listenConn, err := net.Listen("tcp", ":"+listenPort)
	if err != nil {
		fmt.Println("Error opening Listen Socket on %s port", listenPort)
	}
//	fmt.Printf("Server up and listening on port %s\n", listenPort)
	for {
		conn, err := listenConn.Accept()
		if err != nil {
			fmt.Println(err)
			continue
		}
		go handleClient(conn)
	}
}

func incTimer() {
	ticker := time.NewTicker(time.Second * 1)
	go func() {
		for _ = range ticker.C {
			atomic.AddUint64(&psuedoTime, 1)
		}
	}()
}

func main() {
	serverMain()
}

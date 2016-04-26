package main

import "net"
import "fmt"
import "bufio"
import "sync"
import "strings"
import "strconv"
//import "io"
//import "os"

import (
	"testing"
	"time"
)

func TestMain(m *testing.M) {
	m.Run()
}

// Simple serial check of getting and setting
func TestTCPSimple(t *testing.T) {
	termReset();

/**************************************************************/
	rafts, _ := makeMockRafts() // array of []raft.Node
	go serverMain(rafts) // launch the server as a goroutine.
	time.Sleep(5 * time.Second)

	name := "hi.txt"
	contents := "bye"
	exptime := 3000
	conn, err := net.Dial("tcp", "localhost:8080")
	if err != nil {
		t.Error(err.Error()) // report error through testing framework
	}

	scanner := bufio.NewScanner(conn)

	text := "delete hi.txt\r\n"
	fmt.Fprintf(conn, text)

	message, _ := bufio.NewReader(conn).ReadString('\n')

	// Write a file
	fmt.Fprintf(conn, "write %v %v %v\r\n%v\r\n", name, len(contents), exptime, contents)
	scanner.Scan() // read first line
	resp := scanner.Text() // extract the text from the buffer
	arr := strings.Split(resp, " ") // split into OK and <version>
	expect(t, arr[0], "OK")
	ver, err := strconv.Atoi(arr[1]) // parse version as number
	if err != nil {
		t.Error("Non-numeric version found")
	}
	version := int64(ver)

/**************************************************************/
// Leader shutdown and replication check
	var ldr *RaftNode
	for {
		ldr = getLeader(rafts)
		if (ldr != nil) { 
			break
		}
	}
	ldr.ShutDown()

	time.Sleep(5 * time.Second)

	for {
		ldr = getLeader(rafts)
		if (ldr != nil) { 
			break
		}
	}

	fmt.Fprintf(conn, "read %v\r\n", name) // try a read now
	scanner.Scan()

	arr = strings.Split(scanner.Text(), " ")
	expect(t, arr[0], "CONTENTS")
	expect(t, arr[1], fmt.Sprintf("%v", version)) // expect only accepts strings, convert int version to string
	expect(t, arr[2], fmt.Sprintf("%v", len(contents)))	
	scanner.Scan()
	expect(t, contents, scanner.Text())

/**************************************************************/
// Test version increment
	text = "delete input.txt\r\n"
	fmt.Fprintf(conn, text)

	message, _ = bufio.NewReader(conn).ReadString('\n')
	//fmt.Print("Message from server: "+message)

	text = "write input.txt 15\r\nThis is a file.\r\n"
	fmt.Fprintf(conn, text)

	// connect to this socket
	conn2, _ := net.Dial("tcp", "localhost:8080")

	text2 := "write input.txt 10\r\nThis\r\n is.\r\n"
	fmt.Fprintf(conn2, text2)

	message1, _ := bufio.NewReader(conn).ReadString('\n')
	//fmt.Print("Message from server: "+message)

	message2, _ := bufio.NewReader(conn2).ReadString('\n')
	//fmt.Print("Message from server: "+message)

	text = "cas input.txt 1 2\r\n%#\r\n"
	fmt.Fprintf(conn, text)

	message, _ = bufio.NewReader(conn).ReadString('\n')

	if message1 == message2 || len(message) < 2 || (message[:2] != "OK" && (len(message) < 11 || message[:11] != "ERR_VERSION")){
		t.Error("Error in version concurrent write")
	}

	text = "delete input.txt\r\n"
	fmt.Fprintf(conn, text)

	text = "delete input2.txt\r\n"
	fmt.Fprintf(conn, text)
	message, _ = bufio.NewReader(conn).ReadString('\n')
	//fmt.Println("Kill ok")

/**************************************************************/
// Test concurrent write to the same file by multiple clients
	i := 0
	var wg sync.WaitGroup
	wg.Add(10)
	for i < 10 {
		go clients(&wg, t)
		i++
	}

	wg.Wait()

	text = "read input2.txt\r\n"
	fmt.Fprintf(conn, text)

	message, _ = bufio.NewReader(conn).ReadString('\n')
	//fmt.Print("Message from server: "+message)

	if len(message) < 8 || message[:8] != "CONTENTS" {
		message, _ = bufio.NewReader(conn).ReadString('\n')
		//fmt.Print("Message from server: "+message)

		if len(message) < 8 || message[:8] != "CONTENTS" {
			t.Error("Error in concurrent write")
		}
	}

	text = "delete input2.txt\r\n"
	fmt.Fprintf(conn, text)

}

// Client which performs read and write in parellel with itself
func clients(wg *sync.WaitGroup, t *testing.T) {
	conn, _ := net.Dial("tcp", "localhost:8080")
	defer conn.Close()

	//checkError(err)
	reader := bufio.NewReader(conn)
	_, _ = conn.Write([]byte("write input2.txt 5\r\n"))
	_, _ = conn.Write([]byte("!#bin\r\n"))
	line, _ := reader.ReadBytes('\n')

	var str_temp string = string(line)
	if len(str_temp) < 2 || str_temp[:2] != "OK" {
		t.Error(str_temp)
	}

	wg.Done()
}



// Useful testing function
func expect(t *testing.T, a string, b string) {
	if a != b {
		t.Error(fmt.Sprintf("Expected %v, found %v", b, a)) // t.Error is visible when running `go test -verbose`
	}
}


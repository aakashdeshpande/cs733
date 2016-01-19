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
	go serverMain() // launch the server as a goroutine.
	time.Sleep(1 * time.Second)
	m.Run()
}

// Simple serial check of getting and setting
func TestTCPSimple(t *testing.T) {
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
	ver, err := strconv.Atoi(arr[1]) // parse version as number
	if err != nil {
		t.Error("Non-numeric version found")
	}
	version := int64(ver)

	fmt.Fprintf(conn, "read %v\r\n", name) // try a read now
	scanner.Scan()

	arr = strings.Split(scanner.Text(), " ")
	expect(t, arr[0], "CONTENTS")
	expect(t, arr[1], fmt.Sprintf("%v", version)) // expect only accepts strings, convert int version to string
	expect(t, arr[2], fmt.Sprintf("%v", len(contents)))	
	scanner.Scan()
	expect(t, contents, scanner.Text())
}

// Useful testing function
func expect(t *testing.T, a string, b string) {
	if a != b {
		t.Error(fmt.Sprintf("Expected %v, found %v", b, a)) // t.Error is visible when running `go test -verbose`
	}
}

// Test consistency of versions under concurrent write and version check of cas 
func TestVersionIncrement(t *testing.T) {
	// connect to this socket
	conn, _ := net.Dial("tcp", "localhost:8080")
	defer conn.Close()

	text := "delete input2.txt\r\n"
	fmt.Fprintf(conn, text)

	message, _ := bufio.NewReader(conn).ReadString('\n')
	//fmt.Print("Message from server: "+message)

	text = "write input2.txt 15\r\nThis is a file.\r\n"
	fmt.Fprintf(conn, text)

	// connect to this socket
	conn2, _ := net.Dial("tcp", "localhost:8080")

	text2 := "write input2.txt 10\r\nThis\r\n is.\r\n"
	fmt.Fprintf(conn2, text2)

	message1, _ := bufio.NewReader(conn).ReadString('\n')
	//fmt.Print("Message from server: "+message)

	message2, _ := bufio.NewReader(conn2).ReadString('\n')
	//fmt.Print("Message from server: "+message)

	text = "cas input2.txt 1 2\r\n"
	fmt.Fprintf(conn, text)

	message, _ = bufio.NewReader(conn).ReadString('\n')
	//fmt.Print("Message from server: "+message)

	if message1 == message2 || len(message) < 2 || (message != "ERR_VERSION\r\n" && message[:2] != "OK"){
		t.Error("Error in version concurrent write")
	}
}

// Test with jumbled, split and joint commands 
func PartialCommands(t *testing.T) {

	// connect to this socket
	conn, _ := net.Dial("tcp", "localhost:8080")
	defer conn.Close()

	//checkError(err)
	buf := make([]byte, 1024)
	_, _ = conn.Write([]byte("wri"))
	time.Sleep(1000 * time.Millisecond)
	_, _ = conn.Write([]byte("te in"))
	_, _ = conn.Write([]byte("put2.txt 10\r\n"))
	_, _ = conn.Write([]byte("a\r\n"))
	time.Sleep(1000 * time.Millisecond)
	_, _ = conn.Write([]byte("d\n\r"))
	_, _ = conn.Write([]byte("\rdef"))
	time.Sleep(1000 * time.Millisecond)
	_, _ = conn.Write([]byte("\r\n"))
	
	reader := bufio.NewReader(conn)
	//fmt.Println("Sent")
	num, err := reader.Read(buf)
	if len(buf) < 2 || string(buf[:2]) != "OK" || err != nil {
		t.Error(string(buf[:num]))
	}
	_, _ = conn.Write([]byte("delete file##\r\n"))

	text := "delete input2.txt\r\n"
	fmt.Fprintf(conn, text)
	message, _ := bufio.NewReader(conn).ReadString('\n')
	//fmt.Print("Message from server: "+message)

	text = "write input2.txt 15\r\nThis is a file.\r\n"
	fmt.Fprintf(conn, text)
	message, _ = bufio.NewReader(conn).ReadString('\n')
	//fmt.Print("Message from server: "+message)

	text = "read input2.txt\r\nwrite input2.txt 10\r\nThis\r\n is.\r\n"
	fmt.Fprintf(conn, text)

	message, _ = bufio.NewReader(conn).ReadString('\n')
	//fmt.Print("Message from server: "+message)

	message, _ = bufio.NewReader(conn).ReadString('\n')
	//fmt.Print("Message from server: "+message)

	if len(message) <2 || message[:2] != "OK" {

		message, _ = bufio.NewReader(conn).ReadString('\n')
		//fmt.Print("Message from server: "+message)

		if len(message) <2 || message[:2] != "OK" {
			t.Error("Error in command parsing")
		}
	}
}

// Client which performs read and write in parellel with itself
func clients(wg *sync.WaitGroup, t *testing.T) {
	conn, _ := net.Dial("tcp", "localhost:8080")
	defer conn.Close()

	//checkError(err)
	reader := bufio.NewReader(conn)
	_, _ = conn.Write([]byte("write input2.txt 23\r\n"))
	_, _ = conn.Write([]byte("234329giwe039he2~@#4%!@\r\n"))
	line, _ := reader.ReadBytes('\r')
	_, _ = reader.ReadBytes('\n')

	var str_temp string = string(line)
	if len(str_temp) < 2 || str_temp[:2] != "OK" {
		t.Error(str_temp)
	}
	_, _ = conn.Write([]byte("read input2.txt\r\n"))
	line, _ = reader.ReadBytes('\r')
	_, _ = reader.ReadBytes('\n')

	str_temp = string(line)
	if len(str_temp) < 8 || str_temp[:8] != "CONTENTS" {
		t.Error(str_temp)
	}

	line, _ = reader.ReadBytes('\r')
	_, _ = reader.ReadBytes('\n')

	_, _ = conn.Write([]byte("cas input2.txt 12 23\r\n"))
	_, _ = conn.Write([]byte("234329giwe039he2~@#4%!@\r\n"))
	line, _ = reader.ReadBytes('\r')
	_, _ = reader.ReadBytes('\n')

	str_temp = string(line)

	if !(str_temp[:2] == "OK" || str_temp[:11] == "ERR_VERSION") {
		t.Error(str_temp)
	}

	wg.Done()
}

// Test concurrent write to the same file by a large number of clients
func TestConcurrentWrite(t *testing.T) {

	conn, _ := net.Dial("tcp", "localhost:8080")
	defer conn.Close()

	text := "delete input2.txt\r\n"
	fmt.Fprintf(conn, text)
	message, _ := bufio.NewReader(conn).ReadString('\n')
	//fmt.Print("Message from server: "+message)

	i := 0
	var wg sync.WaitGroup
	wg.Add(100)
	for i < 100 {
		go clients(&wg, t)
		i++
	}

	wg.Wait()
	text = "read input2.txt\r\n"
	fmt.Fprintf(conn, text)

	message, _ = bufio.NewReader(conn).ReadString('\n')
	//fmt.Print("Message from server: "+message)

	if len(message) < 8 || message[:8] != "CONTENTS" {
		t.Error("Error in concurrent write")
	}
}

// Test concurrent write to the same file by a large number of clients
func TestWriteExpiry(t *testing.T) {
	conn, _ := net.Dial("tcp", "localhost:8080")
	defer conn.Close()

	text := "delete input2.txt\r\n"
	fmt.Fprintf(conn, text)
	_, _ = bufio.NewReader(conn).ReadString('\n')

	//checkError(err)
	reader := bufio.NewReader(conn)

	_, _ = conn.Write([]byte("write input2.txt 23 2\r\n"))
	_, _ = conn.Write([]byte("234329giwe039he2~@#4%!@\r\n"))
	line, _ := reader.ReadBytes('\r')
	line, _ = reader.ReadBytes('\n')

	time.Sleep(200 * time.Millisecond)
	_, _ = conn.Write([]byte("read input2.txt\r\n"))
	line, _ = reader.ReadBytes('\r')
	_, _ = reader.ReadBytes('\n')
	line2, _ := reader.ReadBytes('\r')
	_, _ = reader.ReadBytes('\n')
	if string(line)+"\n"+string(line2)+"\n" == "ERR_FILE_NOT_FOUND\r\n" {
		t.Error(string(line) + "\n" + string(line2) + "\n")
	}

	time.Sleep(4 * time.Second)
	_, _ = conn.Write([]byte("read input2.txt\r\n"))
	line, _ = reader.ReadBytes('\r')
	_, _ = reader.ReadBytes('\n')
	if string(line)+"\n" != "ERR_FILE_NOT_FOUND\r\n" {
		t.Error(string(line) + "\r\n")
	}

	_, _ = conn.Write([]byte("delete input2.txt\r\n"))

}

// Check with very large input size and when excess input is given
func TestIncorrectInput(t *testing.T) {
	conn, _ := net.Dial("tcp", "localhost:8080")
	defer conn.Close()

	_, _ = conn.Write([]byte("write bigfile! 1000000\r\n"))
	for i := 0; i < 100000; i++ {
		_, _ = conn.Write([]byte("0123456789"))
	}
	buf := make([]byte, 1024)

	_, _ = conn.Write([]byte("\r\n"))

	num, err := conn.Read(buf)
	if len(buf) <2 || string(buf[:2]) != "OK" || err != nil {
		t.Error(string(buf[:num]))
	}

	reader := bufio.NewReader(conn)

	_, _ = conn.Write([]byte("delete bigfile!\r\n"))
	_, _ = reader.ReadBytes('\n')

	//checkError(err)
	_, _ = conn.Write([]byte("write input2.txt 5\r\n"))
	_, _ = conn.Write([]byte("1234556899\r\n"))

	line, _ := reader.ReadBytes('\r')
	_, _ = reader.ReadBytes('\n')

	if len(line) < 2 || string(line)[:2] != "OK" {
		t.Error(string(line) + "\n")
	}

	line, _ = reader.ReadBytes('\r')
	_, _ = reader.ReadBytes('\n')

	if string(line)+"\n" != "ERR_CMD_ERR\r\n" {
		t.Error(string(line) + "\n")
	}

	_, _ = conn.Write([]byte("read input2.txt\r\n"))

	line, _ = reader.ReadBytes('\r')
	_, _ = reader.ReadBytes('\n')

	if len(line) < 8 || string(line)[:8] != "CONTENTS" {
		t.Error(string(line) + "\r\n")
	}

	line, _ = reader.ReadBytes('\r')
	_, _ = reader.ReadBytes('\n')

	if string(line)+"\n" != "12345\r\n" {
		t.Error(string(line) + "\r\n")
	}

	_, _ = conn.Write([]byte("delete input2.txt\r\n"))

}
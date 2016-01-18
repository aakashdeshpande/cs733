package main

import "net"
import "fmt"
import "bufio"
import "sync"

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

func TestVersionIncrement(t *testing.T) {
	// connect to this socket
	conn, _ := net.Dial("tcp", "localhost:8080")

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

	message, _ = bufio.NewReader(conn).ReadString('\n')
	//fmt.Print("Message from server: "+message)

	message, _ = bufio.NewReader(conn2).ReadString('\n')
	//fmt.Print("Message from server: "+message)

	text = "cas input2.txt 1 2\r\n"
	fmt.Fprintf(conn, text)

	message, _ = bufio.NewReader(conn).ReadString('\n')
	//fmt.Print("Message from server: "+message)

	if message != "ERR_VERSION\r\n" {
		t.Error("Error in version concurrent write")
	}
}

func TestJointCommands(t *testing.T) {

	// connect to this socket
	conn, _ := net.Dial("tcp", "localhost:8080")

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

	if message != "OK 2\r\n" {

		message, _ = bufio.NewReader(conn).ReadString('\n')
		//fmt.Print("Message from server: "+message)

		if message != "OK 2\r\n" {
			t.Error("Error in command parsing")
		}
	}
}

func TestConcurrentWrite(t *testing.T) {

	conn, _ := net.Dial("tcp", "localhost:8080")
	defer conn.Close()

	text := "delete input2.txt\r\n"
	fmt.Fprintf(conn, text)
	message, _ := bufio.NewReader(conn).ReadString('\n')
	//fmt.Print("Message from server: "+message)

	i := 0
	var wg sync.WaitGroup
	wg.Add(10)
	for i < 10 {
		go func(wg *sync.WaitGroup) {
			conn, _ := net.Dial("tcp", "localhost:8080")
			text = "write input2.txt 15\r\nThis is a file.\r\n"
			fmt.Fprintf(conn, text)
			bufio.NewReader(conn).ReadString('\n')
			//fmt.Print("Message from server: "+message)
			wg.Done()
		}(&wg)
		i++
	}

	wg.Wait()
	text = "read input2.txt\r\n"
	fmt.Fprintf(conn, text)

	message, _ = bufio.NewReader(conn).ReadString('\n')
	//fmt.Print("Message from server: "+message)

	if message != "CONTENTS 10 15 0\r\n" {
		t.Error("Error in concurrent write")
	}
}

func TestWriteExpiry(t *testing.T) {
	conn, _ := net.Dial("tcp", "localhost:8080")
	defer conn.Close()

	//checkError(err)
	reader := bufio.NewReader(conn)

	_, _ = conn.Write([]byte("write file123@#! 23 1\r\n"))
	_, _ = conn.Write([]byte("234329giwe039he2~@#4%!@\r\n"))
	line, _ := reader.ReadBytes('\r')
	line, _ = reader.ReadBytes('\n')

	time.Sleep(200 * time.Millisecond)
	_, _ = conn.Write([]byte("read file123@#!\r\n"))
	line, _ = reader.ReadBytes('\r')
	_, _ = reader.ReadBytes('\n')
	line2, _ := reader.ReadBytes('\r')
	_, _ = reader.ReadBytes('\n')
	if string(line)+"\n"+string(line2)+"\n" == "ERR_FILE_NOT_FOUND\r\n" {
		t.Error(string(line) + "\n" + string(line2) + "\n")
	}

	time.Sleep(3000 * time.Millisecond)
	_, _ = conn.Write([]byte("read file123@#!\r\n"))
	line, _ = reader.ReadBytes('\r')
	_, _ = reader.ReadBytes('\n')
	if string(line)+"\n" != "ERR_FILE_NOT_FOUND\r\n" {
		t.Error(string(line) + "\r\n")
	}

	_, _ = conn.Write([]byte("delete file123@#!\r\n"))

}

func TestPartialWrite(t *testing.T) {
	conn, _ := net.Dial("tcp", "localhost:8080")
	defer conn.Close()

	//checkError(err)
	reader := bufio.NewReader(conn)
	buf := make([]byte, 1024)
	_, _ = conn.Write([]byte("wri"))
	time.Sleep(1000 * time.Millisecond)
	_, _ = conn.Write([]byte("te fi"))
	_, _ = conn.Write([]byte("le## 9\r\n"))
	_, _ = conn.Write([]byte("a\r\n"))
	time.Sleep(1000 * time.Millisecond)
	_, _ = conn.Write([]byte("d\n\r"))
	_, _ = conn.Write([]byte("\rdf"))
	time.Sleep(1000 * time.Millisecond)
	_, _ = conn.Write([]byte("\r\n"))
	//fmt.Println("Sent")
	num, err := reader.Read(buf)
	if string(buf[:num]) != "OK 1\r\n" || err != nil {
		t.Error(string(buf[:num]))
	}
	_, _ = conn.Write([]byte("delete file##\r\n"))

}

func make_client(t *testing.T, wg *sync.WaitGroup) {
	conn, _ := net.Dial("tcp", "localhost:8080")
	defer conn.Close()

	//checkError(err)
	reader := bufio.NewReader(conn)
	_, _ = conn.Write([]byte("write rand_file! 23\r\n"))
	_, _ = conn.Write([]byte("234329giwe039he2~@#4%!@\r\n"))
	line, _ := reader.ReadBytes('\r')
	_, _ = reader.ReadBytes('\n')

	var str_temp string = string(line)
	if str_temp[:2] != "OK" {
		t.Error(str_temp)
	}
	_, _ = conn.Write([]byte("read rand_file!\r\n"))
	line, _ = reader.ReadBytes('\r')
	_, _ = reader.ReadBytes('\n')

	str_temp = string(line)
	if str_temp[:8] != "CONTENTS" {
		t.Error(str_temp)
	}

	line, _ = reader.ReadBytes('\r')
	_, _ = reader.ReadBytes('\n')

	_, _ = conn.Write([]byte("cas rand_file! 12 23\r\n"))
	_, _ = conn.Write([]byte("234329giwe039he2~@#4%!@\r\n"))
	line, _ = reader.ReadBytes('\r')
	_, _ = reader.ReadBytes('\n')

	str_temp = string(line)

	if !(str_temp[:2] == "OK" || str_temp[:11] == "ERR_VERSION") {
		t.Error(str_temp)
	}
	wg.Done()

}

func TestMultipleClients(t *testing.T) {
	var wg sync.WaitGroup

	wg.Add(100)
	for i := 0; i < 100; i++ {
		go make_client(t, &wg)
	}

	wg.Wait()
	conn, _ := net.Dial("tcp", "localhost:8080")
	defer conn.Close()

	//wg.Add(100)
	for i := 0; i < 100; i++ {
		//go same_session(conn, t, &wg)
	}

	//wg.Wait()

	buf := make([]byte, 1024)

	_, _ = conn.Write([]byte("delete rand_file!\r\n"))
	num, err := conn.Read(buf)
	if string(buf[:num]) != "OK\r\n" || err != nil {
		t.Error(string(buf[:num]))
	}

	conn.Close()

}

func TestBigFile(t *testing.T) {
	conn, _ := net.Dial("tcp", "localhost:8080")
	defer conn.Close()

	_, _ = conn.Write([]byte("write bigfile! 1000000\r\n"))
	for i := 0; i < 100000; i++ {
		_, _ = conn.Write([]byte("2222222222"))
	}
	buf := make([]byte, 1024)

	_, _ = conn.Write([]byte("\r\n"))

	num, err := conn.Read(buf)
	if string(buf[:2]) != "OK" || err != nil {
		t.Error(string(buf[:num]))
	}

	_, _ = conn.Write([]byte("delete bigfile!\r\n"))
}

func TestExcessBytesInWrite(t *testing.T) {
	conn, _ := net.Dial("tcp", "localhost:8080")
	defer conn.Close()

	//checkError(err)
	reader := bufio.NewReader(conn)
	_, _ = conn.Write([]byte("write files## 5\r\n"))
	_, _ = conn.Write([]byte("1234556899\r\n"))

	line, _ := reader.ReadBytes('\r')
	_, _ = reader.ReadBytes('\n')

	if string(line)[:2] != "OK" {
		t.Error(string(line) + "\n")
	}

	line, _ = reader.ReadBytes('\r')
	_, _ = reader.ReadBytes('\n')

	if string(line)+"\n" != "ERR_CMD_ERR\r\n" {
		t.Error(string(line) + "\n")
	}

	_, _ = conn.Write([]byte("read files##\r\n"))

	line, _ = reader.ReadBytes('\r')
	_, _ = reader.ReadBytes('\n')

	if string(line)[:8] != "CONTENTS" {
		t.Error(string(line) + "\r\n")
	}

	line, _ = reader.ReadBytes('\r')
	_, _ = reader.ReadBytes('\n')

	if string(line)+"\n" != "12345\r\n" {
		t.Error(string(line) + "\r\n")
	}

	_, _ = conn.Write([]byte("delete files##\r\n"))

}

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

func TestServer(t *testing.T) {   
	// connect to this socket   
	conn, _ := net.Dial("tcp", "localhost:8080")  

	text := "delete input2.txt\r\n"
	fmt.Fprintf(conn, text)	

	message, _ := bufio.NewReader(conn).ReadString('\n')     
	fmt.Print("Message from server: "+message)   

	text = "write input2.txt 15\r\nThis is a file.\r\n"    
	fmt.Fprintf(conn, text)

	// connect to this socket   
	conn2, _ := net.Dial("tcp", "localhost:8080")  

	text2 := "write input2.txt 10\r\nThis\r\n is.\r\nread input2.txt\r\n"   
	fmt.Fprintf(conn2, text2)

	message, _ = bufio.NewReader(conn).ReadString('\n')     
	fmt.Print("Message from server: "+message)   

	message, _ = bufio.NewReader(conn2).ReadString('\n')     
	fmt.Print("Message from server: "+message)  

	message, _ = bufio.NewReader(conn2).ReadString('.')     
	fmt.Print("Message from server: "+message)  

	message, _ = bufio.NewReader(conn2).ReadString('\n')     
	fmt.Print("Message from server: "+message)  

	text = "write input2.txt\r\n"
	fmt.Fprintf(conn, text)

	message, _ = bufio.NewReader(conn).ReadString('\n')     
	fmt.Print("Message from server: "+message)   

	text = "cas input2.txt"
	fmt.Fprintf(conn, text)

	text = " 2 5\r\n1\r\n"
	fmt.Fprintf(conn, text)

	text = "23\r\n"
	fmt.Fprintf(conn, text)

	message, _ = bufio.NewReader(conn).ReadString('\n')     
	fmt.Print("Message from server: "+message) 

	i := 0
	var wg sync.WaitGroup
	wg.Add(10)
	for i < 10 {
		go func(wg *sync.WaitGroup){
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
	text2 = "read input2.txt\r\n"   
	fmt.Fprintf(conn2, text2)	

	message, _ = bufio.NewReader(conn2).ReadString('2')     
	fmt.Print("Message from server: "+message)  


} 
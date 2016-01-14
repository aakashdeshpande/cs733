package main 
import "net" 
import "fmt" 
import "bufio" 
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

	//for {     
		// read in input from stdin     
		//reader := bufio.NewReader(os.Stdin)     
		text, _ := "write input2.txt 50\r\nThis is serious xyzsgydflysebfkzrmgldkmgiov;SHDUYWfstyawdsahbfduds;hgoidfjmgkdfmglksbdkuyafwdytavdhvawlhbdunThere are two types of assignment operators in go := and =. They are semantically equivalent (with respect to assignment) but the first one is also a ( http://golang.org/ref/spec#Short_variable_declarations ) which means that in the left we need to have at least a new variable declaration for it to be correct. \r", " " //reader.ReadString('\n')     
		// send to socket     
		fmt.Fprintf(conn, text + "\n")

		message, _ := bufio.NewReader(conn).ReadString('\n')     
		fmt.Print("Message from server: "+message)   
	//} 

		// connect to this socket   
	conn2, _ := net.Dial("tcp", "localhost:8080")  

	//for {     
		// read in input from stdin     
		//reader := bufio.NewReader(os.Stdin)     
		text2, _ := "write input2.txt 50\r\nThis is serious too xyzsgydflysebfkzrmgldkmgiov;SHDUYWfstyawdsahbfduds;hgoidfjmgkdfmglksbdkuyafwdytavdhvawlhbdunThere are two types of assignment operators in go := and =. They are semantically equivalent (with respect to assignment) but the first one is also a ( http://golang.org/ref/spec#Short_variable_declarations ) which means that in the left we need to have at least a new variable declaration for it to be correct. \r", " " //reader.ReadString('\n')     
		// send to socket     
		fmt.Fprintf(conn2, text2 + "\n")

		message2, _ := bufio.NewReader(conn2).ReadString('\n')     
		fmt.Print("Message from server: "+message2)   
	//} 	
} 
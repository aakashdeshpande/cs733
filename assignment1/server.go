package main

import "fmt"
import "net"
import "io"
import "os"
//import "strings"

func handleConnection(conn net.Conn) {

	// Make a buffer to hold incoming data.
  	buf := make([]byte, 1024)
  	// Read the incoming connection into the buffer.
  	// Warning : We might not get the complete message
  	// reqLen, err := conn.Read(buf)
  	// if err != nil {
    //	fmt.Println("Error reading:", err.Error())
  	// }
  	totalBytes := 0

    for {
        n, err := conn.Read(buf)
        totalBytes += n
        // Was there an error in reading ?
        if err != nil {
            if err != io.EOF {
                fmt.Println("Error reading:", err.Error())
            }
            break
        }
        //log.Println(n)
    }
    //log.Printf("%d bytes read in %s", totalBytes, time.Now().Sub(start))
  	// Send a response back to person contacting us.
  	fmt.Printf("%v", buf)
  	conn.Write([]byte("Message received."))
  	// Close the connection when you're done with it.
  	conn.Close()
}

func serverMain() {
	ln, err := net.Listen("tcp", ":8080")
	if err != nil {
		// handle error
		fmt.Println("Error listening:", err.Error())
        os.Exit(1)
	}

	//fmt.Println("Listening on " + CONN_HOST + ":" + CONN_PORT)
	//for {
		conn, err := ln.Accept()
		if err != nil {
			// handle error
			fmt.Println("Error accepting: ", err.Error())
            os.Exit(1)
		}
		go handleConnection(conn)
	//}
}

func main() {
	serverMain()
	fmt.Printf("Hello, world.\n")
}

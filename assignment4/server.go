package main

import "fmt"
import "os"
import "net"


func serverMain(rafts []RaftNode) {
 
	fservers := make([]*FileService, 3)

	for i:=0; i<3; i++ {
		defer rafts[i].lg.Close()
		go rafts[i].processEvents()
	}

	var max_clients int = 1000

	var cArray []*ClientHandler
	var rArray []chan Response
	rArray = make([]chan Response, max_clients) // Max clients

	for i:=0; i<max_clients; i++ {
		rArray[i] = make(chan Response)
	}

	for i:=0; i<3; i++ {
		fservers[i] = NewFS(i, rArray, rafts[i].CommitChannel())
		go fservers[i].Listen()
	}

	// Listen for TCP connection on port 8080
	ln, err := net.Listen("tcp", ":8080")
	if err != nil {
		fmt.Println("Error listening:", err.Error())
		os.Exit(1)
	}

	// Keep listening for new connections
	for i := int64(0); ;i++ {
		conn, err := ln.Accept()
		if err != nil {
			fmt.Println("Error accepting: ", err.Error())
			os.Exit(1)
		}
		c := &ClientHandler{i, rArray[int(i)]}
		cArray = append(cArray, c)

		// Handle new connection in a new thread
		go cArray[i].handleConnection(conn, rafts)
	}

}

type Msg struct {
	// Kind = the first character of the command. For errors, it
	// is the first letter after "ERR_", ('V' for ERR_VERSION, for
	// example), except for "ERR_CMD_ERR", for which the kind is 'M'
	Id		int64
	Kind     int
	Filename string
	Contents []byte
	Exptime  int64 // expiry time in seconds
	Version  int64
}

type Response struct {
	Output string
	Err bool
	File []byte
}

func main(){
	rafts, _ := makeMockRafts() // array of []raft.Node
	serverMain(rafts)
}

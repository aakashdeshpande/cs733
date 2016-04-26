Aakash Deshpande
120050005
==========================================

CS733
==========================================

This repository contains assignments for the course CS733. 
Assignments are written in golang.

Repository must be added to the src folder of the golang workspace.

Leveldb package for Golang must be imported. This can be done using the following command:
go get github.com/syndtr/goleveldb/leveldb

1] Assignment 1 : File Versioning system
	
	a] server.go
	This file contains serverMain() routine which initiates a server listening on localhost, port 8080
	Server can perform following operations:
		1.Write to file
		2.Compare version and swap file
		3.Read file
		4.Delete file

	b] server_test.go
	Contains test cases to ensure that server behaviour is consistent with the requirements

	In the assignment1 directory, 
	"go run server.go" initiates the server
	"go test" performs the test written in server_test.go

2] Assignment 2 : Raft State Machine

	a] raft.go
	Contains a description of the state machine with the following structs:
		1. events: Interface for output events such as Send, Alarm etc. sent on actionCh
		2. command: Interface for input events such as VoteReq, AppendEntriesReq, Timeout etc. Messages from other servers come on the netCh. Timeouts are deliverd on timeCh
		3. stateMachine : Implements all the methods of raft state machine 

	b] raft_test.go	
	Contains test cases to ensure that state machine behaviour is consistent with the requirements

	In the assignment2 directory, 
	"go test" performs the test written in raft_test.go

3] Assignment 3 : Raft Node

	a] raft.go
	Contains a description of the Raft state machine as described in the previous assignment

	b] node.go
	Contains a description of a RaftNode which implements output events of the state machine, interacts with the client and other RaftNodes. Also, contains functions to generate a cluster of RaftNodes or a mock cluster of RaftNodes

	b] node_test.go	
	Contains test cases to ensure that RaftNode behaviour is consistent with the requirements

	In the assignment3 directory, 
	"go test" performs the test written in node_test.go	

4] Distributed file system

	We work on a file system with the following system design/layers -

	a] Client handlers : Frontend for every client. Composed of the file ClientHandler.go which is described in detail in the section on Assignment1.
	b] Raft Block : For replication. Attached to a file system
		1. Node.go : Wrapper on the Raft state machine which listens to Client Handlers, and passes Raft output to File system. Described in detail in the 				section on Assignment2.
		2. Raft.go : Raft state machine which processes the messages recieved by the Node. Described in detail in the section on Assignment3.
	c] File system : Independent file system which implements the instructions passed on by the Raft Node. Ensures consistency through locking.
					 Passes output to the appropriate Client Handler.	
	d] Server : Setup and initialisation of the cluster and file system blocks. Entry point in the code.				 

	This Distributed File System can perform the following operations:
		1.Write to file
		2.Compare version and swap file
		3.Read file
		4.Delete file

	The File System mechanism replicates writes over all Raft + FS blocks. Thus, it provides fault tolerance and replication. It ensures Strong Consistency of the data through linearization of writes at the Raft level.					 
	Reads are not replicated, but can be processed by any node. Thus it provides performance benifits as well.

	In the assignment4 directory, 
	"go test" performs the test written in server_test.go

	Tests check the following -
	a] Basic read/write functionality 
	b] Version increment under write/CAS
	c] Leader shutdown and reelection
	d] Concurrent write by multiple clients





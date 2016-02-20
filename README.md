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

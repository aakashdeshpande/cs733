Aakash Deshpande
120050005
==========================================

CS733
==========================================

This repository contains assignments for the course CS733. 
Assignments are written in golang.

Repository must be added to the src folder of the golang workspace.

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

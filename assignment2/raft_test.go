package main

import (
	"testing"
	"time"
)

//import "fmt"

var clientCh chan command = make(chan command)

func TestMain(m *testing.M) {
	//go serverMain(clientCh) // launch the server as a goroutine.
	//time.Sleep(1 * time.Second)
	m.Run()
}

func TestCommit(t *testing.T) {

	var clientCh chan command = make(chan command)
	var netCh chan command = make(chan command)
	var actionCh chan events = make(chan events)
	var timeCh chan bool = make(chan bool)
	sm := NewStateMachine(5, 0, netCh, clientCh, timeCh, actionCh)
	go sm.eventLoop()

	sm.timeCh <- true

	for i:=0; i<5; i++ {
		<- actionCh
	}

	netCh <- NewVoteResp(sm.term, true)
	time.Sleep(time.Second)
	if sm.status != "Candidate" {
		t.Error("Status incorrect")
	}

	netCh <- NewVoteResp(sm.term, true)
	time.Sleep(time.Second)
	if sm.status != "Leader" {
		t.Error("Status incorrect")
	}

	for i:=0; i<5; i++ {
		<- actionCh
	}

	clientCh <- NewAppend([]byte("Msg"))

	for i:=0; i<5; i++ {
		<- actionCh
	}

	netCh <- NewAppendEntriesResp(1, sm.term, 0, true)
	netCh <- NewAppendEntriesResp(2, sm.term, 0, true)

	c := <- actionCh
	if c.eventName() != "Commit" {
		t.Error("Commit incorrect")
	}

	return
}

// Serial check of leader and commit
/*
func TestRaftSimple(t *testing.T) {

	time.Sleep(time.Second)

	clientCh <- NewAppend([]byte("Msg"))

    time.Sleep(5*time.Second)

    clientCh <- NewAppend([]byte("Msg2"))
    clientCh <- NewAppend([]byte("Msg3"))

    time.Sleep(10*time.Second)
}
*/

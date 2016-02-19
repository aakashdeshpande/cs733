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

func TestStateChange(t *testing.T) {

	var clientCh chan command = make(chan command)
	var netCh chan command = make(chan command)
	var actionCh chan events = make(chan events)
	var timeCh chan bool = make(chan bool)
	sm := NewStateMachine(5, 0, netCh, clientCh, timeCh, actionCh)
	go sm.eventLoop()

	// Follower timeout
	timeCh <- true

	for i:=0; i<5; i++ {
		<- actionCh
	}

	// Candidate election
	netCh <- NewVoteResp(sm.term, true)
	time.Sleep(time.Second)
	if sm.status != "Candidate" {
		t.Error("Status incorrect")
	}

	// Leader election
	netCh <- NewVoteResp(sm.term, true)
	time.Sleep(time.Second)
	if sm.status != "Leader" {
		t.Error("Status incorrect")
	}

	for i:=0; i<5; i++ {
		<- actionCh
	}

	// Append new message
	clientCh <- NewAppend([]byte("Msg"))

	for i:=0; i<5; i++ {
		<- actionCh
	}

	// Check log commit on majority and not before
	netCh <- NewAppendEntriesResp(1, sm.term, 0, true)
	netCh <- NewAppendEntriesResp(2, sm.term, 0, true)

	c := <- actionCh
	if c.eventName() != "Commit" {
		t.Error("Commit incorrect")
	}

	// Leader change
	netCh <- NewAppendEntriesResp(3, sm.term + 1, 0, false)

	c = <- actionCh
	if sm.status != "Follower" {
		t.Error("Status incorrect")
	}
	if sm.votedFor != -1 {
		t.Error("Voting incorrect")
	}
	if c.eventName() != "Alarm" {
		t.Error("Status incorrect")
	}

	// Vote request for term that has not seen voting
	netCh <- NewVoteReq(4, sm.term, 4, 0, 2)

	c = <- actionCh
	c = <- actionCh	
	if c.eventName() != "Alarm" {
		t.Error("Status incorrect")
	}

	netCh <- NewAppendEntriesReq(4, sm.term, 0, 2, nil, 2, 0)
	c = <- actionCh

	return
}

func TestRewrite(t *testing.T) {

	var clientCh chan command = make(chan command)
	var netCh chan command = make(chan command)
	var actionCh chan events = make(chan events)
	var timeCh chan bool = make(chan bool)
	sm := NewStateMachine(5, 0, netCh, clientCh, timeCh, actionCh)
	go sm.eventLoop()

	timeCh <- true

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

	// Previous log term dont match, no spurious commit
	netCh <- NewAppendEntriesReq(4, sm.term + 1, 0, sm.term + 1, nil, sm.term + 1, 0)

	<- actionCh
	<- actionCh

	if sm.term != 2 {
		t.Error("Term incorrect")
	}

	// Check if previous entry is overwritten by new entry sent by the leader
	netCh <- NewAppendEntriesReq(4, sm.term, -1, sm.term, []byte("Other Msg"), sm.term, 0)

	<- actionCh
	<- actionCh
	<- actionCh
	<- actionCh

	if string(sm.log[0]) != "Other Msg" {
		t.Error("Log incorrect")
	} 

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

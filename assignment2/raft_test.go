package main

import (
	"testing"
	//"time"
)

//import "fmt"

var clientCh chan command = make(chan command)

func TestMain(m *testing.M) {
	//go serverMain(clientCh) // launch the server as a goroutine.
	//time.Sleep(1 * time.Second)
	m.Run()
}

// State changes under appropriate conditions from follower to candidate, candidate to leader and vice versa
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
	<- actionCh

	netCh <- NewVoteResp(1, sm.term, false)
	<- actionCh
	netCh <- NewVoteResp(2, sm.term, false)
	<- actionCh
	netCh <- NewVoteResp(3, sm.term, false)

	

	// Check election rejection 
	c := <- actionCh	
	if c.eventName() != "Alarm" {
		t.Error("Timeout incorrect")
	}
	
	if sm.status != "Follower" {
		t.Error("Status incorrect")
	}
	<- actionCh

	// Follower timeout
	timeCh <- true

	for i:=0; i<5; i++ {
		<- actionCh
	}
	<- actionCh

	// Candidate election
	netCh <- NewVoteResp(1, sm.term, true)
	<- actionCh
	// Leader election
	netCh <- NewVoteResp(2, sm.term, true)
	
	for i:=0; i<5; i++ {
		<- actionCh
	}

	if sm.status != "Leader" {
		t.Error("Status incorrect")
	}
	<- actionCh

	// Append new message
	clientCh <- NewAppend([]byte("Msg"))

	for i:=0; i<5; i++ {
		<- actionCh
	}
	<- actionCh

	// Check log commit on majority and not before
	netCh <- NewAppendEntriesResp(1, sm.term, 0, true)
	<- actionCh
	netCh <- NewAppendEntriesResp(2, sm.term, 0, true)

	c = <- actionCh
	if c.eventName() != "Commit" {
		t.Error("Commit incorrect")
	}
	<- actionCh

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
	<- actionCh

	// Vote request for term that has not seen voting
	netCh <- NewVoteReq(4, sm.term, 4, 0, 2)

	c = <- actionCh
	c = <- actionCh	
	if c.eventName() != "Alarm" {
		t.Error("Status incorrect")
	}
	<- actionCh

	netCh <- NewAppendEntriesReq(4, sm.term, 0, 2, nil, 2, 0)
	c = <- actionCh
	<- actionCh

	return
}

// Check log overwritten correctly by new leader and no spurious commits occur
func TestRewrite(t *testing.T) {

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
	<- actionCh

	netCh <- NewVoteResp(1, sm.term, true)
	<- actionCh
	netCh <- NewVoteResp(2, sm.term, true)

	for i:=0; i<5; i++ {
		<- actionCh
	}

	// Leader election
	if sm.status != "Leader" {
		t.Error("Status incorrect")
	}
	<- actionCh

	clientCh <- NewAppend([]byte("Msg"))

	for i:=0; i<5; i++ {
		<- actionCh
	}
	<- actionCh

	// Previous log term doesnt match, no spurious commit
	netCh <- NewAppendEntriesReq(4, sm.term + 1, 0, sm.term + 1, []byte("Other Msg"), sm.term + 1, 0)

	<- actionCh
	<- actionCh

	if sm.term != 2 {
		t.Error("Term incorrect")
	}

	if sm.commitIndex != -1 {
		t.Error("Commit incorrect")
	}
	<- actionCh

	// Check if previous entry is overwritten by new entry sent by the leader
	netCh <- NewAppendEntriesReq(4, sm.term, -1, sm.term, []byte("Other Msg"), sm.term, 0)

	<- actionCh
	<- actionCh
	<- actionCh
	<- actionCh

	if string(sm.log[0]) != "Other Msg" {
		t.Error("Log incorrect")
	} 

	if sm.commitIndex != 0{
		t.Error("Commit incorrect")
	}
	<- actionCh

	// Check if new leader entries are placed appropriately
	netCh <- NewAppendEntriesReq(3, sm.term + 1, 0, sm.term, []byte("Msg"), sm.term + 1, 1)

	<- actionCh
	<- actionCh
	<- actionCh
	<- actionCh

	if string(sm.log[1]) != "Msg" {
		t.Error("Log incorrect")
	} 

	if sm.commitIndex != 1{
		t.Error("Commit incorrect")
	}
	<- actionCh

}

// Check montonicity and correctness of logTerm and commitIndex
func TestLogMonotonicity(t *testing.T) {

	var clientCh chan command = make(chan command)
	var netCh chan command = make(chan command)
	var actionCh chan events = make(chan events)
	var timeCh chan bool = make(chan bool)
	sm := NewStateMachine(5, 0, netCh, clientCh, timeCh, actionCh)
	go sm.eventLoop()

	// Append entries request with a gap
	netCh <- NewAppendEntriesReq(4, sm.term, 0, 0, nil, 0, 1)

	<- actionCh	
	<- actionCh

	// Commit monotonicity and correctness
	if sm.commitIndex != -1 {
		t.Error("Commit incorrect")
	}
	<- actionCh

	// Correct append entries request 
	netCh <- NewAppendEntriesReq(4, sm.term, -1, sm.term, []byte("Msg"), sm.term, -1)

	<- actionCh
	<- actionCh
	<- actionCh

	if string(sm.log[0]) != "Msg" {
		t.Error("Log incorrect")
	} 

	// Commit monotonicity and correctness
	if sm.commitIndex != -1 {
		t.Error("Commit incorrect")
	}
	<- actionCh

	// Follower timeout
	timeCh <- true

	for i:=0; i<5; i++ {
		<- actionCh
	}
	<- actionCh

	netCh <- NewVoteResp(1, sm.term, true)
	<- actionCh
	netCh <- NewVoteResp(2, sm.term, true)

	for i:=0; i<5; i++ {
		<- actionCh
	}
	<- actionCh

	// Leader election
	if sm.status != "Leader" {
		t.Error("Status incorrect")
	}

	// Check log commit on majority and not before
	netCh <- NewAppendEntriesResp(1, sm.term, 0, true)
	<- actionCh	
	netCh <- NewAppendEntriesResp(2, sm.term, 0, true)

	// Must commit only if entry of current term has majority
	if sm.commitIndex != -1 {
		t.Error("Commit incorrect")
	}
	<- actionCh

	return

}

// Checking leader log update when some responses are droppped
func TestLogResponse(t *testing.T) {

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
	<- actionCh

	netCh <- NewVoteResp(1, sm.term, true)
	<- actionCh
	netCh <- NewVoteResp(2, sm.term, true)

	for i:=0; i<5; i++ {
		<- actionCh
	}

	// Leader election
	if sm.status != "Leader" {
		t.Error("Status incorrect")
	}
	<- actionCh

	netCh <- NewAppend([]byte("Msg1"))

	for i:=0; i<5; i++ {
		<- actionCh
	}
	<- actionCh

	netCh <- NewAppend([]byte("Msg2"))

	for i:=0; i<5; i++ {
		<- actionCh
	}
	<- actionCh

	// Accepting later entry, some responses dropped
	netCh <- NewAppendEntriesResp(2, sm.term, 1, true)
	<- actionCh
	netCh <- NewAppendEntriesResp(3, sm.term, 1, true)

	<- actionCh

	// Commit monotonicity and correctness
	if sm.commitIndex != 1 {
		t.Error("Commit incorrect")
	}
	<- actionCh	
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

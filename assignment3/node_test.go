package main

import (
	//"fmt"
	"testing"
	"time"
)

func TestMain(m *testing.M) {
	m.Run()
}

/**************************************************************/
// Usage of cluster with TCP port assigned to each node

// Log, Term and VotedFor initialisation Initialisation. Append entry and Commit info check 
// State Machine shutdown, re-election and future Append response
func TestRaft(t *testing.T) {

	termReset()

	rafts := makeRafts() // array of []raft.Node 

	for i:=0; i<5; i++ {
		defer rafts[i].lg.Close()
		go rafts[i].processEvents()
	}

	time.Sleep(1*time.Second)
	var ldr *RaftNode
	for {
		ldr = getLeader(rafts)
		if (ldr != nil) { 
			break
		}
	}
	
	ldr.Append([]byte("foo"))
	time.Sleep(1*time.Second)

	for _, node := range rafts { 
		select {
			case ci := <- node.CommitChannel():
				if ci.Err != nil {t.Fatal(ci.Err)} 
				if string(ci.Data) != "foo" {
					t.Fatal("Got different data")
				} else{
					//fmt.Println("Proper Commit ", ci.Index)	
				}
			default: //fmt.Println("Expected message on all nodes")
		}
	}

	ldr.ShutDown()
	id := ldr.Id()

	time.Sleep(2*time.Second)
	for {
		ldr = getLeader(rafts)
		if (ldr != nil) { 
			break
		}
	}
	//fmt.Println("Leader is", ldr.Id())

	ldr.Append([]byte("fooAgain"))
	time.Sleep(1*time.Second)

	select {
		case ci := <- ldr.CommitChannel():
			if ci.Err != nil {t.Fatal(ci.Err)} 
			if string(ci.Data) != "fooAgain" {
				t.Fatal("Got different data")
			} else{
				//fmt.Println("Proper Commit ", ci.Index)	
			}
		default: //fmt.Println("Expected message on all nodes")
	}

	for _, node := range rafts { 
		if (node.Id() != id) {node.ShutDown()}
	}

}

/*
// State Machine restart, term and log initialisation
func TestShutDown(t *testing.T) {

	termReset()

	rafts := makeRafts() // array of []raft.Node 

	for i:=0; i<5; i++ {
		defer rafts[i].lg.Close()
		go rafts[i].processEvents()
	}

	time.Sleep(1*time.Second)
	var ldr *RaftNode
	for {
		ldr = getLeader(rafts)
		if (ldr.Id() != -1) { 
			break
		}
	}
	
	ldr.Append([]byte("foo"))
	time.Sleep(3*time.Second)

	for _, node := range rafts { 
		node.ShutDown()
	}

	time.Sleep(1*time.Second)

	rafts2 := makeRafts() // array of []raft.Node

	for i:=0; i<5; i++ {
		defer rafts2[i].lg.Close()
		go rafts2[i].processEvents()
	}

	time.Sleep(1*time.Second)
	ldr = getLeader(rafts)
	b, _ := ldr.Get(0)
	//fmt.Println(ldr.Id(), " ", ldr.CommittedIndex(), " ", string(b))

	for _, node2 := range rafts2 { 
		node2.ShutDown()
	}
}
*/

/**************************************************************/
// Usage of mock cluster for testing

// Shutdown and restart of all nodes
// Check if Log, Term and VotedFor is stored, flushed and then initialised using previous data
// Append on top of initialised log
func TestShutDown(t *testing.T) {

	termReset()

	rafts, _ := makeMockRafts() // array of []raft.Node 

	for i:=0; i<5; i++ {
		defer rafts[i].lg.Close()
		go rafts[i].processEvents()
	}

	time.Sleep(1*time.Second)
	var ldr *RaftNode
	for {
		ldr = getLeader(rafts)
		if (ldr != nil) { 
			break
		}
	}

	ldr.Append([]byte("foo"))
	time.Sleep(2*time.Second)

	for _, node := range rafts { 
		select {
			case ci := <- node.CommitChannel():
				if ci.Err != nil {t.Fatal(ci.Err)} 
				if string(ci.Data) != "foo" {
					t.Fatal("Got different data")
				} 
			default: //fmt.Println("Expected message on all nodes")
		}
	}

	for _, node := range rafts { 
		node.ShutDown()
	}

	time.Sleep(1*time.Second)

	rafts2, _ := makeMockRafts() // array of []raft.Node 

	for i:=0; i<5; i++ {
		defer rafts2[i].lg.Close()
		go rafts2[i].processEvents()
	}

	time.Sleep(1*time.Second)
	for {
		ldr = getLeader(rafts2)
		if (ldr != nil) { 
			break
		}
	}

	ldr.Append([]byte("fooAgain"))
	time.Sleep(2*time.Second)

	for _, node := range rafts2 { 
		select {
			case ci := <- node.CommitChannel():
				if ci.Err != nil {t.Fatal(ci.Err)} 
				if string(ci.Data) != "fooAgain" {
					t.Fatal("Got different data")
				} 
			default: //fmt.Println("Expected message on all nodes")
		}
	}

	// Leader should have seen previous log entry from its log on disk
	b, _ := ldr.Get(1)
	if string(b) != "fooAgain" {t.Fatal("Log not initialised correctly")} 

	for _, node := range rafts2 { 
		node.ShutDown()
	}
}

// Test Append on majority
// Leader election on leader shutdown and leader term monotonicity
// Overwriting log to match leader
func TestPartitions(t *testing.T) {

	termReset()

	rafts, cluster := makeMockRafts() // array of []raft.Node 

	for i:=0; i<5; i++ {
		defer rafts[i].lg.Close()
		go rafts[i].processEvents()
	}

	time.Sleep(1*time.Second)
	var ldr *RaftNode
	for {
		ldr = getLeader(rafts)
		if (ldr != nil) { 
			break
		}
	}

	ldr.Append([]byte("foo"))
	time.Sleep(2*time.Second)

	for _, node := range rafts { 
		select {
			case ci := <- node.CommitChannel():
				if ci.Err != nil {t.Fatal(ci.Err)} 
				if string(ci.Data) != "foo" {
					t.Fatal("Got different data")
				} 
			default: 
		}
	}

	for {
		ldr = getLeader(rafts)
		if (ldr != nil) { 
			break
		}
	}

	// Ensure current leader remains in minority
	if(ldr.Id() < 2) {
		cluster.Partition([]int{0, 1}, []int{2, 3, 4})
	} else if(ldr.Id() > 2)  {
		cluster.Partition([]int{0, 1, 2}, []int{3, 4})
	} else {
		cluster.Partition([]int{0, 1, 3}, []int{2, 4})
	}

	// Not commited as leader doesnt have majority
	ldr.Append([]byte("fooAgain"))
	time.Sleep(2*time.Second)

	for _, node := range rafts { 
		select {
			case ci := <- node.CommitChannel():
				t.Fatal(ci.Err)
			default: 
		}
	}

	cluster.Heal()

	time.Sleep(2*time.Second)
	for {
		ldr = getLeader(rafts)
		if (ldr != nil) { 
			break
		}
	}

	// Leader will not have "fooAgain" entry, will force new entry to all nodes
	ldr.Append([]byte("fooThrice"))
	time.Sleep(2*time.Second)

	for _, node := range rafts { 
		select {
			case ci := <- node.CommitChannel():
				if string(ci.Data) != "fooThrice" {
					t.Fatal("Got different data")
				} 			
		}
	}

	for _, node := range rafts { 
		node.ShutDown()
	}

}
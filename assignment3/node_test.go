package main

import (
	"fmt"
	"testing"
	"time"
)

func TestMain(m *testing.M) {
	m.Run()
}

// Log Initialisation, Append and Commit
func TestAppend(t *testing.T) {

	rafts := makeRafts() // array of []raft.Node 

	for i:=0; i<3; i++ {
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
		select {
			case ci := <- node.CommitChannel():
				if ci.Err != nil {t.Fatal(ci.Err)} 
				if string(ci.Data) != "foo" {
					t.Fatal("Got different data")
				} else{
					fmt.Println("Proper Commit ", ci.Index)	
				}
			default: fmt.Println("Expected message on all nodes")
		}
	}
}

// Stae Machine restart, term and log initialisation
func TestShutDown(t *testing.T) {

	rafts := makeRafts() // array of []raft.Node 

	for i:=0; i<3; i++ {
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

	for i:=0; i<3; i++ {
		defer rafts2[i].lg.Close()
		go rafts2[i].processEvents()
	}

	time.Sleep(1*time.Second)
	ldr = getLeader(rafts)
	b, _ := ldr.Get(0)
	fmt.Println(ldr.Id(), " ", ldr.CommittedIndex(), " ", string(b))
}
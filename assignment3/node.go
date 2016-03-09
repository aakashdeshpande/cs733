package raft

import "time"
import "github.com/cs733-iitb/log"
import "github.com/cs733-iitb/cluster"
import "strconv"
import "github.com/syndtr/goleveldb/leveldb"

// data goes in via Append, comes out as CommitInfo from the node's CommitChannel // Index is valid only if err == nil
type CommitInfo struct { 
	Data []byte
	Index int64  
	Err error // Err can be errred
}

// This is an example structure for Config .. change it to your convenience.
type NodeConfig struct {
	cluster cluster.Config // Information about all servers, including this.
	Id 		int // this node's id. One of the cluster's entries should match.
	LogDir 	string // Log file directory for this node
	ElectionTimeout  int
	HeartbeatTimeout int
}

type Node interface {
	// Client's message to Raft node 
	Append([]byte)

    // A channel for client to listen on. What goes into Append must come out of here at some point.
	CommitChannel() <- chan CommitInfo

	// Last known committed index in the log. This could be -1 until the system stabilizes.
	CommittedIndex() int64

	// Returns the data at a log index, or an error.
    Get(index int64) (error, []byte)
	
	// Node's id
	Id() int

	// Id of leader. -1 if unknown
    LeaderId() int
    
    // Signal to shut down all goroutines, stop sockets, flush log and close it, cancel timers.
	Shutdown() 
}

var configs cluster.Config = cluster.Config{
        Peers: []cluster.PeerConfig{
            {Id: 0, Address: "localhost:7070"},
            {Id: 1, Address: "localhost:8080"},
            {Id: 2, Address: "localhost:9090"},
        }}


/**************************************************************/

type RaftNode struct { // implements Node interface
	sm       *StateMachine
	timer    *time.Timer
	clientCh chan command
	actionCh chan events
	commitCh chan CommitInfo
	timeCh 	 chan bool
	cluster  cluster.Config
	server 	 cluster.Server
	LogDir 	 string // Log file directory for this node
	lg 		 *log.Log
}

func New(config NodeConfig) RaftNode {
	var rn RaftNode 
	rn.cluster = config.cluster
	rn.server, _ = cluster.New(config.Id, rn.cluster)

	rn.LogDir = config.LogDir
	rn.lg, _ = log.Open(rn.LogDir + "/Log" + strconv.Itoa(config.Id))
    defer rn.lg.Close()

	rn.clientCh = make(chan command)
	rn.actionCh = make(chan events, len(rn.cluster.Peers) + 5)
	rn.timeCh = make(chan bool)
	rn.commitCh = make(chan CommitInfo)

	// Database to store currentTerm
	currentTerm, _ := leveldb.OpenFile("$GOPATH/src/github.com/aakashdeshpande/cs733/assignment3/currentTerm", nil)
	defer currentTerm.Close()
	// Database to store votedFor
	votedFor, _ := leveldb.OpenFile("$GOPATH/src/github.com/aakashdeshpande/cs733/assignment3/votedFor", nil)
	defer votedFor.Close()

	rn.sm = NewStateMachine(int64(len(rn.cluster.Peers)), int64(config.Id), rn.actionCh, config.ElectionTimeout, currentTerm, votedFor, rn.lg)

	return rn
}

func (rn *RaftNode) process(ev events) {
	if (ev.eventName() == "Alarm") {
		//ev, _ = ev.(Alarm)
		rn.timer.Reset(ev.(Alarm).duration)
	} else if (ev.eventName() == "LogStore") {
		ev, _ := ev.(LogStore)
		rn.lg.TruncateToEnd(ev.index)
		rn.lg.Append(ev.data)
	} else if (ev.eventName() == "Commit") {
		ev, _ := ev.(Commit)
		out := CommitInfo{ev.data, ev.index, ev.err}
		rn.commitCh <- out
	} else if (ev.eventName() == "Send") {
		ev, _ := ev.(Send)
    	go func(){
    		rn.server.Outbox() <- &cluster.Envelope{Pid: int(ev.to), Msg: ev.c}
    	}()
	}
}

func (rn *RaftNode) processEvents() {
	rn.timer = time.NewTimer(rn.sm.electionTimeout + time.Duration(r1.Intn(1000)))
	for { 
		var actions []events
		if rn.sm.status == "Leader" {
			select {
				case appendMsg := <- rn.clientCh :
					//fmt.Println("Started ", sm.id)
					appendMsg.execute(rn.sm)
				case envMsg := <- rn.server.Inbox() :	
					peerMsg := envMsg.Msg.(command)
					peerMsg.execute(rn.sm)	
				case <- rn.timer.C :
					rn.sm.Timeout()
			}
		} else {
			select {
				case envMsg := <- rn.server.Inbox() :	
					peerMsg := envMsg.Msg.(command)	
					peerMsg.execute(rn.sm)	
				case <- rn.timer.C :
					rn.sm.Timeout()	
			}
		}
 		for ev := range rn.actionCh {
        	actions = append(actions, ev)
    	}	    
    	for i:=0; i< len(actions); i++ {
	    	rn.process(actions[i])
	    }
	}
}

func (rn *RaftNode) Id() int {
	return int(rn.sm.id)
}

func (rn *RaftNode) Append(data []byte) {
    // Append new message
	rn.clientCh <- NewAppend(data)
}

func (rn *RaftNode) CommitChannel() <- chan CommitInfo {
	return rn.commitCh
}

func (rn *RaftNode) CommittedIndex() int64 {
	return rn.sm.commitIndex
}

func (rn *RaftNode) Get(index int64) ([]byte, error) {
	return rn.lg.Get(index)
}

func (rn *RaftNode) LeaderId() int {
	if (rn.sm.status == "Leader") {
		return int(rn.sm.id)
	} else {
		return -1
	}
}
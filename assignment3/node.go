package main

import "fmt"
import "math/rand"
import "time"
import "github.com/cs733-iitb/log"
import "github.com/cs733-iitb/cluster"
import "strconv"
import "github.com/syndtr/goleveldb/leveldb"
import "encoding/json"
import "sync"

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
	clientCh chan Command
	actionCh chan events
	commitCh chan CommitInfo
	quitCh 	 chan bool
	cluster  cluster.Config
	server 	 cluster.Server
	LogDir 	 string // Log file directory for this node
	lg 		 *log.Log
	mutex	 *sync.RWMutex
	logMutex *sync.RWMutex	
}

func New(config NodeConfig) RaftNode {
	var rn RaftNode 
	rn.cluster = config.cluster
	rn.server, _ = cluster.New(config.Id, rn.cluster)

	rn.LogDir = config.LogDir
	rn.lg, _ = log.Open(rn.LogDir + "/Log" + strconv.Itoa(config.Id))

	rn.clientCh = make(chan Command)
	rn.actionCh = make(chan events, len(rn.cluster.Peers) + 5)
	rn.quitCh = make(chan bool)
	rn.commitCh = make(chan CommitInfo, 100)

	// Database to store currentTerm
	currentTerm, _ := leveldb.OpenFile("$GOPATH/src/github.com/aakashdeshpande/cs733/assignment3/currentTerm", nil)
	defer currentTerm.Close()
	// Database to store votedFor
	votedFor, _ := leveldb.OpenFile("$GOPATH/src/github.com/aakashdeshpande/cs733/assignment3/votedFor", nil)
	defer votedFor.Close()

	rn.sm = NewStateMachine(int64(len(rn.cluster.Peers)), int64(config.Id), rn.actionCh, config.ElectionTimeout, currentTerm, votedFor, rn.lg)

	rn.mutex = &sync.RWMutex{}
	rn.logMutex = &sync.RWMutex{}
	return rn
}

func parse(name string, b []byte) Command {
	if(name == "VoteRequest"){
		var n VoteRequest
		json.Unmarshal(b, &n)
		return n
	} else if (name == "VoteResponse") {
		var n VoteResponse
		json.Unmarshal(b, &n)
		return n
	} else if (name == "AppendEntriesRequest") {
		var n AppendEntriesRequest
		json.Unmarshal(b, &n)
		return n
	} else if (name == "AppendEntriesResponse") {
		var n AppendEntriesResponse
		json.Unmarshal(b, &n)
		return n
	}
	return nil
}

func (rn *RaftNode) process(ev events) {
	if (ev.eventName() == "Alarm") {
		//ev, _ = ev.(Alarm)
		rn.timer.Reset(ev.(Alarm).duration)
	} else if (ev.eventName() == "LogStore") {
		ev, _ := ev.(LogStore)
		fmt.Println("Append ", rn.lg.GetLastIndex(), ev.Index, string(ev.Data))
		rn.logMutex.Lock()
		rn.lg.TruncateToEnd(ev.Index)
		rn.lg.Append(ev.Data)	
		rn.logMutex.Unlock()
	} else if (ev.eventName() == "Commit") {
		ev, _ := ev.(Commit)
		out := CommitInfo{ev.Data, ev.Index, ev.err}
		rn.commitCh <- out
	} else if (ev.eventName() == "Send") {
		ev, _ := ev.(Send)
		fmt.Println("Send ", ev.to, ev.eventName())
		b, _ := json.Marshal(ev.c)
		fmt.Println(string(b))
    	go func(){
    		rn.server.Outbox() <- &cluster.Envelope{Pid: int(ev.to), Msg: b}
    	}()
	}
}

func (rn *RaftNode) processEvents() {
	fmt.Println("Started ", rn.sm.id)
	rn.timer = time.NewTimer(rn.sm.electionTimeout + time.Duration(rand.Intn(1000)))
	for { 
		rn.mutex.Lock()
		var actions []events
		if rn.sm.status == "Leader" {
			select {
				case <- rn.quitCh :
					return
				case appendMsg := <- rn.clientCh :
					//fmt.Println("Started ", sm.id)
					appendMsg.execute(rn.sm)
				case envMsg := <- rn.server.Inbox() :	
					b := envMsg.Msg.([]byte)
					var temp Append
					json.Unmarshal(b, &temp)
					peerMsg := parse(temp.Name, b)
					peerMsg.execute(rn.sm)	
				case <- rn.timer.C :
					rn.sm.Timeout()
			}
		} else {
			select {
				case <- rn.quitCh :
					return
				case envMsg := <- rn.server.Inbox() :	
					b := envMsg.Msg.([]byte)
					var temp Append
					json.Unmarshal(b, &temp)
					peerMsg := parse(temp.Name, b)
					peerMsg.execute(rn.sm)	
				case <- rn.timer.C :
					rn.sm.Timeout()	
			}
		}
		rn.mutex.Unlock()
		var e Done
		rn.actionCh <- e
 		for {
 			ev := <- rn.actionCh
 			if(ev.eventName() == "Event Processed") {
 				break
 			}  
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
	rn.mutex.RLock()
	c := rn.sm.commitIndex
	rn.mutex.RUnlock()
	return c    
}

func (rn *RaftNode) Get(index int64) ([]byte, error) {
	rn.logMutex.RLock()
	c, err := rn.lg.Get(index)
	rn.logMutex.RUnlock()
	return c, err
}

func (rn *RaftNode) LeaderId() int {
	rn.mutex.RLock()
	if (rn.sm.status == "Leader") {
		rn.mutex.RUnlock()
		return int(rn.sm.id)
	} else {
		rn.mutex.RUnlock()
		return -1
	}
}

func (rn *RaftNode) ShutDown() {
	rn.quitCh <- true
	rn.timer.Stop()
	rn.lg.Close()
	rn.server.Close()
}

func makeRafts() []RaftNode {
	var r []RaftNode
	for i:=0; i<3; i++ {
		config := NodeConfig{configs, i, "$GOPATH/src/github.com/aakashdeshpande/cs733/assignment3/", 500, 500}
		r = append(r, New(config))
	}
	return r
}

func getLeader(r []RaftNode) *RaftNode {
	for _, node := range r {
		if(node.LeaderId() != -1) {
			return &node
		}
	}
	return nil
}

func main(){

	//var actionCh chan events = make(chan events)
	//serverMain(actionCh);

	rafts := makeRafts() // array of []raft.Node 
	for i:=0; i<3; i++ {
		defer rafts[i].lg.Close()
		go rafts[i].processEvents()
	}
	time.Sleep(1*time.Second)
	ldr := getLeader(rafts)
	ldr.Append([]byte("foo"))
	time.Sleep(3*time.Second)
	for _, node := range rafts { 
		select {
			case ci := <- node.CommitChannel():
				if ci.Err != nil {fmt.Println(ci.Err)} 
				if string(ci.Data) != "foo" {
					fmt.Println("Got different data")
				} else{
					fmt.Println("Proper Commit")	
				}
			//default: fmt.Println("Expected message on all nodes")
		}
	}
}


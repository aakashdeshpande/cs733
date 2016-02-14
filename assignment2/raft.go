package main

import "fmt"	
import "time"
import "math/rand"
//import "github.com/syndtr/goleveldb/leveldb"

var serverChannel []chan command
var	s1 rand.Source = rand.NewSource(time.Now().UnixNano())
var	r1 *rand.Rand = rand.New(s1)


func min(a int64, b int64) int64{
	if(a > b){ 
		return a
	} else{
		return b
	}
}

type command interface {
    commandName() string
    execute(sm *StateMachine)	  
}

// The request sent to a server to vote for a candidate to become a leader.
type VoteRequest struct {
	from          int64
	term          int64
	candidateId	  int64	
	lastLogIndex  int64
	lastLogTerm   int64
}

func NewVoteReq(from int64, term int64, candidateId int64, lastLogIndex int64, lastLogTerm int64) VoteRequest {
	var c VoteRequest
	c.from = from
	c.term = term
	c.candidateId = candidateId
	c.lastLogIndex = lastLogIndex 
	c.lastLogTerm = lastLogTerm 
	return c
}

// The response returned from a server after a vote for a candidate to become a leader.
type VoteResponse struct {
	term          int64
	voteGranted   bool
}

func NewVoteResp(term int64, voteGranted bool) VoteResponse {
	var c VoteResponse
	c.term = term
	c.voteGranted = voteGranted
	return c
} 

type AppendEntriesRequest struct {
	leaderId      int64
	term          int64
	lastLogIndex  int64
	lastLogTerm   int64
	entries 	  []byte
	leaderCommit  int64
}

func NewAppendEntriesReq(leaderId int64, term int64, lastLogIndex int64, lastLogTerm int64, entries []byte, leaderCommit int64) AppendEntriesRequest {
	var c AppendEntriesRequest
	c.leaderId = leaderId
	c.term = term
	c.lastLogIndex = lastLogIndex
	c.lastLogTerm = lastLogTerm
	c.entries = entries
	c.leaderCommit = leaderCommit
	return c
}

type AppendEntriesResponse struct {
	from	      int64
	term          int64
	index         int64
	success		  bool
}

func NewAppendEntriesResp(from int64, term int64, index int64, success bool) AppendEntriesResponse {
	var c AppendEntriesResponse
	c.from = from
	c.term = term
	c.index = index
	c.success = success
	return c
}

type Append struct {
	data          []byte
}

func NewAppend(data []byte) Append{
	var c Append
	c.data = data
	return c
}

type StateMachine struct {
	servers 	  int64
	id 			  int64
	status		  string
	term 		  int64
	lastLogIndex  int64
	lastLogTerm   int64
	log  		  [][]byte	
	logTerm 	  map[int64]int64
	votedFor 	  map[int64]int64  		// Should votedFor be linked to the term?
	commitIndex   int64		
	netCh		  chan command
	clientCh	  chan command	
	timer 	 	  *time.Timer
	electionTimeout time.Duration
	votesRecieved int64
	nextIndex	  map[int64]int64
	matchIndex	  map[int64]int64
	acksRecieved  map[int64]int64
}

func NewStateMachine(servers int64, id int64, netCh chan command, clientCh chan command, electionTimeout int) *StateMachine{
	var sm *StateMachine = new(StateMachine)
	sm.servers = servers
	sm.id = id
	sm.status = "Follower"
	sm.term = 0
	sm.lastLogIndex = -1
	sm.lastLogTerm = 0
	sm.log = make([][]byte, 10000000)
	sm.logTerm = make(map[int64]int64)
	sm.votedFor = make(map[int64]int64)
	sm.commitIndex = -1
	sm.netCh = netCh
	sm.clientCh = clientCh
	sm.electionTimeout = time.Millisecond * time.Duration(electionTimeout)
	sm.votesRecieved = 0
	sm.nextIndex = make(map[int64]int64) 
	sm.matchIndex = make(map[int64]int64)
	sm.acksRecieved = make(map[int64]int64)
	return sm		
}

func Send(id int64, c command) bool {
	go func(){
		serverChannel[id] <- c	
	}()
	return true	
}

func (sm *StateMachine) Alarm(duration time.Duration) {
	if sm.status == "Leader" {
		sm.timer.Reset(duration) //500 + r1.Intn(3000)
	} else if sm.status == "Candidate" {
		sm.timer.Reset((duration + time.Duration(r1.Intn(1000)))*5)
	} else {
		sm.timer.Reset(duration + time.Duration(r1.Intn(1000)))
	}
}

func (sm *StateMachine) Commit(index int64) {
	if sm.status == "Leader" {
		fmt.Println("Commited by leader ", sm.id, "index ", index)
	}
}

func (sm *StateMachine) LogStore(index int64, entries []byte) {
	sm.log[index] = entries
	sm.logTerm[index] = sm.term
}

func (sm *StateMachine) Timeout() {
	if sm.status == "Leader" {
		for i:=int64(0); i<sm.servers; i++ {
			if (i != sm.id) {
				Send(i, NewAppendEntriesReq(sm.id, sm.term, sm.lastLogIndex, sm.lastLogTerm, nil, sm.commitIndex))
			}
		}
		sm.Alarm(sm.electionTimeout)
	} else {
		sm.status = "Candidate"
		sm.term ++
		sm.votedFor[sm.term] = sm.id
		sm.votesRecieved = 1
		for i:=int64(0); i<sm.servers; i++ {
			if (i != sm.id) {
				Send(i, NewVoteReq(sm.id, sm.term, sm.id, sm.lastLogIndex, sm.lastLogTerm))
			}
		}
		sm.Alarm(sm.electionTimeout)	
	}	
}

func (c VoteRequest) commandName() string{
	return "VoteRequest"
}

func (msg VoteRequest) execute(sm *StateMachine) {
	_, ok := sm.votedFor[msg.term]
	if  sm.term <= msg.term && (ok == false || sm.votedFor[msg.term] == msg.from) && //changed
		(sm.lastLogTerm < msg.lastLogTerm || (sm.lastLogTerm == msg.lastLogTerm && sm.lastLogIndex <= msg.lastLogIndex)){ 
		sm.term = msg.term
		sm.status = "Follower"
		sm.votedFor[msg.term] = msg.from
		Send(msg.from, NewVoteResp(msg.term, true))
		sm.Alarm(sm.electionTimeout)
	} else{
		Send(msg.from, NewVoteResp(msg.term, false))
	}
}

func (c VoteResponse) commandName() string{
	return "VoteResponse"
}

func (msg VoteResponse) execute(sm *StateMachine) {
	if sm.status == "Candidate" {
		if sm.term == msg.term && msg.voteGranted {
			sm.votesRecieved ++	
			if sm.votesRecieved > sm.servers/2	{
				sm.status = "Leader"
				fmt.Println("Elected Leader ", sm.id)
				for i:=int64(0); i<sm.servers; i++ {
					if (i != sm.id) {
						sm.nextIndex[i] = sm.lastLogIndex + 1
						sm.matchIndex[i] = 0
						Send(i, NewAppendEntriesReq(sm.id, sm.term, sm.lastLogIndex, sm.lastLogTerm, nil, sm.commitIndex))
					}
				}
				for i:=int64(0); i<=sm.lastLogIndex; i++ {
					sm.acksRecieved[i] = 0
				}
				sm.Alarm(sm.electionTimeout)	
			}
		}	
	}
}

func (c AppendEntriesRequest) commandName() string{
	return "AppendEntriesRequest"
}

func (msg AppendEntriesRequest) execute(sm *StateMachine) {
	//fmt.Println("Response ", msg.leaderId, msg.term, msg.lastLogTerm, msg.lastLogIndex, sm.status, sm.term)
	if sm.term < msg.term || (sm.status == "Candidate" && sm.term == msg.term) {
		sm.status = "Follower"
		sm.term = msg.term
		sm.Alarm(sm.electionTimeout) // To reset only on success?
	} 

	if  sm.term > msg.term {
		Send(msg.leaderId, NewAppendEntriesResp(sm.id, sm.term, sm.lastLogIndex, false))
	} else if (sm.logTerm[msg.lastLogIndex] != msg.lastLogTerm)	{
		Send(msg.leaderId, NewAppendEntriesResp(sm.id, sm.term, sm.lastLogIndex, false))
	} else {
		if(msg.entries != nil){
			if  sm.lastLogIndex > msg.lastLogIndex {
				sm.lastLogIndex = msg.lastLogIndex
			}
			sm.LogStore(sm.lastLogIndex + 1, msg.entries)	
			sm.lastLogIndex ++
			sm.lastLogTerm = sm.term
			Send(msg.leaderId, NewAppendEntriesResp(sm.id, sm.term, sm.lastLogIndex, true))
		}

		if sm.commitIndex < msg.leaderCommit {	// with success or outside?
			sm.commitIndex = min(msg.leaderCommit, sm.lastLogIndex)
			sm.Commit(sm.commitIndex) //, data, err)
			//fmt.Println("Commited ", sm.id, sm.commitIndex) 
		}
		sm.Alarm(sm.electionTimeout) // To reset only on success?
	}
}

func (c AppendEntriesResponse) commandName() string{
	return "AppendEntriesResponse"
}

func (msg AppendEntriesResponse) execute(sm *StateMachine) {
	//fmt.Println("Response ", msg.leaderId, msg.term, msg.lastLogTerm, msg.lastLogIndex, sm.status, sm.term)
	if sm.status == "Leader" {
		if msg.success {
			sm.matchIndex[msg.from] = msg.index 
	        sm.nextIndex[msg.from] = msg.index + 1
	        sm.acksRecieved[msg.index] ++
	        if sm.acksRecieved[msg.index] > sm.servers/2 {
	        	if sm.commitIndex < msg.index && sm.term == sm.logTerm[msg.index] {
	        		sm.commitIndex = msg.index 
	        		sm.Commit(sm.commitIndex) //, data, err) Commit(msg.index - 1, data, err = nil)
	        	}
	        }
	    } else { //If AppendEntries fails because of log inconsistency
	        sm.nextIndex[msg.from] --
	        sm.nextIndex[msg.from] = min(sm.nextIndex[msg.from], 0)
	    }
	    if sm.lastLogIndex >= sm.nextIndex[msg.from] {
	    	Send(msg.from, NewAppendEntriesReq(sm.id, sm.term, sm.nextIndex[msg.from] - 1, sm.logTerm[sm.nextIndex[msg.from] - 1],
	    	sm.log[sm.nextIndex[msg.from]], sm.commitIndex))  
	    }
    } else{
    	if sm.term < msg.term {
			sm.status = "Follower"
			sm.term = msg.term
			sm.Alarm(sm.electionTimeout) // To reset only on success?
		}
    }
}

func (c Append) commandName() string{
	return "Append"
}

func (msg Append) execute(sm *StateMachine) {
	if sm.status == "Leader" {
		sm.LogStore(sm.lastLogIndex + 1, msg.data)
		sm.acksRecieved[sm.lastLogIndex + 1] = 1
	    for i:=int64(0); i<sm.servers; i++ {
			if (i != sm.id) {
				Send(i, NewAppendEntriesReq(sm.id, sm.term, sm.lastLogIndex, sm.lastLogTerm, msg.data, sm.commitIndex)) // Send next index
			}
		}
		sm.lastLogIndex ++		
		sm.lastLogTerm = sm.term
		//fmt.Println("Done Leader")
	} 
}

func (sm *StateMachine) eventLoop(){

    sm.timer = time.NewTimer(sm.electionTimeout + time.Duration(r1.Intn(1000))) 
	for { 
		if sm.status == "Leader" {
			select {
				case appendMsg := <- sm.clientCh :
					fmt.Println("Started ", sm.id)
					appendMsg.execute(sm)
				case peerMsg := <- sm.netCh :	
					peerMsg.execute(sm)	
				case <- sm.timer.C :
					sm.Timeout()
			}
		} else {
			select {
				case peerMsg := <- sm.netCh :	
					peerMsg.execute(sm)	
				case <- sm.timer.C :
					sm.Timeout()	
			}
		}
	}
}

func serverMain(clientCh chan command){
	var servers int64 = 5

	serverChannel = make([]chan command, servers)
	// Database to store currentTerm
	//currentTerm, _ := leveldb.OpenFile("$GOPATH/src/github.com/aakashdeshpande/cs733/assignment2/currentTerm", nil)
	// Database to store votedFor
	//votedFor, _ := leveldb.OpenFile("$GOPATH/src/github.com/aakashdeshpande/cs733/assignment2/votedFor", nil)

	for i :=int64(0); i<5; i++ {
		serverChannel[i] = make(chan command)
		sm := NewStateMachine(servers, i, serverChannel[i], clientCh, 500)
		go sm.eventLoop()
	}
}

func main(){
	var clientCh chan command = make(chan command)
	serverMain(clientCh);
}

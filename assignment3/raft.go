package raft

//import "fmt"	
import "time"
import "math/rand"
import "encoding/json"
import "strconv"
import "github.com/cs733-iitb/log"
import "github.com/syndtr/goleveldb/leveldb"

var	s1 rand.Source = rand.NewSource(time.Now().UnixNano())
var	r1 *rand.Rand = rand.New(s1)

func min(a int64, b int64) int64{
	if(a < b){ 
		return a
	} else{
		return b
	}
}

type LogInfo struct{
	data []byte
	term int64
}

/**************************************************************/

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
	from 		  int64
	term          int64
	voteGranted   bool
}

func NewVoteResp(from int64, term int64, voteGranted bool) VoteResponse {
	var c VoteResponse
	c.from = from
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
	entryTerm	  int64
	leaderCommit  int64
}

func NewAppendEntriesReq(leaderId int64, term int64, lastLogIndex int64, lastLogTerm int64, entries []byte, entryTerm int64, leaderCommit int64) AppendEntriesRequest {
	var c AppendEntriesRequest
	c.leaderId = leaderId
	c.term = term
	c.lastLogIndex = lastLogIndex
	c.lastLogTerm = lastLogTerm
	c.entries = entries
	c.entryTerm = entryTerm
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

/**************************************************************/

type events interface {
    eventName() string
}

type Send struct{
	to 	 int64
	c 	 command
}

func NewSend(id int64, c command) Send{
	var s Send
	s.to = id
	s.c = c
	return s
}

type Alarm struct{
	to 	 	 int64
	duration time.Duration
}

func NewAlarm(id int64, duration time.Duration) Alarm{
	var s Alarm
	s.to = id
	s.duration = duration
	return s
}

type Commit struct{
	index 	 int64
	data 	 []byte
	err 	 error
}

func NewCommit(index int64, data []byte, err error) Commit{
	var s Commit
	s.index = index
	s.data = data
	s.err = err
	return s
}

type LogStore struct{
	index 	 int64
	data 	 []byte
}

func NewLogStore(index int64, data []byte) LogStore{
	var s LogStore
	s.index = index
	s.data = data
	return s
}

type Done struct{
}

/**************************************************************/

type StateMachine struct {
	servers 	  int64
	id 			  int64
	status		  string
	term 		  int64
	currentTerm   *leveldb.DB
	votedFor 	  int64  		// Should votedFor be linked to the term?
	voted         *leveldb.DB
	lastLogIndex  int64
	lastLogTerm   int64
	log  		  [][]byte	
	logTerm 	  map[int64]int64
	commitIndex   int64			
	//timer 	 	  *time.Timer
	actionCh	  chan events
	electionTimeout time.Duration
	votesMap	  map[int64]int
	nextIndex	  map[int64]int64
	matchIndex	  map[int64]int64
	//acksRecieved  map[int64]int64
}

func NewStateMachine(servers int64, id int64, actionCh chan events, electionTimeout int, currentTerm *leveldb.DB, voted *leveldb.DB, lg *log.Log) *StateMachine{
	var sm *StateMachine = new(StateMachine)
	sm.servers = servers
	sm.id = id
	sm.status = "Follower"
	sm.currentTerm = currentTerm
	term, err := currentTerm.Get([]byte(strconv.FormatInt(sm.id, 10)), nil)
	if err == nil {
		sm.term, _ = strconv.ParseInt(string(term), 10, 64)
	} else {
		sm.term = int64(0)
	}

	sm.voted = voted
	vote, err := voted.Get([]byte(strconv.FormatInt(sm.id, 10)), nil)
	if err == nil {
		sm.votedFor, _ = strconv.ParseInt(string(vote), 10, 64)
	} else {
		sm.votedFor = int64(-1)
	}

	sm.log = make([][]byte, 10000000)
	sm.logTerm = make(map[int64]int64)

	size := lg.GetLastIndex() + 1
	if (size == 0) {
		sm.lastLogIndex = -1
		sm.lastLogTerm = 0
	} else {
		for i:=int64(0); i<size; i++ {
			b, _ := lg.Get(i)
			var entry LogInfo
			json.Unmarshal(b, &entry)
			sm.log[i] = entry.data
			sm.logTerm[i] = entry.term
		}
		sm.lastLogIndex = size - 1
		sm.lastLogTerm = sm.logTerm[size - 1]
	}

	sm.commitIndex = -1
	sm.actionCh = actionCh
	sm.electionTimeout = time.Millisecond * time.Duration(electionTimeout)
	sm.votesMap = make(map[int64]int) 
	sm.nextIndex = make(map[int64]int64) 
	sm.matchIndex = make(map[int64]int64)
	//sm.acksRecieved = make(map[int64]int64)
	return sm		
}

/**************************************************************/

func (c Send) eventName() string{
	return "Send"
}

func (c Alarm) eventName() string{
	return "Alarm"
}

func (c Commit) eventName() string{
	return "Commit"
}

func (c LogStore) eventName() string{
	return "LogStore"
}

func (c Done) eventName() string{
	return "Event Processed"
}

/**************************************************************/


func (sm *StateMachine) countOnes() int64{
	var count int64 = 0
	for i:=int64(0); i<sm.servers; i++ {
			if (sm.votesMap[i] == 1) {
				count ++
			}
	}
	return count;
}

func (sm *StateMachine) countNeg() int64{
	var count int64 = 0
	for i:=int64(0); i<sm.servers; i++ {
			if (sm.votesMap[i] == -1) {
				count ++
			}
	}
	return count;
}

func (sm *StateMachine) Send(id int64, c command) bool {
	sm.actionCh <- NewSend(id, c)	
	return true	
}

func (sm *StateMachine) Alarm(duration time.Duration) {
	if sm.status == "Leader" {
		sm.actionCh <- NewAlarm(sm.id, duration)
	} else if sm.status == "Candidate" {
		sm.actionCh <- NewAlarm(sm.id, (duration + time.Duration(r1.Intn(1000)))*5)
	} else {
		sm.actionCh <- NewAlarm(sm.id, duration + time.Duration(r1.Intn(1000)))
	}
}

func (sm *StateMachine) Commit(index int64) {
	if(index != -1) {
		sm.actionCh <- NewCommit(index, sm.log[index], nil)
	} else {
		sm.actionCh <- NewCommit(index, nil, nil)
	}
}

func (sm *StateMachine) LogStore(index int64, entries []byte, entryTerm int64) {
	sm.log[index] = entries
	sm.logTerm[index] = entryTerm
	entry := LogInfo{ entries, entryTerm }
	b, _ := json.Marshal(entry)
	sm.actionCh <- NewLogStore(index, b)
}

/**************************************************************/

func (sm *StateMachine) Timeout() {
	if sm.status == "Leader" {
		for i:=int64(0); i<sm.servers; i++ {
			if (i != sm.id) {
				sm.Send(i, NewAppendEntriesReq(sm.id, sm.term, sm.lastLogIndex, sm.lastLogTerm, nil, sm.term, sm.commitIndex))
			}
		}
		sm.Alarm(sm.electionTimeout)
	} else {
		sm.status = "Candidate"
		sm.currentTerm.Put([]byte(strconv.FormatInt(sm.id, 10)), []byte(strconv.FormatInt(sm.term + 1, 10)), nil)
		sm.term ++
		sm.voted.Put([]byte(strconv.FormatInt(sm.id, 10)), []byte(strconv.FormatInt(sm.id, 10)), nil)
		sm.votedFor = sm.id
		for i:=int64(0); i<sm.servers; i++ {
			sm.votesMap[i] = 0
		}
		sm.votesMap[sm.id] = 1
		for i:=int64(0); i<sm.servers; i++ {
			if (i != sm.id) {
				sm.Send(i, NewVoteReq(sm.id, sm.term, sm.id, sm.lastLogIndex, sm.lastLogTerm))
			}
		}
		sm.Alarm(sm.electionTimeout)
	}	
}

/**************************************************************/

func (c VoteRequest) commandName() string{
	return "VoteRequest"
}

func (msg VoteRequest) execute(sm *StateMachine) {
	if  sm.term <= msg.term && (sm.votedFor == -1 || sm.votedFor == msg.from) && //changed
		(sm.lastLogTerm < msg.lastLogTerm || (sm.lastLogTerm == msg.lastLogTerm && sm.lastLogIndex <= msg.lastLogIndex)){ 
		sm.currentTerm.Put([]byte(strconv.FormatInt(sm.id, 10)), []byte(strconv.FormatInt(msg.term, 10)), nil)
		sm.term = msg.term
		sm.voted.Put([]byte(strconv.FormatInt(sm.id, 10)), []byte(strconv.FormatInt(msg.from, 10)), nil)
		sm.votedFor = msg.from
		sm.status = "Follower"
		
		sm.Send(msg.from, NewVoteResp(sm.id, msg.term, true))
		sm.Alarm(sm.electionTimeout)
	} else{
		sm.Send(msg.from, NewVoteResp(sm.id, msg.term, false))
	}
}

func (c VoteResponse) commandName() string{
	return "VoteResponse"
}

func (msg VoteResponse) execute(sm *StateMachine) {
	if sm.status == "Candidate" {
		if sm.term == msg.term && msg.voteGranted {
			sm.votesMap[msg.from] = 1
			if sm.countOnes() > sm.servers/2	{
				sm.status = "Leader"
				//fmt.Println("Elected Leader ", sm.id)
				for i:=int64(0); i<sm.servers; i++ {
					if (i != sm.id) {
						sm.nextIndex[i] = sm.lastLogIndex + 1
						sm.matchIndex[i] = -1
						sm.Send(i, NewAppendEntriesReq(sm.id, sm.term, sm.lastLogIndex, sm.lastLogTerm, nil, sm.term, sm.commitIndex))
					}
				}
				sm.Alarm(sm.electionTimeout)	
			}
		} else if sm.term == msg.term && !msg.voteGranted {
			sm.votesMap[msg.from] = -1
			if sm.countNeg() > sm.servers/2	{
				sm.status = "Follower"
				sm.voted.Put([]byte(strconv.FormatInt(sm.id, 10)), []byte(strconv.FormatInt(sm.id, 10)), nil)
				sm.votedFor = sm.id
				sm.Alarm(sm.electionTimeout) // To reset only on success?
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
		sm.currentTerm.Put([]byte(strconv.FormatInt(sm.id, 10)), []byte(strconv.FormatInt(msg.term, 10)), nil)
		sm.term = msg.term
		sm.voted.Put([]byte(strconv.FormatInt(sm.id, 10)), []byte(strconv.FormatInt(int64(-1), 10)), nil)
		sm.votedFor = -1
		sm.Alarm(sm.electionTimeout) // To reset only on success?
	} 

	// Check that nextIndex is more than commitIndex
	if  sm.term > msg.term {
		sm.Send(msg.leaderId, NewAppendEntriesResp(sm.id, sm.term, sm.lastLogIndex, false))
	} else if (msg.lastLogIndex != -1) && 
	(msg.lastLogIndex < sm.commitIndex || (msg.entries != nil && (msg.lastLogIndex > sm.lastLogIndex || sm.logTerm[msg.lastLogIndex] != msg.lastLogTerm))) {
		sm.Send(msg.leaderId, NewAppendEntriesResp(sm.id, sm.term, sm.lastLogIndex, false))
	} else {
		if(msg.entries != nil){
			if  sm.lastLogIndex > msg.lastLogIndex {
				sm.lastLogIndex = msg.lastLogIndex
			}
			sm.LogStore(sm.lastLogIndex + 1, msg.entries, msg.entryTerm)	
			sm.lastLogIndex ++
			sm.lastLogTerm = msg.entryTerm
			sm.Send(msg.leaderId, NewAppendEntriesResp(sm.id, sm.term, sm.lastLogIndex, true)) //If overwriting, should this have current term?
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
	if sm.term < msg.term && msg.success == false{
		sm.status = "Follower"
		sm.currentTerm.Put([]byte(strconv.FormatInt(sm.id, 10)), []byte(strconv.FormatInt(msg.term, 10)), nil)
		sm.term = msg.term
		sm.voted.Put([]byte(strconv.FormatInt(sm.id, 10)), []byte(strconv.FormatInt(int64(-1), 10)), nil)
		sm.votedFor = -1
		sm.Alarm(sm.electionTimeout) // To reset only on success?
	} else if sm.status == "Leader" {
		if msg.success {
			sm.matchIndex[msg.from] = msg.index 
	        sm.nextIndex[msg.from] = msg.index + 1

	        count := int64(1)
	        for i:=int64(0); i<sm.servers; i++ {
				if (i != sm.id) {
					if(sm.matchIndex[i] >= sm.matchIndex[msg.from]) { count++ }
				}
			}

	        if count > sm.servers/2 {
	        	if sm.commitIndex < msg.index && sm.term == sm.logTerm[msg.index] {
	        		sm.commitIndex = msg.index 
	        		sm.Commit(sm.commitIndex) 
	        	}
	        }
	    } else { //If AppendEntries fails because of log inconsistency
	        sm.nextIndex[msg.from] --
	        sm.nextIndex[msg.from] = min(sm.nextIndex[msg.from], 0)
	    }
	    if sm.lastLogIndex >= sm.nextIndex[msg.from] {
	    	sm.Send(msg.from, NewAppendEntriesReq(sm.id, sm.term, sm.nextIndex[msg.from] - 1, sm.logTerm[sm.nextIndex[msg.from] - 1],
	    	sm.log[sm.nextIndex[msg.from]], sm.logTerm[sm.nextIndex[msg.from]], sm.commitIndex))  
	    }
    } 
}

/**************************************************************/

func (c Append) commandName() string{
	return "Append"
}

func (msg Append) execute(sm *StateMachine) {
	if sm.status == "Leader" {
		sm.LogStore(sm.lastLogIndex + 1, msg.data, sm.term)
		//sm.acksRecieved[sm.lastLogIndex + 1] = 1
	    for i:=int64(0); i<sm.servers; i++ {
			if (i != sm.id) {
				sm.Send(i, NewAppendEntriesReq(sm.id, sm.term, sm.lastLogIndex, sm.lastLogTerm, msg.data, sm.term, sm.commitIndex)) // Send next index
			}
		}
		sm.lastLogIndex ++		
		sm.lastLogTerm = sm.term
	} 
}


func serverMain(actionCh chan events){
	var servers int64 = 5
	//for i :=int64(0); i<5; i++ {

	// Database to store currentTerm
	currentTerm, _ := leveldb.OpenFile("$GOPATH/src/github.com/aakashdeshpande/cs733/assignment3/currentTerm", nil)
	defer currentTerm.Close()
	// Database to store votedFor
	votedFor, _ := leveldb.OpenFile("$GOPATH/src/github.com/aakashdeshpande/cs733/assignment3/votedFor", nil)
	defer votedFor.Close()

	lg, _ := log.Open("$GOPATH/src/github.com/aakashdeshpande/cs733/assignment3/Log" + strconv.Itoa(0))

	NewStateMachine(servers, 0, actionCh, 500, currentTerm, votedFor, lg)
	//}
}

func main(){
	// Database to store currentTerm
	//currentTerm, _ := leveldb.OpenFile("$GOPATH/src/github.com/aakashdeshpande/cs733/assignment2/currentTerm", nil)
	// Database to store votedFor
	//votedFor, _ := leveldb.OpenFile("$GOPATH/src/github.com/aakashdeshpande/cs733/assignment2/votedFor", nil)

	var actionCh chan events = make(chan events)

	serverMain(actionCh);
}

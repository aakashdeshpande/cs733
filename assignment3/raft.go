package main

//import "fmt"	
import "time"
import "math/rand"
import "encoding/json"
import "strconv"
import "github.com/cs733-iitb/log"
import "github.com/syndtr/goleveldb/leveldb"

//var	s1 rand.Source = rand.NewSource(time.Now().UnixNano())
//var	r1 *rand.Rand = rand.New(s1)

func min(a int64, b int64) int64{
	if(a < b){ 
		return a
	} else{
		return b
	}
}

type LogInfo struct{
	Data []byte
	Term int64
}

/**************************************************************/

type Command interface {
    CommandName() string
    execute(sm *StateMachine)	  
}

// The request sent to a server to vote for a candidate to become a leader.
type VoteRequest struct {
	Name 		  string
	From          int64
	Term          int64
	CandidateID	  int64	
	LastLogIndex  int64
	LastLogTerm   int64
}

func NewVoteReq(From int64, Term int64, CandidateID int64, LastLogIndex int64, LastLogTerm int64) VoteRequest {
	var c VoteRequest
	c.Name = "VoteRequest"
	c.From = From
	c.Term = Term
	c.CandidateID = CandidateID
	c.LastLogIndex = LastLogIndex 
	c.LastLogTerm = LastLogTerm 
	return c
}

// The response returned From a server after a vote for a candidate to become a leader.
type VoteResponse struct {
	Name 		  string
	From 		  int64
	Term          int64
	VoteGranted   bool
}

func NewVoteResp(From int64, Term int64, VoteGranted bool) VoteResponse {
	var c VoteResponse
	c.Name = "VoteResponse"
	c.From = From
	c.Term = Term
	c.VoteGranted = VoteGranted
	return c
} 

type AppendEntriesRequest struct {
	Name 		  string
	LeaderID      int64
	Term          int64
	LastLogIndex  int64
	LastLogTerm   int64
	Entries 	  []byte
	EntryTerm	  int64
	LeaderCommit  int64
}

func NewAppendEntriesReq(LeaderID int64, Term int64, LastLogIndex int64, LastLogTerm int64, Entries []byte, EntryTerm int64, LeaderCommit int64) AppendEntriesRequest {
	var c AppendEntriesRequest
	c.Name = "AppendEntriesRequest"
	c.LeaderID = LeaderID
	c.Term = Term
	c.LastLogIndex = LastLogIndex
	c.LastLogTerm = LastLogTerm
	c.Entries = Entries
	c.EntryTerm = EntryTerm
	c.LeaderCommit = LeaderCommit
	return c
}

type AppendEntriesResponse struct {
	Name 		  string
	From	      int64
	Term          int64
	Index         int64
	Success		  bool
}

func NewAppendEntriesResp(From int64, Term int64, Index int64, Success bool) AppendEntriesResponse {
	var c AppendEntriesResponse
	c.Name = "AppendEntriesResponse"
	c.From = From
	c.Term = Term
	c.Index = Index
	c.Success = Success
	return c
}

type Append struct {
	Name 		  string
	Data          []byte
}

func NewAppend(Data []byte) Append{
	var c Append
	c.Data = Data
	return c
}

/**************************************************************/

type events interface {
    eventName() string
}

type Send struct{
	to 	 int64
	c 	 Command
}

func NewSend(id int64, c Command) Send{
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
	Index 	 int64
	Data 	 []byte
	err 	 error
}

func NewCommit(Index int64, Data []byte, err error) Commit{
	var s Commit
	s.Index = Index
	s.Data = Data
	s.err = err
	return s
}

type LogStore struct{
	Index 	 int64
	Data 	 []byte
}

func NewLogStore(Index int64, Data []byte) LogStore{
	var s LogStore
	s.Index = Index
	s.Data = Data
	return s
}

type Done struct{
}

/**************************************************************/

type StateMachine struct {
	servers 	  int64
	id 			  int64
	status		  string
	Term 		  int64
	currentTerm   *leveldb.DB
	votedFor 	  int64  		// Should votedFor be linked to the Term?
	voted         *leveldb.DB
	LastLogIndex  int64
	LastLogTerm   int64
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

func NewStateMachine(servers int64, id int64, actionCh chan events, electionTimeout int, lg *log.Log) *StateMachine{
	var sm *StateMachine = new(StateMachine)
	sm.servers = servers
	sm.id = id
	sm.status = "Follower"

	currentTerm, _ := leveldb.OpenFile("$GOPATH/src/github.com/aakashdeshpande/cs733/assignment3/currentTerm", nil)
	defer currentTerm.Close()
	// Database to store votedFor
	voted, _ := leveldb.OpenFile("$GOPATH/src/github.com/aakashdeshpande/cs733/assignment3/votedFor", nil)
	defer voted.Close()
	//sm.currentTerm = currentTerm
	//defer sm.currentTerm.Close()
	Term, err := currentTerm.Get([]byte(strconv.FormatInt(sm.id, 10)), nil)
	if err == nil {
		sm.Term, _ = strconv.ParseInt(string(Term), 10, 64)
	} else {
		sm.Term = int64(0)
	}

	//sm.voted = voted
	//defer sm.voted.Close()
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
		sm.LastLogIndex = -1
		sm.LastLogTerm = 0
	} else {
		for i:=int64(0); i<size; i++ {
			b, _ := lg.Get(i)
			var entry LogInfo
			json.Unmarshal(b.([]byte), &entry)
			sm.log[i] = entry.Data
			sm.logTerm[i] = entry.Term
		}
		sm.LastLogIndex = size - 1
		sm.LastLogTerm = sm.logTerm[size - 1]
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

func (sm *StateMachine) Send(id int64, c Command) bool {
	sm.actionCh <- NewSend(id, c)	
	return true	
}

func (sm *StateMachine) Alarm(duration time.Duration) {
	if sm.status == "Leader" {
		sm.actionCh <- NewAlarm(sm.id, duration/3)
	} else if sm.status == "Candidate" {
		//rm := rand.Intn(10000)
		rm := rand.Intn(100)
		//fmt.Println("Candidate Timeout ",sm.id, " ", rm)
		sm.actionCh <- NewAlarm(sm.id, duration*5 + time.Duration(rm)*time.Millisecond)
	} else {
		//rm := rand.Intn(1000)
		rm := rand.Intn(10)
		//fmt.Println("Follower Timeout ",sm.id, " ", rm)
		sm.actionCh <- NewAlarm(sm.id, duration + time.Duration(rm)*time.Millisecond)
	}
}

func (sm *StateMachine) Commit(Index int64) {
	if(Index != -1) {
		sm.actionCh <- NewCommit(Index, sm.log[Index], nil)
	} else {
		sm.actionCh <- NewCommit(Index, nil, nil)
	}
}

func (sm *StateMachine) LogStore(Index int64, Entries []byte, EntryTerm int64) {
	sm.log[Index] = Entries
	sm.logTerm[Index] = EntryTerm
	//fmt.Println("Log ", Index, string(Entries))
	entry := LogInfo{Entries, EntryTerm}
	b, _ := json.Marshal(entry)
	sm.actionCh <- NewLogStore(Index, b)
}

/**************************************************************/

func (sm *StateMachine) Timeout() {
	if sm.status == "Leader" {
		for i:=int64(0); i<sm.servers; i++ {
			if (i != sm.id) {
				sm.Send(i, NewAppendEntriesReq(sm.id, sm.Term, sm.LastLogIndex, sm.LastLogTerm, nil, sm.Term, sm.commitIndex))
			}
		}
		sm.Alarm(sm.electionTimeout)
	} else {
		sm.status = "Candidate"
		//sm.currentTerm.Put([]byte(strconv.FormatInt(sm.id, 10)), []byte(strconv.FormatInt(sm.Term + 1, 10)), nil)
		sm.Term ++
		//sm.voted.Put([]byte(strconv.FormatInt(sm.id, 10)), []byte(strconv.FormatInt(sm.id, 10)), nil)
		sm.votedFor = sm.id
		for i:=int64(0); i<sm.servers; i++ {
			sm.votesMap[i] = 0
		}
		sm.votesMap[sm.id] = 1
		for i:=int64(0); i<sm.servers; i++ {
			if (i != sm.id) {
				sm.Send(i, NewVoteReq(sm.id, sm.Term, sm.id, sm.LastLogIndex, sm.LastLogTerm))
			}
		}
		sm.Alarm(sm.electionTimeout)
	}	
}

/**************************************************************/

func (c VoteRequest) CommandName() string{
	return "VoteRequest"
}

func (msg VoteRequest) execute(sm *StateMachine) {
	if  (sm.Term < msg.Term || (sm.Term == msg.Term && (sm.votedFor == -1 || sm.votedFor == msg.From))) && //changed
		(sm.LastLogTerm < msg.LastLogTerm || (sm.LastLogTerm == msg.LastLogTerm && sm.LastLogIndex <= msg.LastLogIndex)){ 
		//sm.currentTerm.Put([]byte(strconv.FormatInt(sm.id, 10)), []byte(strconv.FormatInt(msg.Term, 10)), nil)
		sm.Term = msg.Term
		//sm.voted.Put([]byte(strconv.FormatInt(sm.id, 10)), []byte(strconv.FormatInt(msg.From, 10)), nil)
		sm.votedFor = msg.From
		sm.status = "Follower"
		
		sm.Send(msg.From, NewVoteResp(sm.id, msg.Term, true))
		//fmt.Println("Voted ", msg.From, msg.Term, sm.id, sm.status, sm.Term, " True")
		sm.Alarm(sm.electionTimeout)
	} else{
		sm.Send(msg.From, NewVoteResp(sm.id, msg.Term, false))
		//fmt.Println("Voted ", msg.From, msg.Term, sm.id, sm.status, sm.Term, " False")
	}
}

func (c VoteResponse) CommandName() string{
	return "VoteResponse"
}

func (msg VoteResponse) execute(sm *StateMachine) {
	if sm.status == "Candidate" {
		if sm.Term == msg.Term && msg.VoteGranted {
			sm.votesMap[msg.From] = 1
			if sm.countOnes() > sm.servers/2	{
				sm.status = "Leader"
				//fmt.Println("Elected Leader ", sm.id)
				for i:=int64(0); i<sm.servers; i++ {
					if (i != sm.id) {
						sm.nextIndex[i] = sm.LastLogIndex + 1
						sm.matchIndex[i] = -1
						sm.Send(i, NewAppendEntriesReq(sm.id, sm.Term, sm.LastLogIndex, sm.LastLogTerm, nil, sm.Term, sm.commitIndex))
					}
				}
				sm.Alarm(sm.electionTimeout)	
			}
		} else if sm.Term == msg.Term && !msg.VoteGranted {
			sm.votesMap[msg.From] = -1
			if sm.countNeg() > sm.servers/2	{
				sm.status = "Follower"
				//sm.voted.Put([]byte(strconv.FormatInt(sm.id, 10)), []byte(strconv.FormatInt(sm.id, 10)), nil)
				sm.votedFor = sm.id
				sm.Alarm(sm.electionTimeout) // To reset only on Success?
			}
		}
	}
}

func (c AppendEntriesRequest) CommandName() string{
	return "AppendEntriesRequest"
}

func (msg AppendEntriesRequest) execute(sm *StateMachine) {
	//fmt.Println("Request ", msg.LeaderID, msg.Term, msg.LastLogTerm, msg.LastLogIndex, sm.status, sm.Term)
	if sm.Term < msg.Term || (sm.status == "Candidate" && sm.Term == msg.Term) {
		sm.status = "Follower"
		//sm.currentTerm.Put([]byte(strconv.FormatInt(sm.id, 10)), []byte(strconv.FormatInt(msg.Term, 10)), nil)
		sm.Term = msg.Term
		//sm.voted.Put([]byte(strconv.FormatInt(sm.id, 10)), []byte(strconv.FormatInt(int64(-1), 10)), nil)
		sm.votedFor = -1
		sm.Alarm(sm.electionTimeout) // To reset only on Success?
	} 

	// Check that nextIndex is more than commitIndex
	if  sm.Term > msg.Term {
		sm.Send(msg.LeaderID, NewAppendEntriesResp(sm.id, sm.Term, sm.LastLogIndex, false))
	} else if (msg.LastLogIndex != -1) && 
	(msg.LastLogIndex < sm.commitIndex || (msg.Entries != nil && (msg.LastLogIndex > sm.LastLogIndex || sm.logTerm[msg.LastLogIndex] != msg.LastLogTerm))) {
		sm.Send(msg.LeaderID, NewAppendEntriesResp(sm.id, sm.Term, sm.LastLogIndex, false))
	} else {
		if(msg.Entries != nil){
			if  sm.LastLogIndex > msg.LastLogIndex {
				sm.LastLogIndex = msg.LastLogIndex
			}
			sm.LogStore(sm.LastLogIndex + 1, msg.Entries, msg.EntryTerm)	
			sm.LastLogIndex ++
			sm.LastLogTerm = msg.EntryTerm
			sm.Send(msg.LeaderID, NewAppendEntriesResp(sm.id, sm.Term, sm.LastLogIndex, true)) //If overwriting, should this have current Term?
		}

		if sm.commitIndex < msg.LeaderCommit {	// with Success or outside?
			sm.commitIndex = min(msg.LeaderCommit, sm.LastLogIndex)
			sm.Commit(sm.commitIndex) //, Data, err)
			//fmt.Println("Commited ", sm.id, sm.commitIndex) 
		}
		sm.Alarm(sm.electionTimeout) // To reset only on Success?
	}
}

func (c AppendEntriesResponse) CommandName() string{
	return "AppendEntriesResponse"
}

func (msg AppendEntriesResponse) execute(sm *StateMachine) {
	//fmt.Println("Response ", msg.From, msg.Term, msg.Success, msg.Index, sm.status, sm.Term)
	if sm.Term < msg.Term && msg.Success == false{
		sm.status = "Follower"
		//sm.currentTerm.Put([]byte(strconv.FormatInt(sm.id, 10)), []byte(strconv.FormatInt(msg.Term, 10)), nil)
		sm.Term = msg.Term
		//sm.voted.Put([]byte(strconv.FormatInt(sm.id, 10)), []byte(strconv.FormatInt(int64(-1), 10)), nil)
		sm.votedFor = -1
		sm.Alarm(sm.electionTimeout) // To reset only on Success?
	} else if sm.status == "Leader" {
		if msg.Success {
			sm.matchIndex[msg.From] = msg.Index 
	        sm.nextIndex[msg.From] = msg.Index + 1

	        count := int64(1)
	        for i:=int64(0); i<sm.servers; i++ {
				if (i != sm.id) {
					if(sm.matchIndex[i] >= sm.matchIndex[msg.From]) { count++ }
				}
			}

	        if count > sm.servers/2 {
	        	if sm.commitIndex < msg.Index && sm.Term == sm.logTerm[msg.Index] {
	        		sm.commitIndex = msg.Index 
	        		sm.Commit(sm.commitIndex) 
	        	}
	        }
	    } else { //If AppendEntries fails because of log inconsistency
	        sm.nextIndex[msg.From] --
	        sm.nextIndex[msg.From] = min(sm.nextIndex[msg.From], 0)
	    }
	    if sm.LastLogIndex >= sm.nextIndex[msg.From] {
	    	sm.Send(msg.From, NewAppendEntriesReq(sm.id, sm.Term, sm.nextIndex[msg.From] - 1, sm.logTerm[sm.nextIndex[msg.From] - 1],
	    	sm.log[sm.nextIndex[msg.From]], sm.logTerm[sm.nextIndex[msg.From]], sm.commitIndex))  
	    }
    } 
}

/**************************************************************/

func (c Append) CommandName() string{
	return "Append"
}

func (msg Append) execute(sm *StateMachine) {
	if sm.status == "Leader" {
		sm.LogStore(sm.LastLogIndex + 1, msg.Data, sm.Term)
		//sm.acksRecieved[sm.LastLogIndex + 1] = 1
	    for i:=int64(0); i<sm.servers; i++ {
			if (i != sm.id) {
				sm.Send(i, NewAppendEntriesReq(sm.id, sm.Term, sm.LastLogIndex, sm.LastLogTerm, msg.Data, sm.Term, sm.commitIndex)) // Send next Index
			}
		}
		sm.LastLogIndex ++		
		sm.LastLogTerm = sm.Term
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

	NewStateMachine(servers, 0, actionCh, 500, lg)
	//}
}


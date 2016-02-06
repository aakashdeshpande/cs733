package main

//import "fmt"	

type command interface {
    commandName() string
    execute()	  
}

// The request sent to a server to vote for a candidate to become a leader.
type VoteRequest struct {
	from          int64
	term          int64
	candidateId	  int64	
	lastLogIndex  int64
	lastLogTerm   int64
}

// The response returned from a server after a vote for a candidate to become a leader.
type VoteResponse struct {
	term          int64
	VoteGranted   bool
}

// The request sent to a server to vote for a candidate to become a leader.
type AppendEntriesRequest struct {
	leaderId      int64
	term          int64
	lastLogIndex  int64
	lastLogTerm   int64
	entries 	  []byte
	leaderCommit  int64
}

type StateMachine struct {
	id 			  int64
	status		  string
	term 		  int64
	lastLogIndex  int64
	lastLogTerm   int64
	log  		  [][]byte	
	logTerm 	  []int64
	votedFor 	  []int64  		\\ Should votedFor be linked to the term?
	commitIndex   int64		
	netCh		  chan command	
	timeout 	  chan bool
	electionTimeout int64
}

func Send(id int64, c command) bool {
}

func (c VoteRequest) commandName() string{
	return "VoteRequest"
}

func (msg VoteRequest) execute(sm *StateMachine) {
	if  sm.term <= msg.term && (sm.votedFor[msg.term] == nil or sm.votedFor[msg.term] == msg.id){ 
		if 	sm.lastLogTerm < msg.lastLogTerm || (sm.lastLogTerm == msg.lastLogTerm && sm.lastLogIndex < msg.lastLogIndex){ 
			sm.term = msg.term
			sm.votedFor[sm.term] = msg.id
			action := Send(msg.from, VoteResp(sm.term, voteGranted =true))
		}
	} else{
		action := Send(msg.from, VoteResp(msg.term, voteGranted =false))
	}
	Alarm(sm.electionTimeout)
}

func (c VoteResponse) commandName() string{
	return "VoteResponse"
}

func (c AppendEntriesRequest) commandName() string{
	return "AppendEntriesRequest"
}

func (msg AppendEntriesRequest) execute(sm *StateMachine) {
	if sm.term < msg.term {
		sm.status = "Follower"
		sm.term = msg.term
	}

	if  sm.term > msg.term {
		action = Send(msg.leaderID, AppendEntriesResp(sm.term, success =no))
	} else if (sm.logTerm[msg.lastLogIndex] != msg.lastLogTerm)	{
		action = Send(msg.leaderID, AppendEntriesResp(sm.term, success =no))
	} else {
		if  sm.lastLogIndex > msg.lastLogIndex {
			sm.lastLogIndex = msg.lastLogIndex
		}
		LogStore(sm.lastLogIndex + 1, entries)	
		sm.lastLogIndex ++
		sm.lastLogTerm = sm.term
		action = Send(msg.leaderID, AppendEntriesResp(sm.Id, sm.term, sm.term, success =yes))
		if sm.commitIndex < msg.leaderCommit {	\\ with success or outside?
			commitIndex = min(leaderCommit, index of last new entry)
			Commit(index, data, err)
		}
	}
	Alarm(sm.electionTimeout) // To reset only on success?
}

func (sm *StateMachine) eventLoop(){
	for { 
		select {
			case peerMsg := <- sm.netCh :	
				switch peerMsg.commandName() {
					case "VoteRequest" :
						switch sm.status {
							case "Follower" : 
								peerMsg.execute()
						}
				}
		}
	}
}


	

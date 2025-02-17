package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"bytes"
	"labgob"
	"labrpc"
	"log"
	"math"
	"sync"
	"time"
)

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in Lab 3 you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh; at that point you can add fields to
// ApplyMsg, but set CommandValid to false for these other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
}

//
// example AppendEntriesArgs RPC structure.
// field names must start with capital letters!
//
type AppendEntriesArgs struct {
	Term              int
	LeaderID          int
	PrevLogIndex      int
	PrevLogTerm       int
	Entries           []LogEntry
	LeaderCommitIndex int
}

//
// example AppendEntriesReply RPC structure.
// field names must start with capital letters!
//
type AppendEntriesReply struct {
	Term             int
	Success          bool
	FailType         int // 0 = Term, 1 = Index, 2 = No error
	ConflictingIndex int
}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	CandidateID  int
	LastLogIndex int
	LastLogTerm  int
}

//
// example RequestVoteReply RPC structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
}

//
// example MyRequestVoteReply RPC structure.
// For my own implementation
//
type MyRequestVoteReply struct {
	Term          int
	VoteGranted   bool
	MyReturnValue bool
	MyServer      int
}

// LogEntry: Each Log Entry in log array, command is int
type LogEntry struct {
	Term    int
	Command interface{}
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	// Persistent on all servers
	currentTerm int
	votedFor    int
	log         []LogEntry

	// Volatile on all servers
	commitIndex int
	lastApplied int

	// Volatile on leaders
	nextIndex  []int
	matchIndex []int

	// Extra
	serverState     int // 0 = Follower, 1 = Candidate, 2 = Leader
	electionTimer   *time.Timer
	electionTimeOut time.Duration
	applyCh         chan ApplyMsg
	killChan        chan int

	// For Debugging purposes
	funcName        string
	msgCount        int
}

//
// example AppendEntries RPC handler.
//
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// Invalid append entry
	if args.Term < rf.currentTerm {
		reply.Success = false
		reply.Term = rf.currentTerm
		reply.FailType = 0
		return
	}

	rf.currentTerm = args.Term
	rf.persist()
	rf.serverState = 0
	rf.electionTimer.Stop()
	rf.electionTimer.Reset(rf.electionTimeOut)

	reply.Term = rf.currentTerm
	reply.ConflictingIndex = 1 // Default reply value for Conflicting Index

	// TODO: Check this part
	// Add code for Logs here
	// Before checking term we have to check for index to prevent array index out of bound
	if len(rf.log) - 1 < args.PrevLogIndex {
		reply.Success = false
		reply.FailType = 1
		if rf.commitIndex > 1 {
			reply.ConflictingIndex = rf.commitIndex
		}
		return
	}

	// Now we proper index, check term if it's matching, if not then get the conflicting index, otherwise proceed
	// Check conflicting entries here and return to the Leader
	if rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {
		// Conflicting entries need to be checked from PrevLogIndex since length of log is checked previously
		lastLogTerm := rf.log[args.PrevLogIndex].Term
		lastLogIndex := args.PrevLogIndex

		// Check till commitIndex only since it can't be a conflicting term and
		// we have to check backwards last term with all to minimize comparisons
		for lastLogIndex > rf.commitIndex {
			if lastLogTerm == rf.log[lastLogIndex-1].Term {
				lastLogIndex--
			} else {
				break
			}
		}
		reply.Success = false
		if lastLogIndex > 1 {
			reply.ConflictingIndex = lastLogIndex
		}
		reply.FailType = 1

		return
	}

	reply.Success = true
	reply.FailType = 2

	// Delete followers conflicting logs after PrevLog index only if the request is not a heartbeat
	// DPrintf(rf.funcName, "For %d, my len %d, recd len %d prev Log %d",
	// rf.me, len(rf.log), len(args.Entries), args.PrevLogIndex)
	entriesToCopy := 0 // Keeping the track to see how many entries are already present for multiple requests, so we won't copy those
	logIndex := args.PrevLogIndex + 1
	// Removed bug: No need to check the Command, only need to check the Term
	// and removed complications by deleting once instead of deleting one by one so we can persist at once
	for logIndex < len(rf.log) && entriesToCopy < len(args.Entries) {
		if rf.log[logIndex].Term != args.Entries[entriesToCopy].Term {
			rf.log = rf.log[:logIndex]
			rf.persist()
			break
		}
		entriesToCopy++
		logIndex++
	}

	// Now we have reached common index and term, so Append all new Entries and persist
	// Removed complications by appending once instead of appending one by one so we can persist at once
	if entriesToCopy < len(args.Entries) {
		rf.log = append(rf.log, args.Entries[entriesToCopy:]...)
		rf.persist()
	}

	// TODO: Check this part
	// Set commit index (Bug: Sometimes I was reducing the commitIndex)
	if args.LeaderCommitIndex > rf.commitIndex && args.PrevLogIndex + len(args.Entries) > rf.commitIndex {
		// Can't take Leader commit Index directly if we have less entries and can't go beyond leader commit index that's why minimum
		rf.commitIndex = int(math.Min(float64(args.LeaderCommitIndex), float64(args.PrevLogIndex + len(args.Entries))))
		// Sending to channel if commit index is updated
		if rf.commitIndex > rf.lastApplied {
			// DPrintf(rf.funcName, "sendToClient from follower %d commit%d last %d", rf.me, rf.commitIndex, rf.lastApplied)
			rf.sendToClient()
		}
	}

	return
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// If request term is less or already voted in the same term
	if args.Term < rf.currentTerm || (rf.currentTerm == args.Term && rf.votedFor != -1) {
		reply.VoteGranted = false
		reply.Term = rf.currentTerm
		return
	}

	// Duplicate request from same server
	if rf.currentTerm == args.Term && args.CandidateID == rf.votedFor {
		reply.VoteGranted = true
		reply.Term = rf.currentTerm
		return
	}

	// Valid request for vote
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.votedFor = -1
		rf.persist()
		if rf.serverState != 0 {
			rf.serverState = 0
			rf.electionTimer.Stop()
			rf.electionTimer.Reset(rf.electionTimeOut)
		}
	}

	// Check log entries condition here and for LastLogTerm or and length of log
	rfLastTerm := rf.log[len(rf.log)-1].Term
	if rfLastTerm > args.LastLogTerm || (rfLastTerm == args.LastLogTerm && len(rf.log) - 1 > args.LastLogIndex) {
		reply.VoteGranted = false
		reply.Term = rf.currentTerm
		return
	}

	// Give vote
	rf.votedFor = args.CandidateID
	rf.persist()
	reply.Term = rf.currentTerm
	reply.VoteGranted = true
	return
}

// sendAppendEntries
func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

//
// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	var term int
	var isLeader bool

	// Your code here (2A).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	term = rf.currentTerm
	isLeader = false

	if rf.serverState == 2 {
		isLeader = true
	}

	return term, isLeader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	// Bug: Changing Default value for voted for value from -1 to 0, so added little workaround
	// Persisting commit index and last applied to resolve the error in Race condition error in lab 2
	votedFor := rf.votedFor
	if votedFor == 0 {
		votedFor = 100
	} else if votedFor == -1 {
		votedFor = 0
	}

	e.Encode(rf.currentTerm)
	e.Encode(votedFor)
	e.Encode(rf.log)

	data := w.Bytes()
	rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	currentTerm := 0
	votedFor := 0

	// Bug: Decoding was not happening in the same order as encoding
	if d.Decode(&currentTerm) != nil ||
		d.Decode(&votedFor) != nil ||
		d.Decode(&rf.log) != nil {
	  // DPrintf(rf.funcName, "Error in reading Persistent state")
	} else {
	  rf.currentTerm = currentTerm

	  if votedFor == 0 {
		  rf.votedFor = -1
	  } else if votedFor == 100 {
		  rf.votedFor = 0
	  } else {
		  rf.votedFor = votedFor
	  }
	}
}

//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.serverState != 2 {
		return index, term, false
	}

	index = len(rf.log)
	term = rf.currentTerm
	rf.log = append(rf.log, LogEntry{rf.currentTerm, command})
	rf.persist()

	// DPrintf(rf.funcName, "Received Entry term: %d index: %d command: %v, me: [%d]", rf.currentTerm, index, command, rf.me)
	go rf.PushToFollowers()

	return index, term, isLeader
}

//
// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (rf *Raft) Kill() {
	// Your code here, if desired.
	// Killing raft go routines and timers so that test case doesn't keep running till the end
	// DPrintf(rf.funcName, "kill [%d]", rf.me)
	close(rf.killChan)
}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg, funcName string) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.log = make([]LogEntry, 1)
	rf.log[0].Term = 0
	rf.log[0].Command = nil

	// Volatile on all servers
	rf.commitIndex = 0
	rf.lastApplied = 0

	// Volatile on leaders
	rf.nextIndex = make([]int, len(peers))
	rf.matchIndex = make([]int, len(peers))

	// My 0 = Follower, 1 = Candidate, 2 = Leader
	rf.serverState = 0
	rf.electionTimeOut = time.Duration(500+(100*me)) * time.Millisecond
	rf.electionTimer = time.NewTimer(rf.electionTimeOut)
	rf.applyCh = applyCh
	rf.killChan = make(chan int)
	log.SetFlags(log.LstdFlags | log.Lmicroseconds)

	// For Debugging purposes
	rf.funcName = funcName
	rf.msgCount = 0

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// DPrintf(rf.funcName, "Started timer for %d with timer [%v]", me, rf.electionTimeOut)
	go rf.timerUtility()

	return rf
}

func (rf *Raft) timerUtility() {
	for {
		select {
		case <-rf.electionTimer.C:
			// DPrintf(rf.funcName, "timer expired for %d", rf.me)
			rf.startElection()
		case <-rf.killChan:
			return
		}
	}
}

func retryRequestVote(internalChan chan MyRequestVoteReply, args *RequestVoteArgs, server int, rf *Raft) {
	replyArg := RequestVoteReply{}
	myReplyArg := MyRequestVoteReply{}
	myReplyArg.MyReturnValue = true
	retVal := rf.sendRequestVote(server, args, &replyArg)
	if !retVal {
		myReplyArg.MyServer = server
		myReplyArg.MyReturnValue = false
	}
	myReplyArg.Term = replyArg.Term
	myReplyArg.VoteGranted = replyArg.VoteGranted

	internalChan <- myReplyArg
}

func (rf *Raft) startElection() {
	// starting election
	rf.mu.Lock()
	if rf.serverState == 2 {
		rf.mu.Unlock()
		return
	}
	// DPrintf(rf.funcName, "Election started for %d for term %d", rf.me, rf.currentTerm+1)
	rf.currentTerm++
	rf.votedFor = rf.me
	rf.persist()
	rf.serverState = 1
	rf.electionTimer.Reset(rf.electionTimeOut)
	reqArg := RequestVoteArgs{}
	reqArg.Term = rf.currentTerm
	reqArg.CandidateID = rf.me

	//Add log related part here
	reqArg.LastLogIndex = len(rf.log) - 1
	reqArg.LastLogTerm = rf.log[len(rf.log)-1].Term
	rf.mu.Unlock()

	exitTimer := time.NewTimer(rf.electionTimeOut) // For loop exit timer
	internalChan := make(chan MyRequestVoteReply, len(rf.peers)-1)
	votesGathered := 1
	var majority int
	majority = (len(rf.peers) / 2) + 1
	for index := 0; index < len(rf.peers); index++ {
		if index != rf.me {
			go func(ind int, requestArg *RequestVoteArgs) {
				replyArg := RequestVoteReply{}
				myReplyArg := MyRequestVoteReply{}
				myReplyArg.MyReturnValue = true
				myReplyArg.MyServer = ind
				retVal := rf.sendRequestVote(ind, requestArg, &replyArg)
				if !retVal {
					myReplyArg.MyReturnValue = false
				}
				myReplyArg.Term = replyArg.Term
				myReplyArg.VoteGranted = replyArg.VoteGranted

				internalChan <- myReplyArg
			}(index, &reqArg)
		}
	}
	for votesGathered < majority {
		select {
		case msg := <-internalChan:
			if !msg.MyReturnValue {
				go retryRequestVote(internalChan, &reqArg, msg.MyServer, rf)
			} else {
				rf.mu.Lock()
				if msg.VoteGranted && msg.Term == rf.currentTerm {
					votesGathered++
					// DPrintf(rf.funcName, "Vote received to %d from %d for term %d", rf.me, msg.MyServer, msg.Term)
				}
				if !msg.VoteGranted && rf.currentTerm < msg.Term {
					rf.currentTerm = msg.Term
					rf.votedFor = -1
					rf.persist()
					rf.serverState = 0
					rf.electionTimer.Stop()
					rf.electionTimer.Reset(rf.electionTimeOut)
					rf.mu.Unlock()
					return
				}
				rf.mu.Unlock()
			}
		case <-exitTimer.C:
			// DPrintf(rf.funcName, "timer expired for election in %d", rf.me)
			return
		case <-rf.killChan:
			return
		}
	}
	rf.mu.Lock()
	if votesGathered >= majority {
		// DPrintf(rf.funcName, "Majority winner %d", rf.me)
		rf.serverState = 2
		rf.electionTimer.Stop()

		//Init indices part here
		for index := 0; index < len(rf.peers); index++ {
			if index != rf.me {
				rf.nextIndex[index] = len(rf.log)
				rf.matchIndex[index] = 0
			}
		}

		//Start sending heartbeats
		go rf.sendHeartBeats()
	}
	rf.mu.Unlock()
}

func (rf *Raft) sendHeartBeats() {
	// DPrintf(rf.funcName, "called send hb")
	// Changed from 200 to 100 for 2b with 50 append entries so that test case is executed faster
	// Changed to timer based implementation from sleep based implementation to kill go routing after kill event
	hbDuration := time.Duration(100) * time.Millisecond
	hbTimer := time.NewTimer(time.Duration(2) * time.Millisecond) // For loop exit timer
	for {
		select {
		case <-hbTimer.C:
			// DPrintf(rf.funcName, "sending hb entries from %d", rf.me)
			go rf.PushToFollowers()
			rf.mu.Lock()
			if rf.serverState != 2 {
				rf.mu.Unlock()
				return
			} else {
				//hbTimer.Stop()
				hbTimer.Reset(hbDuration)
				rf.mu.Unlock()
			}
		case <-rf.killChan:
			return
		}
	}
}

func (rf *Raft) sendAppendEntry(server int, msg int) {
	rf.mu.Lock()
	if rf.serverState != 2 {
		rf.mu.Unlock()
		return
	}
	reqArg := AppendEntriesArgs{}
	reqArg.Term = rf.currentTerm
	reqArg.LeaderID = rf.me

	// Add log related entries here
	reqArg.PrevLogIndex = rf.nextIndex[server] - 1
	reqArg.PrevLogTerm = rf.log[reqArg.PrevLogIndex].Term

	// Add part here to send empty entries for heartbeats, one entry in normal case, and multiple entries at once in case of failure
	// Optimization to add entries at once instead of in a for loop
	if rf.nextIndex[server] < len(rf.log) {
		reqArg.Entries = append(reqArg.Entries, rf.log[rf.nextIndex[server]:]...)
	}

	reqArg.LeaderCommitIndex = rf.commitIndex
	rf.mu.Unlock()

	replyArg := AppendEntriesReply{}
	rf.sendAppendEntries(server, &reqArg, &replyArg)
	rf.mu.Lock()
	if !replyArg.Success {
		if replyArg.Term > rf.currentTerm && replyArg.FailType == 0 {
			// Failed because of term
			// DPrintf(rf.funcName, "returned fail for %d my term %d recv term %d me %d,
			// msg[%d]", server, rf.currentTerm, replyArg.Term, rf.me, msg)
			rf.currentTerm = replyArg.Term
			rf.votedFor = -1
			rf.persist()
			rf.serverState = 0
			//rf.electionTimer.Stop()
			rf.electionTimer.Reset(rf.electionTimeOut)
			rf.mu.Unlock()
			return
		} else if replyArg.FailType == 1 {
			// Failed because of Logs
			// DPrintf(rf.funcName, "returned fail for %d, prev log %d, ConflictingIndex %d, me %d, msg[%d]",
			// server, reqArg.PrevLogIndex, replyArg.ConflictingIndex, rf.me, msg)
			// Changing nextIndex value to conflictingIndex instead of prevLogIndex when follower is inconsistent
			// with leader, conflictingIndex will always be lower than or equal to prevLogIndex and greater than 0
			rf.nextIndex[server] = replyArg.ConflictingIndex
		}
	} else {
		// Add log success related part here
		// Use previous log index + length of Entries here, to handle reply delays and updating only if higher
		// DPrintf(rf.funcName, "returned success for %d prev log %d entries %d me %d, msg [%d]",
		// server, reqArg.PrevLogIndex, len(reqArg.Entries), rf.me, msg)
		if reqArg.PrevLogIndex + len(reqArg.Entries) >= rf.nextIndex[server] {
			rf.nextIndex[server] = reqArg.PrevLogIndex + len(reqArg.Entries) + 1
			rf.matchIndex[server] = reqArg.PrevLogIndex + len(reqArg.Entries)
		}

		// N = Match Index of the Log after successful replication
		N := reqArg.PrevLogIndex + len(reqArg.Entries)
		if N < len(rf.log) && N > rf.commitIndex && rf.log[N].Term == rf.currentTerm { // Added 1st condition for index out of range
			majority := (len(rf.peers) / 2) + 1
			count := 1 // self
			for index := 0; index < len(rf.peers); index++ {
				if index != rf.me && rf.matchIndex[index] >= N {
					count++
				}
			}
			if count >= majority {
				rf.commitIndex = N
				// Sending to channel if commit index is changed
				if rf.commitIndex > rf.lastApplied {
					// DPrintf(rf.funcName, "sendToClient from leader %d commit %d last %d", rf.me, rf.commitIndex, rf.lastApplied)
					rf.sendToClient()
				}
			}
		}
	}
	rf.mu.Unlock()
}

func (rf *Raft) sendToClient() {
	if rf.lastApplied >= len(rf.log) {
		return
	}
	entriesToPush := rf.commitIndex - rf.lastApplied
	entryIndices := make([]int, entriesToPush)
	entries := make([]LogEntry, entriesToPush)
	for index := 0; index < entriesToPush; index++ {
		entryIndices[index] = index + rf.lastApplied + 1
		entries[index] = rf.log[index+rf.lastApplied+1]
		// DPrintf(rf.funcName, "applying %v", entries[index].Command)
	}
	rf.lastApplied = rf.commitIndex

	for index := 0; index < entriesToPush; index++ {
		rf.applyCh <- ApplyMsg{true, entries[index].Command, entryIndices[index]}
	}
}

func (rf *Raft) PushToFollowers() {
	rf.mu.Lock()
	// DPrintf(rf.funcName, "PushToFollowers me: [%d], msg: [%d]", rf.me, rf.msgCount)
	defer rf.mu.Unlock()
	for index := 0; index < len(rf.peers); index++ {
		if index != rf.me {
			go rf.sendAppendEntry(index, rf.msgCount)
		}
	}
	rf.msgCount++
}


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

import "sync"
import "time"
import "math/rand"
import "github.com/zjsyhjh/distributed-system/labrpc"

// import "bytes"
// import "encoding/gob"

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make().
//
type ApplyMsg struct {
	Index       int
	Command     interface{}
	UseSnapshot bool   // ignore for lab2; only used in lab3
	Snapshot    []byte // ignore for lab2; only used in lab3
}

type Status int

const (
	FOLLOWER Status = iota
	CANDIDATE
	LEADER
)

type Log struct {
	index int
	Term  int
	Cmd   interface{}
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

	// according to raft paper's Figure 2 : State
	// persistent state on all servers
	currentTerm int   // latest term server has been (initilized to 0 on first boot, increases monotonically)
	voteFor     int   // candidateId that received vote in current term(or null if none)
	log         []Log // log entries; each entry contains command for state machine, and term when entry was received by leader(first index is 1)
	// volatile state on all servers
	commitIndex int // index of highest log entry known to be commited(initilized to 0, increases monotonically)
	lastApplied int // inded of highest log entry appiled to state machine(initialized to 0, increases monotonically)
	// volatile state on leader
	nextIndex  []int // for each server, index of next log entry to send to that server(initialized to leader last log index + 1)
	matchIndex []int // for each server, index of the highest log entry known to be replicated on server(initialized to 0, increases monotonically)

	status                    Status // Follower, candidate or leader
	votedCount                int    // vote count
	heartbeatCh               chan bool
	voteResultCh              chan bool
	electionTimeout           time.Duration
	heartbeatTimeout          time.Duration
	randomizedElectionTimeout time.Duration
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	return term, isleader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := gob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := gob.NewDecoder(r)
	// d.Decode(&rf.xxx)
	// d.Decode(&rf.yyy)
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	// according to raft paper figure 2 : RequestVote RPC
	// term candidate’s term
	// candidateId candidate requesting vote
	// lastLogIndex index of candidate's last log entry
	// lastLogTerm term of candidate's last log entry
	Term         int
	CandidateID  int
	LastLogIndex int
	LastLogTerm  int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	// according to raft paper figure 2 : RequestVote RPC
	// term currentTerm, for candidate to update itself
	// voteGranted true means candidate received vote
	Term        int
	VoteGranted bool
}

//
// add AppendEntriesArgs RPC struct
//
type AppendEntriesArgs struct {
	Term     int
	LeaderID int
	Entries  []Log
}

//
// add AppendEntriesReply struct
//
type AppendEntriesReply struct {
	Term    int
	Success bool
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	DPrintf("peer-%v handle the RequestVote from candidate-%v\n", rf.me, args.CandidateID)
	rf.heartbeatCh <- true

	reply.Term = rf.currentTerm
	if args.Term > rf.currentTerm {
		rf.mu.Lock()
		rf.currentTerm = args.Term
		rf.status = FOLLOWER
		rf.mu.Unlock()
		return
	}

	if args.Term < rf.currentTerm {
		// previous request, no reply
		reply.VoteGranted = false
	} else if rf.voteFor != -1 && rf.voteFor != args.CandidateID {
		DPrintf("follower-%v votedFor candidate-%v\n", rf.me, rf.voteFor)
		reply.VoteGranted = false
	} else if args.LastLogIndex < rf.commitIndex || args.LastLogTerm < rf.currentTerm {
		DPrintf("candidate-%v's log is not as least as up-to-date as receiver's log\n", args.CandidateID)
		reply.VoteGranted = false
	} else {
		reply.VoteGranted = true
	}
	DPrintf("RequestVote done.\n")
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

// my code for AppendEntries
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	reply.Term = rf.currentTerm

	if args.Term < rf.currentTerm {
		reply.Success = false
		return
	} else if args.Term > rf.currentTerm {
		rf.mu.Lock()
		rf.currentTerm = args.Term
		rf.status = FOLLOWER
		rf.mu.Unlock()
	}

	rf.heartbeatCh <- true
	if len(args.Entries) == 0 {
		reply.Success = true
		return
	}
}

// my code for sendAppendEntries
func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election.
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
}

//my code for lab-2

func (rf *Raft) convertToCandidate() {
	rf.mu.Lock()
	rf.status = CANDIDATE
	rf.mu.Unlock()
}

func (rf *Raft) convertToFollower() {
	rf.mu.Lock()
	rf.status = FOLLOWER
	rf.mu.Unlock()
}

func (rf *Raft) convertToLeader() {
	rf.mu.Lock()
	rf.status = LEADER
	rf.mu.Unlock()
}

// reset leader election timeout
// [electionTimeout, 2 * electionTimeout - 1]
func (rf *Raft) resetElectionTimeout() time.Duration {
	rand.Seed(time.Now().UTC().UnixNano())
	rf.randomizedElectionTimeout = rf.electionTimeout + time.Duration(rand.Int63n(rf.electionTimeout.Nanoseconds()))
	return rf.randomizedElectionTimeout
}

// candidate broadcast and request for vote
func (rf *Raft) broadcastRequestVote() {
	for server := range rf.peers {
		if server != rf.me && rf.status == CANDIDATE {
			var args RequestVoteArgs
			args.Term = rf.currentTerm
			args.CandidateID = rf.me
			args.LastLogIndex = rf.commitIndex
			args.LastLogTerm = rf.log[rf.commitIndex].Term

			var reply RequestVoteReply
			DPrintf("candidate-%v send RequestVote to peer-%v\n", rf.me, server)
			ok := rf.sendRequestVote(server, &args, &reply)
			if ok {
				if reply.Term > rf.currentTerm {
					rf.mu.Lock()
					rf.currentTerm = reply.Term
					rf.status = FOLLOWER
					rf.mu.Unlock()
					break
				} else if reply.VoteGranted {
					rf.votedCount++
				}
			} else {
				//fail, retry ?
			}
		}
	}
	if rf.votedCount > len(rf.peers)/2 {
		rf.voteResultCh <- true
	} else {
		rf.voteResultCh <- false
	}
}

// candidate elect leader
func (rf *Raft) candidate() {
	hasLeader := false
	for {
		select {
		case <-rf.heartbeatCh:
			hasLeader = true
		default:
		}
		if hasLeader {
			break
		}

		// first, increase currentTerm, vote
		rf.mu.Lock()
		rf.currentTerm++
		rf.voteFor = rf.me
		rf.votedCount = 1
		rf.mu.Unlock()

		// second, broadcast and requestforvote
		go rf.broadcastRequestVote()

		//third, wait for result
		select {
		case result := <-rf.voteResultCh:
			if result {
				hasLeader = true
				rf.convertToLeader()
			} else {
				//fail?
			}
		case <-time.After(rf.resetElectionTimeout()):
			go func() {
				<-rf.voteResultCh
			}()
		}
		if hasLeader {
			break
		}
	}
}

// leader broadcast heartbeat each heartbeatTimeout
func (rf *Raft) leader() {
	tick := time.Tick(rf.heartbeatTimeout)
	for {
		select {
		case <-tick:
			DPrintf("leader-%v begin to broadcast heartbeat\n", rf.me)
			go rf.broadcastHeartbeat()

		}
	}
}

// leader broadcast heartbeat to follower or candidate
func (rf *Raft) broadcastHeartbeat() {
	for server := range rf.peers {
		if server != rf.me && rf.status == LEADER {
			var args AppendEntriesArgs
			args.Term = rf.currentTerm
			args.LeaderID = rf.me

			var reply AppendEntriesReply
			DPrintf("leader-%v send heartbeat to follower-%v\n", rf.me, server)
			ok := rf.sendAppendEntries(server, &args, &reply)
			if ok {
				if reply.Term > rf.currentTerm {
					//convert to follower
					rf.mu.Lock()
					rf.currentTerm = reply.Term
					rf.status = FOLLOWER
					rf.mu.Unlock()
					break
				}
			} else {
				//fail, retry?
			}
		}
	}
}

func (rf *Raft) backgroundLoop() {
	for {
		switch rf.status {
		case FOLLOWER:
			DPrintf("I'm follower-%v\n", rf.me)
			DPrintf("Starting election timeout %v\n", rf.resetElectionTimeout())
			// wait for leader's heartbeat or election timeout
			select {
			case <-rf.heartbeatCh:
			case <-time.After(rf.randomizedElectionTimeout):
				DPrintf("election timeout %v\n", rf.randomizedElectionTimeout)
				rf.convertToCandidate()
			}
		case CANDIDATE:
			DPrintf("I'm candidate-%v\n", rf.me)
			rf.candidate()
		case LEADER:
			DPrintf("I'm leader-%v\n", rf.me)
			rf.leader()
		}
	}
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
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	rf.currentTerm = 0
	rf.status = FOLLOWER
	rf.voteFor = -1
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.log = []Log{{index: 0, Term: 0}}
	rf.heartbeatCh = make(chan bool, 1)
	rf.voteResultCh = make(chan bool)
	rf.heartbeatTimeout = 500 * time.Millisecond
	rf.electionTimeout = 1000 * time.Millisecond

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	// start goroutine
	go rf.backgroundLoop()

	return rf
}

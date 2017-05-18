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

	status     Status // Follower, candidate or leader
	votedCount int    // vote count
	// from raft paper, if election timeout elapses without receiving AppendEntries
	// RPC from current leader or granting vote to candidate: convert to candidate
	heartbeatCh chan bool
	//for vote result
	voteResultCh              chan bool
	electionTimeout           time.Duration
	heartbeatTimeout          time.Duration
	randomizedElectionTimeout time.Duration

	applyCh chan ApplyMsg
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	term = rf.currentTerm
	isleader = (rf.status == LEADER)
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
	if rf.status != LEADER {
		isLeader = false
		return index, term, isLeader
	}

	go func(command interface{}) {
		entry := Log{
			Index: len(rf.log),
			Term:  rf.currentTerm,
			Cmd:   command,
		}
		//append entry
		rf.log = append(rf.log, entry)
		// a majority of servers agree
		agree := rf.broadcastAppendEntries([]Log{entry})

		if agree {
			// if agree, then committed
			rf.mu.Lock()
			rf.commitIndex++
			rf.lastApplied = rf.commitIndex
			rf.nextIndex[rf.me] = len(rf.log)
			rf.mu.Unlock()
			msg := ApplyMsg{
				Index:   entry.Index,
				Command: entry.Cmd,
			}
			rf.applyCh <- msg
		}

	}(command)

	index = len(rf.log)
	term = rf.currentTerm
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

// broadcast appendEntries
func (rf *Raft) broadcastAppendEntries(entries []Log) (agree bool) {
	preLog := rf.log[entries[0].Index-1]
	args := AppendEntriesArgs{
		Term:         rf.currentTerm,
		LeaderID:     rf.me,
		PrevLogIndex: preLog.Index,
		PrevLogTerm:  preLog.Term,
		LeaderCommit: rf.commitIndex,
		Entries:      entries,
	}

	// broadcast
	count := 1
	for server := range rf.peers {
		if server != rf.me && rf.status == LEADER {
			reply := AppendEntriesReply{}
			ok := rf.sendAppendEntries(server, &args, &reply)
			if ok {
				if reply.Term > rf.currentTerm {
					rf.resetTermAndToFollower(reply.Term)
					break
				}
				if reply.Success {
					count++
				}
			} else {
				// fail, retry
			}
		}
	}

	if rf.status == LEADER && count > len(rf.peers)/2 {
		agree = true
	} else {
		agree = false
	}
	return
}

// leader broadcast heartbeat to follower or candidate
func (rf *Raft) broadcastHeartbeatRPC() {
	lastLogIndex := len(rf.log) - 1

	for server := range rf.peers {
		if server != rf.me && rf.status == LEADER {
			var args AppendEntriesArgs
			args.Term = rf.currentTerm
			args.LeaderID = rf.me
			args.LeaderCommit = rf.commitIndex

			if lastLogIndex >= rf.nextIndex[server] {
				args.Entries = rf.log[rf.nextIndex[server]:lastLogIndex]
			}
			var reply AppendEntriesReply
			DPrintf("leader-%v send heartbeat to server-%v\n", rf.me, server)
			ok := rf.sendAppendEntries(server, &args, &reply)
			if ok {
				if reply.Term > rf.currentTerm {
					//convert to follower
					DPrintf("leader-%v received reply from server-%v\n", rf.me, server)
					DPrintf("reply.term is %v larger than currentTerm %v, leader-%v reset term and convert to follower\n", reply.Term, rf.currentTerm, rf.me)
					rf.resetTermAndToFollower(reply.Term)
					break
				}
				if reply.Success {
					//If successful: update nextIndex and matchIndex for follower
					DPrintf("leader-%v appendEntries success.\n", rf.me)
					rf.nextIndex[server] = lastLogIndex + 1
					rf.matchIndex[server] = lastLogIndex
				} else {
					// If AppendEntries fails because of log inconsistency:decrement nextIndex and retry
					DPrintf("leader-%v appendEntries fail.\n", rf.me)
					rf.nextIndex[server]--
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
			rf.follower()
		case CANDIDATE:
			rf.candidate()
		case LEADER:
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
	rf.applyCh = applyCh

	// Your initialization code here (2A, 2B, 2C).
	rf.currentTerm = 0
	rf.status = FOLLOWER
	rf.voteFor = -1
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))
	rf.log = []Log{{Index: 0, Term: 0}}
	rf.heartbeatCh = make(chan bool)
	rf.voteResultCh = make(chan bool)
	rf.heartbeatTimeout = 50 * time.Millisecond
	rf.electionTimeout = 600 * time.Millisecond

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	// start goroutine
	go rf.backgroundLoop()

	return rf
}

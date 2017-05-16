package raft

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
	Term        int
	CandidateID int
	// log replicated
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

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	DPrintf("peer-%v handle the RquestVote from candidate-%v\n", rf.me, args.CandidateID)

	reply.Term = rf.currentTerm

	DPrintf("peer-%v's term is %v, candidate-%v's term is %v\b", rf.me, rf.currentTerm, args.CandidateID, args.Term)
	if args.Term > rf.currentTerm {
		DPrintf("args.term is %v, server's currentTerm is %v\n", args.Term, rf.currentTerm)
		DPrintf("server-%v resetTerm and convert to follower\n", rf.me)
		rf.resetTermAndToFollower(args.Term)
	}

	rf.heartbeatCh <- true //avoid electiontimeout

	if args.Term < rf.currentTerm {
		// previous request, no reply
		reply.VoteGranted = false
	} else if rf.voteFor != -1 && rf.voteFor != args.CandidateID {
		DPrintf("follower-%v votedFor candidate-%v\n", rf.me, rf.voteFor)
		reply.VoteGranted = false
	} else if rf.commitIndex != 0 && (args.LastLogTerm < rf.currentTerm || args.LastLogIndex < rf.commitIndex) {
		// rf.commitIndex equals 0 means no logs
		DPrintf("candidate-%v's log is not at least up-to-date to current-server-%v's log\n", args.CandidateID, rf.me)
		reply.VoteGranted = false
	} else {
		rf.mu.Lock()
		rf.voteFor = args.CandidateID
		rf.mu.Unlock()
		reply.VoteGranted = true
	}
	DPrintf("reply.VoteGrand = %v, RequestVote done.\n", reply.VoteGranted)
}

// candidate broadcast and request for vote
func (rf *Raft) broadcastRequestVote() {
	for server := range rf.peers {
		if server != rf.me && rf.status == CANDIDATE {
			var args RequestVoteArgs
			args.Term = rf.currentTerm
			args.CandidateID = rf.me

			var reply RequestVoteReply
			DPrintf("candidate-%v send RequestVote to peer-%v\n", rf.me, server)
			ok := rf.sendRequestVote(server, &args, &reply)
			if ok {
				DPrintf("candidate-%v\n", rf.me)
				if reply.Term > rf.currentTerm {
					rf.resetTermAndToFollower(reply.Term)
					break
				} else if reply.VoteGranted {
					rf.votedCount++
				}
			} else {
				//fail, retry ?
			}
		}
	}
	if rf.votedCount > len(rf.peers)/2 && rf.status == CANDIDATE {
		rf.voteResultCh <- true
	}
}

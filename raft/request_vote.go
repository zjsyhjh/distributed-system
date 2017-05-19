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
	if ok {
		if reply.Term > rf.currentTerm {
			rf.resetTermAndToFollower(reply.Term)
		} else if reply.VoteGranted && reply.Term == rf.currentTerm {
			rf.countVote()
			if rf.status == CANDIDATE && rf.votedCount > len(rf.peers)/2 {
				rf.voteResultCh <- true
			}
		}
	}
	return ok
}

func (rf *Raft) countVote() {
	rf.mu.Lock()
	rf.votedCount++
	rf.mu.Unlock()
}

/*
 * handle Request vote from candidate
 */
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	if args.Term > rf.currentTerm {
		DPrintf("args.term is %v, server's currentTerm is %v\n", args.Term, rf.currentTerm)
		DPrintf("server-%v resetTerm and convert to follower\n", rf.me)
		rf.resetTermAndToFollower(args.Term)
	}
	reply.Term = rf.currentTerm
	//
	if args.Term < rf.currentTerm {
		// previous request, no reply
		reply.VoteGranted = false
	} else if rf.voteFor != -1 && rf.voteFor != args.CandidateID {
		DPrintf("server-%v has votedFor candidate-%v\n", rf.me, rf.voteFor)
		reply.VoteGranted = false
	} else if !rf.requestUpToDate(args) {
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

func (rf *Raft) requestUpToDate(args *RequestVoteArgs) bool {
	lastLog := rf.log[len(rf.log)-1]
	argsLogIndex := args.LastLogIndex
	argsLogTerm := args.LastLogTerm
	return argsLogTerm > lastLog.Term || (argsLogTerm == lastLog.Term && argsLogIndex >= args.LastLogIndex)
}

/*
 * candidate broadcast and request for vote
 */
func (rf *Raft) broadcastRequestVoteRPC() {
	var args RequestVoteArgs
	args.CandidateID = rf.me
	args.Term = rf.currentTerm
	args.LastLogIndex = len(rf.log) - 1
	args.LastLogTerm = rf.log[len(rf.log)-1].Term

	for server := range rf.peers {
		if server != rf.me && rf.status == CANDIDATE {
			go func(server int) {
				var reply RequestVoteReply
				DPrintf("candidate-%v send RequestVote to peer-%v\n", rf.me, server)
				rf.sendRequestVote(server, &args, &reply)
			}(server)
		}
	}
}

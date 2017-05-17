package raft

//
// add Log struct
//
type Log struct {
	Index int
	Term  int
	Cmd   interface{}
}

//
// add AppendEntriesArgs RPC struct
//
type AppendEntriesArgs struct {
	Term         int   // leader's term
	LeaderID     int   // so follow can redirect clients
	PrevLogIndex int   // index of log entry immediately preceding new ones
	PrevLogTerm  int   // term of prevLogIndex entry
	LeaderCommit int   //leader's commitIndex
	Entries      []Log //log entries for store(empty for heartbeat)
}

//
// add AppendEntriesReply struct
//
type AppendEntriesReply struct {
	Term    int
	Success bool
}

//
// my code for sendAppendEntries
//
func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

//
// my code for AppendEntries
//
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	DPrintf("peer-%v append entries from leader-%v\n", rf.me, args.LeaderID)

	reply.Term = rf.currentTerm

	if args.Term < rf.currentTerm {
		reply.Success = false
		return
	}

	if args.Term > rf.currentTerm {
		DPrintf("server-%v reset term and convert to follower\n", rf.me)
		rf.resetTermAndToFollower(args.Term)
	}
	//
	go func() {
		rf.heartbeatCh <- true
	}()

	rf.mu.Lock()
	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = args.LeaderCommit
		go rf.checkCommitIndexAndApplied()
	}
	rf.mu.Unlock()

	//if entries is empty, heartbeat
	if len(args.Entries) == 0 {
		reply.Success = true
		return
	}

	// replica log
	if args.PrevLogIndex > len(rf.log) || rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {
		reply.Success = false
	} else {
		firstEntry := args.Entries[0]
		if len(rf.log) > firstEntry.Index {
			rf.log = rf.log[:(firstEntry.Index - 1)]
		}
		rf.log = append(rf.log, args.Entries...)
		reply.Success = true
	}
	return
}

//
func (rf *Raft) checkCommitIndexAndApplied() {
	if rf.commitIndex > rf.lastApplied {
		commitIndex := rf.commitIndex
		lastApplied := rf.lastApplied

		rf.mu.Lock()
		rf.lastApplied = rf.commitIndex
		rf.mu.Unlock()

		for index := lastApplied + 1; index <= commitIndex; index++ {
			msg := ApplyMsg{
				Index:   rf.log[index].Index,
				Command: rf.log[index].Cmd,
			}
			go func() {
				rf.applyCh <- msg
			}()
		}
	}
}

package raft

/*
 * add Log struct
 */
type Log struct {
	Index int
	Term  int
	Cmd   interface{}
}

/*
 * add AppendEntriesArgs struct
 */
type AppendEntriesArgs struct {
	Term         int   // leader's term
	LeaderID     int   // so follow can redirect clients
	PrevLogIndex int   // index of log entry immediately preceding new ones
	PrevLogTerm  int   // term of prevLogIndex entry
	LeaderCommit int   //leader's commitIndex
	Entries      []Log //log entries for store(empty for heartbeat)
}

/*
 * add AppendEntriesReply struct
 */
type AppendEntriesReply struct {
	Term    int
	Success bool
}

/*
 * sendAppendEntries to specified server
 */
func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	if ok {
		if reply.Term > rf.currentTerm {
			rf.resetTermAndToFollower(reply.Term)
		} else if reply.Success {
			//update nextIndex and matchIndex
			rf.mu.Lock()
			rf.nextIndex[server] = args.PrevLogIndex + len(args.Entries) + 1
			rf.matchIndex[server] = rf.nextIndex[server] - 1
			rf.mu.Unlock()
			//update commitIndex
			rf.updateCommitIndex()
		} else {
			rf.mu.Lock()
			rf.nextIndex[server]--
			rf.mu.Unlock()
		}
	}
	return ok
}

/*
 * AppendEntries on specified server
 */
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {

	rf.heartbeatCh <- true

	reply.Term = rf.currentTerm

	if args.Term < rf.currentTerm {
		//reply false if term < currentTerm
		reply.Success = false
		return
	}

	if args.Term > rf.currentTerm {
		DPrintf("server-%v update it's term \n", rf.me)
		rf.resetTermAndToFollower(args.Term)
		return
	}

	if args.PrevLogIndex < len(rf.log) && args.PrevLogTerm != rf.log[args.PrevLogIndex].Term {
		reply.Success = false
		return
	}

	reply.Success = true
	//if entries is empty, heartbeat
	//if len(args.Entries) == 0 {
	//	reply.Success = true
	//	return
	//}

	if len(args.Entries) > 0 {
		rf.mu.Lock()
		firstEntry := args.Entries[0]
		//If an existing entry conflicts with a new one (same index
		//but different terms), delete the existing entry and all that
		if len(rf.log) > firstEntry.Index {
			rf.log = rf.log[:(firstEntry.Index - 1)]
		}
		rf.log = append(rf.log, args.Entries...)
		rf.mu.Unlock()
	}
	//If leaderCommit > commitIndex, set commitIndex =
	//min(leaderCommit, index of last new entry)
	if args.LeaderCommit > rf.commitIndex {
		rf.mu.Lock()
		rf.commitIndex = rf.minInt(args.LeaderCommit, len(rf.log)-1)
		rf.mu.Unlock()
	}
}

/*
 * leader braodcast AppendEntriesRPC
 */
func (rf *Raft) broadcastAppendEntriesRPC() {
	for server := range rf.peers {
		if server != rf.me && rf.status == LEADER {
			//send entries in parallel
			go func(server int) {
				prevLogIndex := rf.nextIndex[server] - 1
				//if entries is empty, means heartbeat
				args := AppendEntriesArgs{
					Term:         rf.currentTerm,
					LeaderID:     rf.me,
					LeaderCommit: rf.commitIndex,
					PrevLogIndex: prevLogIndex,
					PrevLogTerm:  rf.log[prevLogIndex].Term,
					Entries:      rf.log[(prevLogIndex + 1):],
				}
				reply := AppendEntriesReply{}
				DPrintf("leader-%v sendAppendEntries to server-%v\n", rf.me, server)
				rf.sendAppendEntries(server, &args, &reply)
			}(server)
		}
	}
}

/*
 * update commotIndex
 */
func (rf *Raft) updateCommitIndex() {
	rf.mu.Lock()
	newCommitIndex := rf.commitIndex
	nCount := 0
	for _, matchIndex := range rf.matchIndex {
		if matchIndex > rf.commitIndex {
			nCount++
			if newCommitIndex == rf.commitIndex || matchIndex < newCommitIndex {
				newCommitIndex = matchIndex
			}
		}
	}
	if nCount > len(rf.peers)/2 && rf.status == LEADER {
		rf.commitIndex = newCommitIndex
	}
	rf.mu.Unlock()
}

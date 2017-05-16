package raft

//
// add Log struct
//
type Log struct {
	index int
	Term  int
	Cmd   interface{}
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
	go func() {
		rf.heartbeatCh <- true
	}()
}

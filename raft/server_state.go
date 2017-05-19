package raft

import "time"

/*
 * Followers only respond to requests from other servers(leader heartbeat or leader election)
 */
func (rf *Raft) follower() {
	DPrintf("follower-%v starts election timeout %v\n", rf.me, rf.resetElectionTimeout())
	// wait for leader's heartbeat or election timeout
	select {
	case <-rf.heartbeatCh:
		DPrintf("recevied heartbeat\n")
	case <-time.After(rf.randomizedElectionTimeout):
		DPrintf("follower-%v elect timeout %v, and convert to candidate\n", rf.me, rf.randomizedElectionTimeout)
		rf.convertToCandidate()
	}
}

/*
 * leader election
 */
func (rf *Raft) candidate() {
	DPrintf("candidate-%v\n", rf.me)
	select {
	case <-rf.voteResultCh:
	case <-rf.heartbeatCh:
		DPrintf("candidate-%v received heartbeat\n", rf.me)
		rf.convertToFollower()
		return
	default:
	}
	//first, increase currentTer, vote self
	rf.vote(rf.me)
	//second, broadcoast and request vote
	go rf.broadcastRequestVoteRPC()
	//third, wait for vote result
	select {
	case becomeLeader := <-rf.voteResultCh:
		//after become leader, broadcast
		if becomeLeader {
			DPrintf("candidate-%v convert to leader\n", rf.me)
			rf.convertToLeaderAndInitState()
			go rf.broadcastAppendEntriesRPC()
		}
	//leader election again
	case <-time.After(rf.resetElectionTimeout()):
	//received heartbeat from leader
	case <-rf.heartbeatCh:
		rf.convertToFollower()
	}
}

/*
 * leader broadcast heartbeat each heartbeatTimeout
 */
func (rf *Raft) leader() {
	time.Sleep(rf.heartbeatTimeout)
	DPrintf("leader-%v begin to broadcast heartbeat\n", rf.me)
	go rf.broadcastAppendEntriesRPC()
}

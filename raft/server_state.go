package raft

import "time"

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

//
// leader election
//
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
		//become leader
		if becomeLeader {
			DPrintf("candidate-%v convert to leader\n", rf.me)
			rf.convertToLeaderAndInitState()
			go rf.broadcastHeartbeatRPC()

		}
	//leader election again
	case <-time.After(rf.resetElectionTimeout()):
	//received heartbeat from leader
	case <-rf.heartbeatCh:
		rf.convertToFollower()
	}
}

//
// leader broadcast heartbeat each heartbeatTimeout
//
func (rf *Raft) leader() {
	DPrintf("leader-%v\n", rf.me)
	tick := time.Tick(rf.heartbeatTimeout)
	for {
		select {
		case <-tick:
			DPrintf("leader-%v begin to broadcast heartbeat\n", rf.me)
			go rf.broadcastHeartbeatRPC()
		}
		if rf.status != LEADER {
			break
		}
	}
}

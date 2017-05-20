package raft

import (
	"math/rand"
	"time"
)

/*
 * server status
 */
const (
	FOLLOWER = iota
	CANDIDATE
	LEADER
)

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

func (rf *Raft) convertToLeaderAndInitState() {
	rf.mu.Lock()
	rf.status = LEADER
	rf.mu.Unlock()
	maxIndex := len(rf.log)
	for server := range rf.peers {
		rf.nextIndex[server] = maxIndex
		rf.matchIndex[server] = 0
	}
}

func (rf *Raft) resetTermAndToFollower(term int) {
	rf.mu.Lock()
	rf.currentTerm = term
	rf.status = FOLLOWER
	rf.voteFor = -1
	rf.mu.Unlock()
}

func (rf *Raft) vote(server int) {
	rf.mu.Lock()
	rf.currentTerm++
	rf.voteFor = server
	rf.votedCount = 1
	rf.mu.Unlock()
}

// reset leader election timeout
// [electionTimeout, 2 * electionTimeout - 1]
func (rf *Raft) resetElectionTimeout() time.Duration {
	rand.Seed(time.Now().UTC().UnixNano())
	rf.randomizedElectionTimeout = rf.electionTimeout + time.Duration(rand.Int63n(rf.electionTimeout.Nanoseconds()))
	return rf.randomizedElectionTimeout
}

func (rf *Raft) minInt(x, y int) int {
	if x < y {
		return x
	}
	return y
}

func (rf *Raft) maxInt(x, y int) int {
	if x > y {
		return x
	}
	return y
}

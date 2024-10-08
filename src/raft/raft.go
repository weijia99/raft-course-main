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

import (
	//	"bytes"

	"fmt"
	"sync"
	"sync/atomic"
	"time"

	//	"course/labgob"
	"course/labrpc"
)

// setting the const of lower bound and upper bound of election time out
const (
	electionTimeoutMin time.Duration = 250 * time.Millisecond
	electionTimeOutMax time.Duration = 400 * time.Millisecond
	// the interval of the log replication
	replicateInterval time.Duration = 70 * time.Millisecond
)

// define Role:candidate ,follower,leader
type Role string

const (
	Follower  Role = "follower"
	Candidate Role = "candidate"
	Leader    Role = "leader"
)
const (
	InvalidIndex int = 0
	InvalidTerm  int = 0
)

// enum

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part PartD you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.

// add Role ,term ,start_time,end_time,etc
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For PartD:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	role        Role
	currentTerm int
	votedFor    int      //vote who (-1 present null)
	log         *RaftLog //local log sequence
	// both of the nextIndex and matchIndex length is equal to the peers
	nextIndex  []int //next index of each server(from which index to send)
	matchIndex []int //match	index of each server(from which index to send)

	electionStart   time.Time
	electionTimeOut time.Duration

	// filed for the applyCh
	commitIndex int
	lastApplied int
	applyCh     chan ApplyMsg
	snapPending bool
	applyCond   *sync.Cond
	// Your data here (PartA, PartB, PartC).
	// add yourself struct
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

}

func (rf *Raft) logString() string {
	var terms string
	prevTerm := rf.log.at(0).Term
	prevStart := 0
	for i := 0; i < rf.log.size(); i++ {
		if rf.log.at(i).Term != prevTerm {
			terms += fmt.Sprintf(" [%d, %d]T%d;", prevStart, i-1, prevTerm)
			prevTerm = rf.log.at(i).Term
			prevStart = i
		}
	}
	terms += fmt.Sprintf(" [%d, %d]T%d;", prevStart, rf.log.size()-1, prevTerm)

	return terms
}

// state change
func (rf *Raft) becomeFollowerLocked(term int) {
	// compare term ,if term is lower than rf.term ,do not change
	if rf.currentTerm > term {
		// add log
		LOG(rf.me, rf.currentTerm, DError, "Can't become Follower, lower term: T%d", term)
		return
	}
	// log
	LOG(rf.me, rf.currentTerm, DLog, "%s->Follower, For T%v->T%v", rf.role, rf.currentTerm, term)
	rf.role = Follower
	shouldPersist := rf.currentTerm != term
	if rf.currentTerm < term {
		rf.votedFor = -1
	}
	rf.currentTerm = term
	// all term
	// WHEN currentTerm is updated, persist
	if shouldPersist {
		rf.persistLocked()
	}

}

func (rf *Raft) becomeCandidateLocked() {
	// compare term ,if term is lower than rf.term ,do not change
	if rf.role == Leader {
		LOG(rf.me, rf.currentTerm, DError, "Leader can't become Candidate")
		return
	}

	LOG(rf.me, rf.currentTerm, DVote, "%s -> Candidate, For T%d->T%d",
		rf.role, rf.currentTerm, rf.currentTerm+1)
	rf.role = Candidate
	rf.currentTerm++
	rf.votedFor = rf.me
	// update votedFor
	rf.persistLocked()

}

func (rf *Raft) becomeLeaderLocked() {

	if rf.role != Candidate {
		LOG(rf.me, rf.currentTerm, DError, "Only Candidate can become Leader")
		return
		//
	}
	LOG(rf.me, rf.currentTerm, DLeader, "Become Leader in T%d", rf.currentTerm)

	rf.role = Leader
	// init the nextIndex and matchIndex
	for peer := 0; peer < len(rf.peers); peer++ {
		rf.nextIndex[peer] = rf.log.size()
		// 假设从后往前匹配，不成功就减少
		rf.matchIndex[peer] = 0
		// 一开始就没有匹配的
	}
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	// var term int
	// var isleader bool
	// Your code here (PartA).
	// lock
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.currentTerm, rf.role == Leader
}

// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.role != Leader {
		return 0, 0, false
	}
	rf.log.append(LogEntry{
		true,
		command,
		rf.currentTerm,
	})
	rf.persistLocked()
	LOG(rf.me, rf.currentTerm, DLeader, "Leader accept log [%d]T%d", rf.log.size()-1, rf.currentTerm)

	return rf.log.size() - 1, rf.currentTerm, true
}

// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}
func (rf *Raft) contextLostLocked(role Role, term int) bool {
	return rf.role != role || rf.currentTerm != term

}

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	// rf.log.tailLog = append(rf.log.tailLog, LogEntry{}) //like dummy head
	rf.log = NewLog(InvalidIndex, InvalidTerm, nil, nil)
	// Your initialization code here (PartA, PartB, PartC).
	/*
		init the filed that you have added
	*/
	rf.nextIndex = make([]int, len(peers))
	rf.matchIndex = make([]int, len(peers))
	rf.role = Follower
	rf.currentTerm = 1
	rf.votedFor = -1
	rf.snapPending = false
	rf.applyCh = applyCh
	rf.lastApplied = 0
	rf.commitIndex = 0

	rf.applyCond = sync.NewCond(&rf.mu)
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.electionTicker()
	// go rf.applicationTicker()
	go rf.applicationTicker()

	return rf
}

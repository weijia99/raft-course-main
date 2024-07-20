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
	"math/rand"
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
)

// reset the election time out
func (rf *Raft) resetElectionTimeLocked() {
	// obtain a random election time out
	// plus the current time
	rf.electionStart = time.Now()
	randRange := int64(electionTimeOutMax - electionTimeoutMin)
	rf.electionTimeOut = electionTimeoutMin + time.Duration(rand.Int63()%randRange)
}

// check the election time out
func (rf *Raft) isElectionTimeOutLocked() bool {
	// just check the election time out is out of time
	return time.Since(rf.electionStart) > rf.electionTimeOut
}

// define Role:candidate ,follower,leader
type Role string

const (
	Follower  Role = "follower"
	Candidate Role = "candidate"
	Leader    Role = "leader"
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

	role            Role
	currentTerm     int
	votedFor        int //vote who (-1 present null)
	electionStart   time.Time
	electionTimeOut time.Duration
	// Your data here (PartA, PartB, PartC).
	// add yourself struct
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

}

// state change
func (rf *Raft) becomeFollowerLocked(term int) {
	// compare term ,if term is lower than rf.term ,do not change
	if rf.currentTerm > term {
		// add log
		LOG(rf.me, rf.currentTerm, DError, "cant become follower ,because the term is T%d", term)

		return
	}
	// log
	LOG(rf.me, rf.currentTerm, DLog, "%s->Follower,For T%s->%s", rf.role, rf.currentTerm, term)
	rf.role = Follower
	if rf.currentTerm < term {
		rf.votedFor = -1
	}
	rf.currentTerm = term
	// all term

}

func (rf *Raft) becomeCandidateLocked() {
	// compare term ,if term is lower than rf.term ,do not change
	if rf.role == Leader {
		// add log
		LOG(rf.me, rf.currentTerm, DVote, "cant become candidate ")

		return
	}
	// log
	LOG(rf.me, rf.currentTerm, DVote, "%s->Candidate,For T%d", rf.role, rf.currentTerm+1)
	rf.role = Candidate
	// 选举人的任期+1,并且投票自己
	rf.currentTerm++
	rf.votedFor = rf.me
	// me 代表序号
	// all term

}

func (rf *Raft) becomeLeaderLocked() {

	if rf.role != Candidate {
		LOG(rf.me, rf.currentTerm, DError, "cant become Leader ")

		return

		//
	}
	LOG(rf.me, rf.currentTerm, DLeader, "Become Leader,in T%d", rf.currentTerm)

	rf.role = Leader
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (PartA).
	return term, isleader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist() {
	// Your code here (PartC).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// raftstate := w.Bytes()
	// rf.persister.Save(raftstate, nil)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (PartC).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (PartD).

}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (PartA, PartB).
	// term & candidate index
	Term        int
	CandidateID int //who call this rpc
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (PartA).
	Term        int
	VoteGranted bool //whether granted to the Candidate
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (PartA, PartB).
	// this function is to handle the RequestVote RPC,when receive rpc
	// what should server do
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.Term = rf.currentTerm
	reply.VoteGranted = false
	// if the term is lower than the current term ,reject the vote
	if args.Term < rf.currentTerm {
		LOG(rf.me, rf.currentTerm, DVote, "Reject vote from s%d,For T%d", args.CandidateID, args.Term)
		return

	}
	// if the term is higher than the current term ,become follower
	if args.Term > rf.currentTerm {
		rf.becomeFollowerLocked(args.Term)
	}
	// if the server has voted for other ,reject the vote
	if rf.votedFor != -1 && rf.votedFor != args.CandidateID {
		LOG(rf.me, rf.currentTerm, DVote, "Reject vote from s%d,For T%d", args.CandidateID, args.Term)
		return

	}
	// all pass ,setting reply
	reply.VoteGranted = true
	rf.votedFor = args.CandidateID
	// reset clock
	rf.resetElectionTimeLocked()
	LOG(rf.me, rf.currentTerm, DVote, "->S%d ,vote granted", args.CandidateID)
}

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
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
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
	index := -1
	term := -1
	isLeader := true

	// Your code here (PartB).

	return index, term, isLeader
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
func (rf *Raft) startElection(term int) {
	// count the vote
	vote := 0
	// build RPC call for each peer ,via peer &args
	askVoteFromPeer := func(peer int, args *RequestVoteArgs) {
		// this is client call
		reply := &RequestVoteReply{}
		// above code ,given  a example to call rpc,just pass arg and reply
		// return ok
		ok := rf.sendRequestVote(peer, args, reply)
		if !ok {
			// rpc failed
			LOG(rf.me, rf.currentTerm, DDebug, "Ask vote from s%d,Lost or error", peer)
			return
		}
		// if currentTerm is lower than peer,become follower
		if reply.Term > rf.currentTerm {
			rf.becomeFollowerLocked(reply.Term)
			return
		}
		// double check current role is Candidate && term is equal
		if rf.contextLostLocked(Candidate, term) {
			LOG(rf.me, rf.currentTerm, DVote, "Lost content s%d,abort", peer)

			return
		}
		// if vote granted ,vote++
		if reply.VoteGranted {
			vote++
			// when half of the peer vote ,become leader
			if vote > len(rf.peers)/2 {
				rf.becomeLeaderLocked()
				// start the leader work,send heartbeat & log replication
				go rf.replicationTicker(term)
			}
		}
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()
	// check the term
	if rf.contextLostLocked(Candidate, term) {
		LOG(rf.me, rf.currentTerm, DVote, "Lost content s%d,abort", peer)

		return
		// early stop
	}
	// build the RPC request for each peer
	for peer := 0; peer < len(rf.peers); peer++ {
		if peer == rf.me {
			// jump
			vote++
			continue
		}
		// RPC call
		args := &RequestVoteArgs{rf.currentTerm, rf.me}

	}
}
func (rf *Raft) electionTicker() {
	for rf.killed() == false {

		// Your code here (PartA)
		// Check if a leader election should be started.

		// here only you are not leader ,you can start election,at the same time
		//  you should check the election time out
		rf.mu.Lock()
		if rf.role != Leader && rf.isElectionTimeOutLocked() {
			rf.becomeCandidateLocked()
			// according to the term ,send request vote to all the peers
			// only you term is higher than the other ,you can get the vote
			go rf.startElection(rf.currentTerm)
		}
		rf.mu.Unlock()

		// pause for a random amount of time between 50 and 350
		// milliseconds.
		ms := 50 + (rand.Int63() % 300)
		time.Sleep(time.Duration(ms) * time.Millisecond)
	}
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

	// Your initialization code here (PartA, PartB, PartC).
	/*
		init the filed that you have added
	*/

	rf.role = Follower
	rf.currentTerm = 0
	rf.votedFor = -1
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.electionTicker()

	return rf
}

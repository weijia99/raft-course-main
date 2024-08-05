package raft

import (
	"math/rand"
	"time"
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
		LOG(rf.me, rf.currentTerm, DVote, "<- S%d, Reject voted, Higher term, T%d>T%d", args.CandidateID, rf.currentTerm, args.Term)
		return

	}
	// if the term is higher than the current term ,become follower
	if args.Term > rf.currentTerm {
		rf.becomeFollowerLocked(args.Term)
	}
	// if the server has voted for other ,reject the vote
	if rf.votedFor != -1 {
		LOG(rf.me, rf.currentTerm, DVote, "<- S%d, Reject voted, Already voted to S%d", args.CandidateID, rf.votedFor)
		return

	}
	// all pass ,setting reply
	reply.VoteGranted = true
	rf.votedFor = args.CandidateID
	// reset clock
	rf.resetElectionTimeLocked()
	LOG(rf.me, rf.currentTerm, DVote, "<- S%d, Vote granted", args.CandidateID)
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
		rf.mu.Lock()
		defer rf.mu.Unlock()
		// 多线程竞争
		if !ok {
			// rpc failed
			LOG(rf.me, rf.currentTerm, DDebug, "-> S%d, Ask vote, Lost or error", peer)
			return
		}
		LOG(rf.me, rf.currentTerm, DDebug, "-> S%d, AskVote Reply=", peer)

		// if currentTerm is lower than peer,become follower
		if reply.Term > rf.currentTerm {
			rf.becomeFollowerLocked(reply.Term)
			return
		}
		// double check current role is Candidate && term is equal
		if rf.contextLostLocked(Candidate, term) {
			LOG(rf.me, rf.currentTerm, DVote, "-> S%d, Lost context, abort RequestVoteReply", peer)
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
		LOG(rf.me, rf.currentTerm, DVote, "Lost Candidate[T%d] to %s[T%d], abort RequestVote", rf.role, term, rf.currentTerm)
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
		LOG(rf.me, rf.currentTerm, DDebug, "-> S%d, AskVote, Args=", peer)

		go askVoteFromPeer(peer, args)
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

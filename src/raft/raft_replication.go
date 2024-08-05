package raft

import "time"

// just like slot in the paper
type LogEntry struct {
	// record the log entry
	CommandValid bool //whether the log entry is valid
	Command      interface{}
	Term         int // the log entry's term
}

// rpc args
type AppendEntriesArgs struct {
	Term     int
	LeaderID int
	// one term may have multiple log entries,so the log entries should be a slice
	// the index of the log entry,according to the paper figure 2
	PrevLogIndex int
	// the term of the log entry,according to the paper figure 2
	PrevLogTerm int
	// the log entries which need to be appended
	Entries []LogEntry
}

// rpc reply
type AppendEntriesReply struct {
	Term    int
	Success bool
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	//  name must be the same as the rpc name,AppendEntries from Raft.AppendEntries
	// this function is to handle the AppendEntries RPC,when receive
	// logic is the same as the RequestVote,server should do
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// default reply
	reply.Term = rf.currentTerm
	reply.Success = false

	if args.Term < rf.currentTerm {
		LOG(rf.me, rf.currentTerm, DLog2, "<-S%d,Reject log,Higher term,T%d<T%d", args.LeaderID, args.Term, rf.currentTerm)
		return
	}
	if args.Term >= rf.currentTerm {
		rf.becomeFollowerLocked(args.Term)
	}
	if args.PrevLogIndex >= len(rf.log) {
		// leader log's index is larger than the follower's log size
		// its obvious that the follower's log cant find the index in its log,so reject
		LOG(rf.me, rf.currentTerm, DLog2, "<- S%d, Reject log, Follower log too short, Len:%d < Prev:%d", args.LeaderID, len(rf.log), args.PrevLogIndex)
		return
	}
	if rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {
		// the term of the log entry is not the same as the leader's log entry
		// reject
		LOG(rf.me, rf.currentTerm, DLog2, "<- S%d, Reject log, Term not match, T%d != T%d", args.LeaderID, rf.log[args.PrevLogIndex].Term, args.PrevLogTerm)
		return
	}
	// start to append the log
	rf.log = append(rf.log[:args.PrevLogIndex+1], args.Entries...)
	reply.Success = true
	LOG(rf.me, rf.currentTerm, DLog2, "Follower accept logs: (%d, %d]", args.PrevLogIndex, args.PrevLogIndex+len(args.Entries))

	// reset clock
	rf.resetElectionTimeLocked()
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	// todo: Raft.AppendEntries RPC
	// client call
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) startReplication(term int) bool {
	replicateToPeer := func(peer int, args *AppendEntriesArgs) {
		reply := &AppendEntriesReply{}
		// client call,just copy the code from the RequestVote
		ok := rf.sendAppendEntries(peer, args, reply)
		rf.mu.Lock()
		defer rf.mu.Unlock()
		if !ok {
			LOG(rf.me, rf.currentTerm, DLog, "Replicate to s%d,Lost or error", peer)
			return

		}
		// aligne the term
		if reply.Term > rf.currentTerm {
			rf.becomeFollowerLocked(reply.Term)
			return
		}
		// handle the reply
		//if prev not matched ,try to reduce the nextIndex
		if !reply.Success {
			idx, term := args.PrevLogIndex, args.PrevLogTerm
			for idx > 0 && rf.log[idx].Term == term {
				// all the log entries with the same term should be removed
				idx--
			}
			rf.nextIndex[peer] = idx + 1
			LOG(rf.me, rf.currentTerm, DLog, "Not mathed with s%d in term %d,try next %d", peer, args.PrevLogIndex, rf.nextIndex[peer])
			return
		}
		// update the matchIndex,prev is start ,len is the length of the entries
		// so the matchIndex is the last index of the entries
		rf.matchIndex[peer] = args.PrevLogIndex + len(args.Entries)
		//next index is the last index of the entries +1
		rf.nextIndex[peer] = rf.matchIndex[peer] + 1
	}
	// add lock
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// check the term
	if rf.contextLostLocked(Leader, term) {
		// not leader ,return false
		LOG(rf.me, rf.currentTerm, DLog, "Lost Leader [%d]to %s [T%d]", term, rf.role, rf.currentTerm)
		return false

	}
	// send peers
	for peer := 0; peer < len(rf.peers); peer++ {
		if peer == rf.me {
			// update the matchIndex
			rf.matchIndex[peer] = len(rf.log) - 1
			// next index is the last log index +1
			rf.nextIndex[peer] = len(rf.log)
			continue
		}
		// record the last log index
		preIdx := rf.nextIndex[peer] - 1
		preTerm := rf.log[preIdx].Term
		// build rpc args
		args := &AppendEntriesArgs{rf.currentTerm, rf.me, preIdx, preTerm, rf.log[preIdx+1:]}
		go replicateToPeer(peer, args)
	}
	return true
}

// only during the term appear ,the leader can send the heartbeat
func (rf *Raft) replicationTicker(term int) {
	// this funtion for leader to send the heartbeat (mainly send the log replication)
	// the leader send the heartbeat to all the peers
	// just copy the code from the startElection
	for !rf.killed() {
		ok := rf.startReplication(term)
		if !ok {
			break
		}
		time.Sleep(replicateInterval)
	}

}

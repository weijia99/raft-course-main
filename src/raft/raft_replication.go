package raft

import (
	"fmt"
	"sort"
	"time"
)

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

	// the leader's commitIndex
	LeaderCommit int
}

// rpc reply
type AppendEntriesReply struct {
	Term    int
	Success bool
	// the index of the log entry which is not matched
	// fast retry the log entry
	ConfilictIndex int
	ConfilictTerm  int
}

func (rf *Raft) firstIndexFor(term int) int {
	// find the first index of the log entry with the same term
	for idx, entry := range rf.log {
		if entry.Term == term {
			return idx
		} else if entry.Term > term {
			break
		}
	}
	return InvalidIndex
}
func (args *AppendEntriesArgs) String() string {
	return fmt.Sprintf("Leader-%d, T%d, Prev:[%d]T%d, (%d, %d], CommitIdx: %d",
		args.LeaderID, args.Term, args.PrevLogIndex, args.PrevLogTerm,
		args.PrevLogIndex, args.PrevLogIndex+len(args.Entries), args.LeaderCommit)
}
func (reply *AppendEntriesReply) String() string {
	return fmt.Sprintf("T%d, Sucess: %v, ConflictTerm: [%d]T%d", reply.Term, reply.Success, reply.ConfilictIndex, reply.ConfilictTerm)
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	//  name must be the same as the rpc name,AppendEntries from Raft.AppendEntries
	// this function is to handle the AppendEntries RPC,when receive
	// logic is the same as the RequestVote,server should do
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// default reply
	LOG(rf.me, rf.currentTerm, DDebug, "<- S%d, Append, Prev=[%d]T%d, Commit: %d", args.LeaderID, args.PrevLogIndex, args.PrevLogTerm, args.LeaderCommit)

	reply.Term = rf.currentTerm
	reply.Success = false

	if args.Term < rf.currentTerm {
		LOG(rf.me, rf.currentTerm, DLog2, "<- S%d, Reject log, Higher term, T%d<T%d", args.LeaderID, args.Term, rf.currentTerm)
		return
	}
	if args.Term >= rf.currentTerm {
		rf.becomeFollowerLocked(args.Term)
	}
	// reset clock
	// rf.resetElectionTimeLocked()
	defer func() {
		rf.resetElectionTimeLocked()
		if !reply.Success {
			LOG(rf.me, rf.currentTerm, DLog2, "<- S%d, Follower Conflict: [%d]T%d", args.LeaderID, reply.ConfilictIndex, reply.ConfilictTerm)
			LOG(rf.me, rf.currentTerm, DDebug, "Follower log=%v", rf.logString())
		}
	}()
	if args.PrevLogIndex >= len(rf.log) {
		// leader log's index is larger than the follower's log size
		// its obvious that the follower's log cant find the index in its log,so reject
		LOG(rf.me, rf.currentTerm, DLog2, "<- S%d, Reject log, Follower log too short, Len:%d < Prev:%d", args.LeaderID, len(rf.log), args.PrevLogIndex)
		// update the confilict index
		reply.ConfilictIndex = len(rf.log)
		reply.ConfilictTerm = InvalidTerm
		return
	}
	if rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {
		// the term of the log entry is not the same as the leader's log entry
		// reject
		reply.ConfilictTerm = rf.log[args.PrevLogIndex].Term
		// find the first index of the log entry with the same term
		reply.ConfilictIndex = rf.firstIndexFor(reply.ConfilictTerm)
		LOG(rf.me, rf.currentTerm, DLog2, "<- S%d, Reject log, Term not match, T%d != T%d", args.LeaderID, rf.log[args.PrevLogIndex].Term, args.PrevLogTerm)
		return
	}
	// start to append the log
	rf.log = append(rf.log[:args.PrevLogIndex+1], args.Entries...)
	// update log,persister
	rf.persistLocked()
	reply.Success = true
	LOG(rf.me, rf.currentTerm, DLog2, "current loglength=%d,Follower accept logs: (%d, %d]", len(rf.log), args.PrevLogIndex, args.PrevLogIndex+len(args.Entries))

	// update the commitIndex
	if args.LeaderCommit > rf.commitIndex {
		LOG(rf.me, rf.currentTerm, DApply, "Follower update the commit index %d->%d", rf.commitIndex, args.LeaderCommit)

		rf.commitIndex = args.LeaderCommit
		if rf.commitIndex >= len(rf.log) {
			rf.commitIndex = len(rf.log) - 1
		}
		// start to apply the log entry
		rf.applyCond.Signal()
	}

}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	// todo: Raft.AppendEntries RPC
	// client call
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) getMajorityIndexLocked() int {

	// get the majority index
	tmpIndex := make([]int, len(rf.matchIndex))
	copy(tmpIndex, rf.matchIndex)
	// sort the index
	sort.Ints(sort.IntSlice(tmpIndex))
	majorityIdx := (len(tmpIndex) - 1) / 2
	LOG(rf.me, rf.currentTerm, DDebug, "Match index after sort: %v, majority[%d]=%d", tmpIndex, majorityIdx, tmpIndex[majorityIdx])

	return tmpIndex[majorityIdx]
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
		LOG(rf.me, rf.currentTerm, DDebug, "-> S%d, Append, Reply=%v", peer, reply.String())
		// aligne the term
		if reply.Term > rf.currentTerm {
			rf.becomeFollowerLocked(reply.Term)
			return
		}
		// check context lost
		if rf.contextLostLocked(Leader, term) {
			LOG(rf.me, rf.currentTerm, DLog, "-> S%d, Context Lost, T%d:Leader->T%d:%s", peer, term, rf.currentTerm, rf.role)
			return
		}
		// handle the reply
		//if prev not matched ,try to reduce the nextIndex
		if !reply.Success {
			// // idx, term := args.PrevLogIndex, args.PrevLogTerm
			// idx := rf.nextIndex[peer] - 1
			// term := rf.log[idx].Term
			// for idx > 0 && rf.log[idx].Term == term {
			// 	// all the log entries with the same term should be removed
			// 	idx--
			// }
			// rf.nextIndex[peer] = idx + 1
			prevNext := rf.nextIndex[peer]
			if reply.ConfilictTerm == InvalidTerm {
				rf.nextIndex[peer] = reply.ConfilictIndex
			} else {
				// leader 回退到term的第一个index
				firstTermIndex := rf.firstIndexFor(reply.ConfilictTerm)
				if firstTermIndex != InvalidIndex {
					rf.nextIndex[peer] = firstTermIndex + 1
				} else {
					rf.nextIndex[peer] = reply.ConfilictIndex
				}
			}
			// avoid the late reply move the nextIndex forward again
			if rf.nextIndex[peer] > prevNext {
				rf.nextIndex[peer] = prevNext
			}
			LOG(rf.me, rf.currentTerm, DLog, "-> S%d, Not matched at Prev=[%d]T%d, Try next Prev=[%d]T%d", peer, args.PrevLogIndex, rf.log[args.PrevLogIndex].Term, rf.nextIndex[peer]-1, rf.log[rf.nextIndex[peer]-1].Term)
			LOG(rf.me, rf.currentTerm, DDebug, "Leader log=%v", rf.logString())
			// LOG(rf.me, rf.currentTerm, DLog, "Not mathed with s%d in term %d,try next %d", peer, args.PrevLogIndex, rf.nextIndex[peer])
			return
		}
		// update the matchIndex,prev is start ,len is the length of the entries
		// so the matchIndex is the last index of the entries
		rf.matchIndex[peer] = args.PrevLogIndex + len(args.Entries)
		//next index is the last index of the entries +1
		rf.nextIndex[peer] = rf.matchIndex[peer] + 1

		// check the commitIndex，obtain the majority
		// that means most of the peers have the same log entry
		// so we can update commitIndex,that means the log entry has been committed
		// the commitIndex is the last index of the log entry that has been committed
		majorityMatched := rf.getMajorityIndexLocked()
		if majorityMatched > rf.commitIndex && rf.log[majorityMatched].Term == rf.currentTerm {
			LOG(rf.me, rf.currentTerm, DApply, "Leader update the commit index %d->%d", rf.commitIndex, majorityMatched)

			rf.commitIndex = majorityMatched
			// wake up the applyCond
			rf.applyCond.Signal()
			// start use log replication,to make the same stat
		}
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
		args := &AppendEntriesArgs{rf.currentTerm, rf.me, preIdx, preTerm, rf.log[preIdx+1:], rf.commitIndex}
		LOG(rf.me, rf.currentTerm, DDebug, "-> S%d, Append, %v", peer, args.String())
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

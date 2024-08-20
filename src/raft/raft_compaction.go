package raft

import "fmt"

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (PartD).
	// Your code here (PartD).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	LOG(rf.me, rf.currentTerm, DSnap, "Snap on %d", index)

	if index <= rf.log.snapLastIdx || index > rf.commitIndex {
		LOG(rf.me, rf.currentTerm, DSnap, "Could not snapshot beyond [%d, %d]", rf.log.snapLastIdx+1, rf.commitIndex)
		return
	}

	rf.log.doSnapshot(index, snapshot)
	rf.persistLocked()

}

// rpc args

type InstallSnapshotArgs struct {
	Term     int
	LeaderId int
	// 	领导人的 ID，以便于跟随者重定向请求

	// 快照中包含的最后日志条目的任期号
	LastIncludeIndex int
	LastIncludeTerm  int
	SnapShot         []byte
}
type InstallSnapshotReply struct {
	Term int
}

func (reply *InstallSnapshotReply) String() string {
	return fmt.Sprintf("T%d", reply.Term)
}

// rpc call
func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
	return ok
}

// InstallSnapshot
func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {

	// 安装InstallSnapshot
	rf.mu.Lock()
	defer rf.mu.Unlock()
	LOG(rf.me, rf.currentTerm, DDebug, "<- S%d, RecvSnap, Args=%v", args.LeaderId, args.String())
	reply.Term = rf.currentTerm

	// align term
	if args.Term < rf.currentTerm {
		LOG(rf.me, rf.currentTerm, DSnap, "<- S%d, Reject Snap, Higher Term, T%d>T%d", args.LeaderId, rf.currentTerm, args.Term)
		return
	}
	if args.Term > rf.currentTerm {
		rf.becomeFollowerLocked(args.Term)
	}
	if rf.log.snapLastIdx >= args.LastIncludeIndex {
		LOG(rf.me, rf.currentTerm, DSnap, "<- S%d, Reject Snap, Already installed, Last: %d>=%d", args.LeaderId, rf.log.snapLastIdx, args.LastIncludeIndex)
		return
	}
	rf.log.installSnapshot(args.LastIncludeIndex, args.LastIncludeTerm, args.SnapShot)
	rf.persistLocked()
	rf.snapPending = true
	rf.applyCond.Signal()

}
func (args *InstallSnapshotArgs) String() string {
	return fmt.Sprintf("Leader-%d, T%d, Last: [%d]T%d", args.LeaderId, args.Term, args.LastIncludeIndex, args.LastIncludeTerm)
}
func (rf *Raft) installOnPeer(peer, term int, args *InstallSnapshotArgs) {
	reply := &InstallSnapshotReply{}
	ok := rf.sendInstallSnapshot(peer, args, reply)

	rf.mu.Lock()
	defer rf.mu.Unlock()
	if !ok {
		LOG(rf.me, rf.currentTerm, DSnap, "-> S%d, Lost or crashed", peer)
		return
	}
	LOG(rf.me, rf.currentTerm, DDebug, "-> S%d, InstallSnap, Reply=%v", peer, reply.String())

	// align the term
	if reply.Term > rf.currentTerm {
		rf.becomeFollowerLocked(reply.Term)
		return
	}
	// update the match and next
	if args.LastIncludeIndex > rf.matchIndex[peer] { // to avoid disorder reply
		rf.matchIndex[peer] = args.LastIncludeIndex
		rf.nextIndex[peer] = args.LastIncludeIndex + 1
	}

	// note: we need not try to update the commitIndex again,
	// because the snapshot included indexes are all committed
}

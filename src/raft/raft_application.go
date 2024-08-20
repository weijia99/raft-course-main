package raft

// func (rf *Raft) applyTicker() {

// 	for !rf.killed() {
// 		rf.mu.Lock()
// 		//wait for the condition
// 		rf.applyCond.Wait()
// 		// get all the entries
// 		entries := make([]LogEntry, 0)

// 		// should start from rf.lastApplied+1 instead of rf.lastApplied
// 		for i := rf.lastApplied + 1; i <= rf.commitIndex; i++ {
// 			entries = append(entries, rf.log.at(i))
// 		}
// 		rf.mu.Unlock()

// 		for i, entry := range entries {
// 			rf.applyCh <- ApplyMsg{
// 				CommandValid: entry.CommandValid,
// 				Command:      entry.Command,
// 				CommandIndex: rf.lastApplied + 1 + i,
// 			}
// 		}

//			rf.mu.Lock()
//			LOG(rf.me, rf.currentTerm, DApply, "Apply log for [%d, %d]", rf.lastApplied+1, rf.lastApplied+len(entries))
//			rf.lastApplied += len(entries)
//			rf.mu.Unlock()
//		}
//	}
//
// --- raft_application.go
func (rf *Raft) applicationTicker() {
	for !rf.killed() {
		rf.mu.Lock()
		rf.applyCond.Wait()
		entries := make([]LogEntry, 0)
		snapPendingInstall := rf.snapPending

		if !snapPendingInstall {
			for i := rf.lastApplied + 1; i <= rf.commitIndex; i++ {
				entries = append(entries, rf.log.at(i))
			}
		}
		rf.mu.Unlock()

		if !snapPendingInstall {
			for i, entry := range entries {
				rf.applyCh <- ApplyMsg{
					CommandValid: entry.CommandValid,
					Command:      entry.Command,
					CommandIndex: rf.lastApplied + 1 + i, // must be cautious
				}
			}
		} else {
			rf.applyCh <- ApplyMsg{
				SnapshotValid: true,
				Snapshot:      rf.log.snapshot,
				SnapshotIndex: rf.log.snapLastIdx,
				SnapshotTerm:  rf.log.snapLastTerm,
			}
		}

		rf.mu.Lock()
		if !snapPendingInstall {
			LOG(rf.me, rf.currentTerm, DApply, "Apply log for [%d, %d]", rf.lastApplied+1, rf.lastApplied+len(entries))
			rf.lastApplied += len(entries)
		} else {
			LOG(rf.me, rf.currentTerm, DApply, "Install Snapshot for [0, %d]", rf.log.snapLastIdx)
			rf.lastApplied = rf.log.snapLastIdx
			if rf.commitIndex < rf.lastApplied {
				rf.commitIndex = rf.lastApplied
			}
			rf.snapPending = false
		}
		rf.mu.Unlock()
	}
}

package raft

func (rf *Raft) applicationTicker() {
	// copy from electionTicker
	if !rf.killed() {
		rf.mu.Lock()
		//wait for the condition
		rf.applyCond.Wait()
		// get all the entries
		entries := make([]LogEntry, 0)
		// get the entries
		for i := rf.lastApplied + 1; i <= rf.commitIndex; i++ {
			// last applied is the last index of the log
			// commitIndex is the last index of the log that has been committed
			entries = append(entries, rf.log[i])
		}
		// apply the entries
		for i, entry := range entries {
			// apply the entries
			rf.applyCh <- ApplyMsg{
				CommandValid: entry.CommandValid,
				Command:      entry.Command,
				CommandIndex: rf.lastApplied + i + 1,
			}
		}
		rf.mu.Lock()
		// update last lastApplied
		LOG(rf.me, rf.currentTerm, DApply, ";Apply log for [%d, %d]", rf.lastApplied+1, rf.lastApplied+len(entries))
		rf.lastApplied += len(entries)
		rf.mu.Unlock()
	}
}

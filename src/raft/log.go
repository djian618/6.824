package raft

import (
	"sync"
	"fmt"
)

type Log struct {
	entries     []LogEntry
	voteFor	    int 
	mutex       sync.Mutex
}

type LogEntry struct{
	Term int
	Command interface{}
}

func NewLog() *Log {
	return &Log{}
}


func (l *Log) GetVotefor() int {
	return l.voteFor
}

func (l *Log) GetPrevIndex() int {
	return (len(l.entries))
	
}

func (l *Log) GetCommandFromIndex(i int) int{
	l.mutex.Lock()
	defer l.mutex.Unlock()
	return l.entries[i].Command.(int)
}

func (l *Log) GetCurrentTerm() int {
	l.mutex.Lock()
	defer l.mutex.Unlock()
	if (len(l.entries)==0) {
		return 0
	}else {
		return l.entries[len(l.entries)-1].Term
	}
}

func (l *Log) GetTermFromIndex(index int) int{
	// l.mutex.Lock()
	// defer l.mutex.Unlock()
	if(index==0) {
		return 0
	}

	if(index > len(l.entries)  || (index==0)) {
		fmt.Printf("index %d greater than its len %d", index, len(l.entries))
	}
	return l.entries[index-1].Term
}

func (l *Log) GetTermForLastIndex() int {
	l.mutex.Lock()
	defer l.mutex.Unlock()
	if (len(l.entries)==0) {
		return 0
	}else {
		return l.entries[len(l.entries)-1].Term
	}
}

func(l *Log) GetLastCommand() interface {} {
	return l.entries[len(l.entries)-1].Command
}


func (l *Log) GetLastIndex() int {
	l.mutex.Lock()
	defer l.mutex.Unlock()
	return len(l.entries)
}


func (l *Log) IsEmpty() bool {
	l.mutex.Lock()
	defer l.mutex.Unlock()
	return (len(l.entries) == 0)
}

func (l *Log) TermMatchIndex(prevLogIndex int, prevLogTerm int) bool {
	l.mutex.Lock()
	defer l.mutex.Unlock()	
	if(prevLogIndex==0) {
		return true
	}
	if(len(l.entries)<prevLogIndex)  {
		return false
	}
	return (l.entries[prevLogIndex-1].Term == prevLogTerm)
}

/*
Find the last index that is not conflicting
*/
func (l *Log) FindConflictIndex(prevLogIndex int,leaderLog []LogEntry) int {
	l.mutex.Lock()
	defer l.mutex.Unlock()	
	for i := prevLogIndex; i<len(l.entries); i++ {
		if(l.entries[i].Term != leaderLog[i].Term) {
			return i
		}
	}
	return len(l.entries)
}

// delete follower log after index 
func (l *Log) TruncateLogAfter(index int) {
	l.mutex.Lock()
	defer l.mutex.Unlock()	
	if(index== len(l.entries)) {
		return 
	}
	if(index == 0) {
		l.entries = nil
		return
	}
	l.entries = l.entries[:index]
}

// append entries to log
func(l *Log) AppendEntries(entries_append []LogEntry) {
	l.mutex.Lock()
	defer l.mutex.Unlock()	
	l.entries = append(l.entries, entries_append...)
}

func(l *Log) AppendEntry (entry LogEntry) {
	l.mutex.Lock()
	defer l.mutex.Unlock()	
	l.entries = append(l.entries, entry)

}

// get logs start from 
func(l *Log) GettingIndexfrom(index int) []LogEntry {
	l.mutex.Lock()
	defer l.mutex.Unlock()
	//BPrintf("Getting Index After %d", index)
	if(index>=len(l.entries)) {
		return nil	
	}
	if(index==0) {
		return l.entries
	}
	return l.entries[index:]
}




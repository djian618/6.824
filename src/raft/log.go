package raft

import (
	"sync"
)

type Log struct {
	entries     []*LogEntry
	voteFor	    int 
	commitIndex int
	lastApplied int 
	mutex       sync.Mutex
}

type LogEntry struct{
	Term int
	command string
}

func NewLog() *Log {
	return &Log{}
}


func (l *Log) GetVotefor() int {
	return l.voteFor
}

func (l *Log) GetPrevIndex() int {
	l.mutex.Lock()
	defer l.mutex.Unlock()	
	if (len(l.entries)==0) {
		return 0
	}else {
		return (len(l.entries) - 1)
	}
}

func (l *Log) GetCommitIndex() int {
	l.mutex.Lock()
	defer l.mutex.Unlock()	
	return l.commitIndex
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


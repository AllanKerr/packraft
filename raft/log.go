package raft

import (
	"log"

	"github.com/allankerr/packraft/protos"
)

type Log struct {
	entries []*protos.LogEntry
}

func (l *Log) Append(entry *protos.LogEntry) {
	l.entries = append(l.entries, entry)
}

func (l *Log) AppendTail(startIndex uint64, entries []*protos.LogEntry) {
	if startIndex == 0 {
		log.Fatalf("unable to append tail starting at 0")
	}
	logIndex := startIndex - 1
	entryIndex := 0
	for logIndex < uint64(len(l.entries)) && entryIndex < len(entries) && l.entries[logIndex].Term == entries[entryIndex].Term {
		logIndex += 1
		entryIndex += 1
	}
	l.entries = append(l.entries[:logIndex], entries[entryIndex:]...)
}

func (l *Log) NextLogIndex() uint64 {
	return l.LastLogIndex() + 1
}

func (l *Log) LastLogIndex() uint64 {
	return uint64(len(l.entries))
}

func (l *Log) LastLogIndexAndTerm() (uint64, uint64) {
	if len(l.entries) == 0 {
		return 0, 0
	}
	index := uint64(len(l.entries))
	e := l.entries[index-1]
	return index, e.Term
}

func (l *Log) GetTerm(index uint64) uint64 {
	if index == 0 {
		return 0
	}
	e := l.Get(index)
	return e.Term
}

func (l *Log) Get(index uint64) *protos.LogEntry {
	if index == 0 {
		return &protos.LogEntry{Term: 0, Command: nil}
	}
	return l.entries[index-1]
}

func (l *Log) GetTail(startIndex uint64) []*protos.LogEntry {
	if startIndex == 0 {
		log.Fatal("unable to get tail starting at 0")
	}
	return l.entries[startIndex-1:]
}

func (l *Log) HasEntry(index uint64, term uint64) bool {
	if index == 0 && term == 0 {
		return true
	}
	if index-1 >= uint64(len(l.entries)) {
		return false
	}
	entry := l.Get(index)
	return entry.Term == term
}

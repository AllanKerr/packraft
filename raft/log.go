package raft

type Entry struct {
	Term    uint64
	Command []byte
}

type Log struct {
	entries []Entry
}

func (l *Log) Append(entry Entry) {
	l.entries = append(l.entries, entry)
}

func (l *Log) NextLogIndex() uint64 {
	return l.LastLogIndex() + 1
}

func (l *Log) LastLogIndex() uint64 {
	return uint64(len(l.entries))
}

func (l *Log) Get(index uint64) Entry {
	if index == 0 {
		return Entry{Term: 0, Command: nil}
	}
	return l.entries[index-1]
}

func (l *Log) Tail(startIndex uint64) []Entry {
	return l.entries[startIndex-1:]
}

func (l *Log) HasEntry(index uint64, term uint64) bool {
	entry := l.Get(index)
	return entry.Term == term
}

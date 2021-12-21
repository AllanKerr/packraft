package raft

import (
	"context"
	"errors"
	"log"
	"math/rand"
	"net"
	"reflect"
	"sort"
	"time"

	"github.com/allankerr/packraft/protos"
)

var (
	ErrNotLeader = errors.New("node is not leader")
)

const (
	tickDuration = time.Millisecond * 1

	maxElectionTimeoutTicks = 1000
	minElectionTimeoutTicks = 500

	maxHeartbeatTimeoutTicks = 100
	minHeartbeatTimeoutTicks = 50
)

type tickMessage struct {
}

type proposeRequest struct {
	cmd []byte
}

type proposeResponse struct {
	index uint64
}

type raftState struct {
	me      uint32
	cluster map[uint32]string

	timeoutTicks int
	term         uint64
	votedFor     *uint32

	log              *Log
	commitIndex      uint64
	lastAppliedIndex uint64

	applyCh chan LogEntry
}

func (rs *raftState) commit() {
	// TODO: Commit persistent state
}

func (rs *raftState) resetTimeout(min int, max int) {
	rs.timeoutTicks = randInt(min, max)
}

func (rs *raftState) updateTerm(term uint64) bool {
	if rs.term >= term {
		return false
	}
	rs.term = term
	rs.votedFor = nil
	return true
}

type StateType uint16

const (
	Unknown   StateType = 0
	Follower            = 1
	Candidate           = 2
	Leader              = 3
	Passive             = 4
)

type RequestHandler interface {
	Execute(cur *machineState, rs *raftState, c *Client, ctx context.Context, msg interface{}) (*machineState, interface{}, error)
}

type ResponseHandler interface {
	Execute(cur *machineState, rs *raftState, c *Client, serverID uint32, msg interface{}, err error) *machineState
}

type machineState struct {
	stateType        StateType
	requestHandlers  []RequestHandler
	responseHandlers []ResponseHandler

	votes      int
	matchIndex map[uint32]uint64
	nextIndex  map[uint32]uint64
}

func newFollowerState() *machineState {
	state := new(machineState)
	state.stateType = Follower
	state.requestHandlers = []RequestHandler{
		electionTickHandler{},
		rejectProposeHandler{},
		voteRequestHandler{},
		appendEntriesHandler{},
	}
	state.responseHandlers = []ResponseHandler{
		rejectVoteResponseHandler{},
	}
	return state
}

func newCandidateState() *machineState {
	state := new(machineState)
	state.stateType = Candidate
	state.requestHandlers = []RequestHandler{
		electionTickHandler{},
		rejectProposeHandler{},
		voteRequestHandler{},
		appendEntriesHandler{},
	}
	state.responseHandlers = []ResponseHandler{
		voteResponseHandler{},
	}
	state.votes = 1
	return state
}

func newLeaderState(rs *raftState) *machineState {
	state := new(machineState)
	state.stateType = Leader
	state.requestHandlers = []RequestHandler{
		heartbeatTickHandler{},
		leaderProposeHandler{},
		voteRequestHandler{},
	}
	state.responseHandlers = []ResponseHandler{
		appendEntriesResponseHandler{},
		rejectVoteResponseHandler{},
	}
	state.nextIndex = make(map[uint32]uint64)
	state.matchIndex = make(map[uint32]uint64)
	for id := range rs.cluster {
		state.matchIndex[id] = 0
		state.nextIndex[id] = rs.log.NextLogIndex()
	}
	state.matchIndex[rs.me] = rs.log.LastLogIndex()
	return state
}

type LogEntry struct {
	Index   uint64
	Command []byte
}

type Raft struct {
	client    *Client
	server    *Server
	raftState *raftState

	ticker     *time.Ticker
	requestCh  chan IncomingRequestEnvelope
	responseCh chan IncomingResponseEnvelope

	C chan LogEntry
}

type RaftOpts = func(*Raft)

func New(id uint32, cluster map[uint32]string, opts ...RaftOpts) *Raft {
	requestCh := make(chan IncomingRequestEnvelope)
	responseCh := make(chan IncomingResponseEnvelope)

	rs := &raftState{
		me:      id,
		cluster: cluster,
		log:     &Log{},
		applyCh: make(chan LogEntry),
	}
	rf := &Raft{
		client:     NewClient(responseCh),
		server:     NewServer(requestCh),
		raftState:  rs,
		ticker:     time.NewTicker(tickDuration),
		requestCh:  requestCh,
		responseCh: responseCh,
		C:          rs.applyCh,
	}
	for _, opt := range opts {
		opt(rf)
	}
	return rf
}

func (rf *Raft) Start(lis net.Listener) error {
	cur := newFollowerState()
	rf.raftState.resetTimeout(minElectionTimeoutTicks, maxElectionTimeoutTicks)
	for id, addr := range rf.raftState.cluster {
		if id != rf.raftState.me {
			if err := rf.client.Connect(id, addr); err != nil {
				return err
			}
		}
	}
	go rf.stateMachineLoop(cur)
	return rf.server.Serve(lis)
}

func (rf *Raft) Propose(ctx context.Context, cmd []byte) (uint64, error) {
	propose := IncomingRequestEnvelope{
		ctx:        ctx,
		msg:        proposeRequest{cmd: cmd},
		responseCh: make(chan OutgoingResponseEnvelope),
	}
	defer close(propose.responseCh)
	rf.requestCh <- propose
	out := <-propose.responseCh
	if out.err != nil {
		return 0, out.err
	}
	res := out.msg.(proposeResponse)
	return res.index, nil
}

func (rf *Raft) stateMachineLoop(cur *machineState) {
	tick := IncomingRequestEnvelope{
		ctx:        context.Background(),
		msg:        tickMessage{},
		responseCh: make(chan OutgoingResponseEnvelope, 1),
	}
	defer close(tick.responseCh)
	for {
		var next *machineState
		select {
		case req := <-rf.requestCh:
			next = rf.executeRequest(cur, req)
		case res := <-rf.responseCh:
			next = rf.executeResponse(cur, res)
		case <-rf.ticker.C:
			next = rf.executeRequest(cur, tick)
			if out := <-tick.responseCh; out.err != nil {
				log.Fatalf("tick error: %v", out.err)
			}
		}
		cur = next
	}
}

func (rf *Raft) executeRequest(cur *machineState, req IncomingRequestEnvelope) *machineState {

	var next *machineState
	var res interface{}
	var err error
	for _, h := range cur.requestHandlers {
		next, res, err = h.Execute(cur, rf.raftState, rf.client, req.ctx, req.msg)
		if next != nil {
			break
		}
	}
	if next == nil {
		log.Fatalf("missing %v request handler for message %v{%v}", cur.stateType, reflect.TypeOf(req.msg), req.msg)
	}
	rf.raftState.commit()
	rf.client.Commit()
	req.responseCh <- OutgoingResponseEnvelope{msg: res, err: err}
	return next
}

func (rf *Raft) executeResponse(cur *machineState, res IncomingResponseEnvelope) *machineState {

	var next *machineState
	for _, h := range cur.responseHandlers {
		next = h.Execute(cur, rf.raftState, rf.client, res.serverID, res.msg, res.err)
		if next != nil {
			break
		}
	}
	if next == nil {
		log.Fatalf("missing %v response handler for message %v{%v}", cur.stateType, reflect.TypeOf(res.msg), res.msg)
	}
	rf.raftState.commit()
	rf.client.Commit()
	return next
}

type electionTickHandler struct {
}

func (h electionTickHandler) Execute(cur *machineState, rs *raftState, c *Client, ctx context.Context, msg interface{}) (*machineState, interface{}, error) {
	if _, ok := msg.(tickMessage); !ok {
		return nil, nil, nil
	}
	rs.timeoutTicks -= 1
	if rs.timeoutTicks != 0 {
		return cur, nil, nil
	}
	if len(rs.cluster) == 1 {
		next := newLeaderState(rs)
		sendAllEntries(rs, c, next.nextIndex)
		return next, nil, nil
	}
	rs.term += 1
	rs.votedFor = &rs.me
	rs.resetTimeout(minElectionTimeoutTicks, maxElectionTimeoutTicks)

	for id := range rs.cluster {
		if id != rs.me {
			index, term := rs.log.LastLogIndexAndTerm()
			req := protos.RequestVoteRequest{
				Term:         rs.term,
				CandidateId:  rs.me,
				LastLogIndex: index,
				LastLogTerm:  term,
			}
			res := protos.RequestVoteResponse{}
			c.RequestVote(id, &req, &res)
		}
	}
	return newCandidateState(), nil, nil
}

type heartbeatTickHandler struct {
}

func (h heartbeatTickHandler) Execute(cur *machineState, rs *raftState, c *Client, ctx context.Context, msg interface{}) (*machineState, interface{}, error) {
	if _, ok := msg.(tickMessage); !ok {
		return nil, nil, nil
	}
	rs.timeoutTicks -= 1
	if rs.timeoutTicks != 0 {
		return cur, nil, nil
	}
	sendAllEntries(rs, c, cur.nextIndex)
	return cur, nil, nil
}

type leaderProposeHandler struct {
}

func (h leaderProposeHandler) Execute(cur *machineState, rs *raftState, c *Client, ctx context.Context, msg interface{}) (*machineState, interface{}, error) {
	req, ok := msg.(proposeRequest)
	if !ok {
		return nil, nil, nil
	}
	rs.log.Append(&protos.LogEntry{
		Term:    rs.term,
		Command: req.cmd,
	})
	cur.matchIndex[rs.me] = rs.log.LastLogIndex()
	cur.nextIndex[rs.me] = rs.log.NextLogIndex()
	return cur, proposeResponse{}, nil
}

type rejectProposeHandler struct {
}

func (h rejectProposeHandler) Execute(cur *machineState, rs *raftState, c *Client, ctx context.Context, msg interface{}) (*machineState, interface{}, error) {
	if _, ok := msg.(proposeRequest); !ok {
		return nil, nil, nil
	}
	return cur, proposeResponse{}, ErrNotLeader
}

type voteRequestHandler struct {
}

func (h voteRequestHandler) Execute(cur *machineState, rs *raftState, c *Client, ctx context.Context, msg interface{}) (*machineState, interface{}, error) {
	req, ok := msg.(*protos.RequestVoteRequest)
	if !ok {
		return nil, nil, nil
	}
	if !rs.updateTerm(req.Term) {
		return cur, &protos.RequestVoteResponse{Term: rs.term, VoteGranted: false}, nil
	}
	next := cur
	voteGranted := h.hasVote(rs, req.CandidateId, req.Term, req.LastLogIndex, req.LastLogTerm)
	if voteGranted {
		rs.votedFor = &req.CandidateId
		rs.resetTimeout(minElectionTimeoutTicks, maxElectionTimeoutTicks)
		next = newFollowerState()
	}
	return next, &protos.RequestVoteResponse{Term: rs.term, VoteGranted: voteGranted}, nil
}

func (h voteRequestHandler) hasVote(rs *raftState, candidateID uint32, currentTerm uint64, lastLogIndex uint64, lastLogTerm uint64) bool {
	if currentTerm < rs.term {
		return false
	}
	if rs.votedFor != nil && *rs.votedFor != candidateID {
		return false
	}
	index, term := rs.log.LastLogIndexAndTerm()
	if lastLogTerm < term {
		return false
	}
	if lastLogTerm == term && lastLogIndex < index {
		return false
	}
	return true
}

type voteResponseHandler struct {
}

func (h voteResponseHandler) Execute(cur *machineState, rs *raftState, c *Client, serverID uint32, msg interface{}, err error) *machineState {
	res, ok := msg.(*protos.RequestVoteResponse)
	if !ok {
		return nil
	}
	if err != nil {
		return cur
	}
	if rs.updateTerm(res.Term) {
		rs.resetTimeout(minElectionTimeoutTicks, maxElectionTimeoutTicks)
		return newFollowerState()
	}
	if res.Term != rs.term {
		return cur
	}
	if res.VoteGranted {
		cur.votes += 1
	}
	majority := len(rs.cluster)/2 + 1
	if cur.votes < majority {
		return cur
	}
	rs.term += 1
	rs.votedFor = nil

	next := newLeaderState(rs)
	sendAllEntries(rs, c, next.nextIndex)
	return next
}

type rejectVoteResponseHandler struct {
}

func (h rejectVoteResponseHandler) Execute(cur *machineState, rs *raftState, c *Client, serverID uint32, msg interface{}, err error) *machineState {
	res, ok := msg.(*protos.RequestVoteResponse)
	if !ok {
		return nil
	}
	if err != nil {
		return cur
	}
	if rs.updateTerm(res.Term) {
		rs.resetTimeout(minElectionTimeoutTicks, maxElectionTimeoutTicks)
		return newFollowerState()
	}
	return cur
}

type appendEntriesHandler struct {
}

func (h appendEntriesHandler) Execute(cur *machineState, rs *raftState, c *Client, ctx context.Context, msg interface{}) (*machineState, interface{}, error) {
	req, ok := msg.(*protos.AppendEntriesRequest)
	if !ok {
		return nil, nil, nil
	}
	if req.Term < rs.term {
		return cur, &protos.AppendEntriesResponse{
			Term:    rs.term,
			Success: false,
		}, nil
	}
	rs.updateTerm(req.Term)
	rs.resetTimeout(minElectionTimeoutTicks, maxElectionTimeoutTicks)

	var conflictIndex, conflictTerm uint64
	success := rs.log.HasEntry(req.PrevLogIndex, req.PrevLogTerm)
	if success {
		rs.log.AppendTail(req.PrevLogIndex+1, req.Entries)
		rs.commitIndex = uint64Min(req.LeaderCommit, rs.log.LastLogIndex())
		applyEntries(rs)
	} else {
		conflictIndex, conflictTerm = findConflictIndexAndTerm(rs.log, req.PrevLogIndex, req.PrevLogTerm)
	}
	return newFollowerState(), &protos.AppendEntriesResponse{
		Term:          rs.term,
		Success:       success,
		LastLogIndex:  rs.log.LastLogIndex(),
		ConflictIndex: conflictIndex,
		ConflictTerm:  conflictTerm,
	}, nil
}

type appendEntriesResponseHandler struct {
}

func (h appendEntriesResponseHandler) Execute(cur *machineState, rs *raftState, c *Client, serverID uint32, msg interface{}, err error) *machineState {
	res, ok := msg.(*protos.AppendEntriesResponse)
	if !ok {
		return nil
	}
	if err != nil {
		return cur
	}
	if rs.updateTerm(res.Term) {
		return newFollowerState()
	}
	if res.Term != rs.term {
		return cur
	}
	if !res.Success {
		cur.nextIndex[serverID] = findNextIndex(rs.log, res.ConflictIndex, res.ConflictTerm)
		return cur
	}
	cur.matchIndex[serverID] = uint64Max(cur.matchIndex[serverID], res.LastLogIndex)
	cur.nextIndex[serverID] = uint64Max(cur.nextIndex[serverID], res.LastLogIndex+1)

	rs.commitIndex = computeCommitIndex(cur.matchIndex)
	applyEntries(rs)
	return cur
}

func sendAllEntries(rs *raftState, c *Client, nextIndex map[uint32]uint64) {
	rs.resetTimeout(minHeartbeatTimeoutTicks, maxHeartbeatTimeoutTicks)
	for id := range rs.cluster {
		if id != rs.me {
			sendServerEntries(rs, c, id, nextIndex[id])
		}
	}
}

func sendServerEntries(rs *raftState, c *Client, id uint32, nextIndex uint64) {

	prevIndex := nextIndex - 1
	prev := rs.log.Get(prevIndex)

	req := &protos.AppendEntriesRequest{
		Term:         rs.term,
		LeaderId:     rs.me,
		PrevLogIndex: prevIndex,
		PrevLogTerm:  prev.Term,
		Entries:      rs.log.GetTail(nextIndex),
		LeaderCommit: rs.commitIndex,
	}
	c.AppendEntries(id, req, &protos.AppendEntriesResponse{})
}

func computeCommitIndex(matchIndex map[uint32]uint64) uint64 {

	sorted := make([]uint64, 0, len(matchIndex))
	for _, commitIndex := range matchIndex {
		sorted = append(sorted, commitIndex)
	}
	sort.Slice(sorted, func(i, j int) bool { return sorted[i] < sorted[j] })
	majorityIndex := len(matchIndex) / 2
	return sorted[majorityIndex]
}

func findConflictIndexAndTerm(log *Log, prevLogIndex uint64, prevLogTerm uint64) (uint64, uint64) {
	if prevLogIndex >= log.NextLogIndex() {
		return log.NextLogIndex(), 0
	}
	conflictIndex := prevLogIndex
	conflictTerm := log.GetTerm(prevLogIndex)

	for conflictIndex > 1 && log.GetTerm(conflictIndex) == conflictTerm {
		conflictIndex--
	}
	return conflictIndex, conflictTerm
}

func findNextIndex(log *Log, conflictIndex uint64, conflictTerm uint64) uint64 {
	if conflictTerm == 0 {
		return conflictIndex
	}
	var lastIndexOfTerm uint64
	for i := log.LastLogIndex(); i >= 1; i-- {
		if log.GetTerm(i) == conflictTerm {
			lastIndexOfTerm = i
			break
		}
	}
	if lastIndexOfTerm >= 1 {
		return lastIndexOfTerm
	}
	return conflictIndex
}

func applyEntries(rs *raftState) {
	for i := rs.lastAppliedIndex + 1; i <= rs.commitIndex; i += 1 {
		entry := rs.log.Get(i)
		rs.applyCh <- LogEntry{
			Index:   i,
			Command: entry.Command,
		}
	}
	rs.lastAppliedIndex = rs.commitIndex
}

func randInt(min int, max int) int {
	return rand.Intn(max-min+1) + min
}

func uint64Max(a uint64, b uint64) uint64 {
	if a > b {
		return a
	}
	return b
}

func uint64Min(a uint64, b uint64) uint64 {
	if a < b {
		return a
	}
	return b
}

package raft

import (
	"context"
	"fmt"
	"log"
	"math/rand"
	"net"
	"time"

	"github.com/allankerr/packraft/protos"
)

const (
	tickDuration = time.Millisecond * 1

	maxElectionTimeoutTicks = 1000
	minElectionTimeoutTicks = 500
)

type LogEntry struct {
	Index   uint64
	Command []byte
}

type tickMessage struct {
}

type proposeMessage struct {
}

type raftState struct {
	serverID uint32
	peers    []uint32
	term     uint64
	votedFor *uint32
}

func (rs *raftState) commit() {
	// TODO: Commit persistent state
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

	electionTimeoutTicks int
	votes                int
}

func newFollowerState() *machineState {
	state := new(machineState)
	state.stateType = Follower
	state.requestHandlers = []RequestHandler{
		tickHandler{},
	}
	state.electionTimeoutTicks = randInt(minElectionTimeoutTicks, maxElectionTimeoutTicks)
	return state
}

func newCandidateState(rs *raftState) *machineState {
	state := new(machineState)
	state.stateType = Candidate
	state.requestHandlers = []RequestHandler{
		tickHandler{},
	}
	state.electionTimeoutTicks = randInt(minElectionTimeoutTicks, maxElectionTimeoutTicks)

	rs.votedFor = &rs.serverID
	rs.term += 1
	return state
}

func newLeaderState(rs *raftState) *machineState {
	state := new(machineState)
	state.stateType = Leader
	state.requestHandlers = []RequestHandler{
		leaderTickHandler{},
	}

	rs.votedFor = nil
	rs.term += 1
	return state
}

func newPassiveState(rs *raftState) *machineState {
	state := new(machineState)
	state.stateType = Passive
	state.requestHandlers = []RequestHandler{
		passiveTickHandler{},
	}
	return state
}

type Raft struct {
	passive bool

	client    *Client
	server    *Server
	raftState *raftState

	ticker     *time.Ticker
	requestCh  chan IncomingRequestEnvelope
	responseCh chan IncomingResponseEnvelope
}

type RaftOpts = func(*Raft)

func WithPassive(passive bool) func(*Raft) {
	return func(rf *Raft) {
		rf.passive = passive
	}
}

func New(id uint32, opts ...RaftOpts) *Raft {
	requestCh := make(chan IncomingRequestEnvelope)
	responseCh := make(chan IncomingResponseEnvelope)

	rf := &Raft{
		client:     NewClient(responseCh),
		server:     NewServer(requestCh),
		raftState:  &raftState{serverID: id},
		ticker:     time.NewTicker(tickDuration),
		requestCh:  requestCh,
		responseCh: responseCh,
	}
	for _, opt := range opts {
		opt(rf)
	}
	return rf
}

func (rf *Raft) Start(lis net.Listener) error {
	var cur *machineState
	if rf.passive {
		cur = newPassiveState(rf.raftState)
	} else {
		cur = newFollowerState()
	}
	go rf.stateMachineLoop(cur)
	return rf.server.Serve(lis)
}

func (r *Raft) Propose(ctx context.Context, cmd []byte) (uint64, error) {
	return 0, nil
}

func (rf *Raft) stateMachineLoop(cur *machineState) {
	tick := IncomingRequestEnvelope{
		ctx:        context.Background(),
		msg:        &tickMessage{},
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
		panic(fmt.Errorf("missing request handler for message %v", req.msg))
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
		panic(fmt.Errorf("missing response handler for message %v", res.msg))
	}
	rf.raftState.commit()
	rf.client.Commit()
	return next
}

type tickHandler struct {
}

func (h tickHandler) Execute(cur *machineState, rs *raftState, c *Client, ctx context.Context, msg interface{}) (*machineState, interface{}, error) {
	if _, ok := msg.(*tickMessage); !ok {
		return nil, nil, nil
	}
	cur.electionTimeoutTicks--
	if cur.electionTimeoutTicks != 0 {
		return cur, nil, nil
	}
	if len(rs.peers) == 0 {
		log.Println("become leader")
		return newLeaderState(rs), nil, nil
	}
	for _, peerID := range rs.peers {
		req := protos.RequestVoteRequest{
			Term:        rs.term,
			CandidateId: rs.serverID,
		}
		res := protos.RequestVoteResponse{}
		c.RequestVote(peerID, &req, &res)
	}
	return newCandidateState(rs), nil, nil
}

type leaderTickHandler struct {
}

func (h leaderTickHandler) Execute(cur *machineState, rs *raftState, c *Client, ctx context.Context, msg interface{}) (*machineState, interface{}, error) {
	return cur, nil, nil
}

func randInt(min int, max int) int {
	return rand.Intn(max-min+1) + min
}

type passiveTickHandler struct {
}

func (h passiveTickHandler) Execute(cur *machineState, rs *raftState, c *Client, ctx context.Context, msg interface{}) (*machineState, interface{}, error) {
	// no-op so that nodes joining a cluster won't attempt to become leader
	return cur, nil, nil
}

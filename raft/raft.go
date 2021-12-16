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
)

type MessageHandler interface {
	Execute(cur *machineState, rs *raftState, c *Client, msg interface{}) *machineState
}

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
	messageHandlers  []MessageHandler

	electionTimeoutTicks int
	votes                int
}

func newFollowerState() *machineState {
	state := new(machineState)
	state.messageHandlers = []MessageHandler{
		tickHandler{},
	}
	state.electionTimeoutTicks = randInt(minElectionTimeoutTicks, maxElectionTimeoutTicks)
	return state
}

func newCandidateState(rs *raftState) *machineState {
	state := new(machineState)
	state.messageHandlers = []MessageHandler{
		tickHandler{},
	}
	state.electionTimeoutTicks = randInt(minElectionTimeoutTicks, maxElectionTimeoutTicks)

	rs.votedFor = &rs.serverID
	rs.term += 1
	return state
}

func newLeaderState(rs *raftState) *machineState {
	state := new(machineState)
	state.messageHandlers = []MessageHandler{
		leaderTickHandler{},
	}

	rs.votedFor = nil
	rs.term += 1
	return state
}

type Raft struct {
	client    *Client
	server    *Server
	raftState *raftState

	ticker     *time.Ticker
	requestCh  chan IncomingRequestEnvelope
	responseCh chan IncomingResponseEnvelope
}

func New(id uint32) *Raft {
	requestCh := make(chan IncomingRequestEnvelope)
	responseCh := make(chan IncomingResponseEnvelope)

	return &Raft{
		client:     NewClient(responseCh),
		server:     NewServer(requestCh),
		raftState:  &raftState{serverID: id},
		ticker:     time.NewTicker(tickDuration),
		requestCh:  requestCh,
		responseCh: responseCh,
	}
}

func (r *Raft) Start(lis net.Listener) error {
	cur := newFollowerState()
	go r.stateMachineLoop(cur)
	return r.server.Serve(lis)
}

func (r *Raft) Propose(ctx context.Context, cmd []byte) (uint64, error) {
	return 0, nil
}

func (rf *Raft) stateMachineLoop(cur *machineState) {
	for {
		var next *machineState
		select {
		case req := <-rf.requestCh:
			next = rf.executeRequest(cur, req)
		case res := <-rf.responseCh:
			next = rf.executeResponse(cur, res)
		case <-rf.ticker.C:
			next = rf.executeMessage(cur, &tickMessage{})
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

func (rf *Raft) executeMessage(cur *machineState, msg interface{}) *machineState {

	var next *machineState
	for _, h := range cur.messageHandlers {
		next = h.Execute(cur, rf.raftState, rf.client, msg)
		if next != nil {
			break
		}
	}
	if next == nil {
		panic(fmt.Errorf("missing message handler for message %v", msg))
	}
	rf.raftState.commit()
	rf.client.Commit()
	return next
}

type tickHandler struct {
}

func (h tickHandler) Execute(cur *machineState, rs *raftState, c *Client, msg interface{}) *machineState {

	if _, ok := msg.(*tickMessage); !ok {
		return nil
	}
	cur.electionTimeoutTicks--
	if cur.electionTimeoutTicks != 0 {
		return cur
	}
	if len(rs.peers) == 0 {
		log.Println("become leader")
		return newLeaderState(rs)
	}
	for _, peerID := range rs.peers {
		req := protos.RequestVoteRequest{
			Term:        rs.term,
			CandidateId: rs.serverID,
		}
		res := protos.RequestVoteResponse{}
		c.RequestVote(peerID, &req, &res)
	}
	return newCandidateState(rs)
}

type leaderTickHandler struct {
}

func (h leaderTickHandler) Execute(cur *machineState, rs *raftState, c *Client, msg interface{}) *machineState {
	return cur
}

func randInt(min int, max int) int {
	return rand.Intn(max-min+1) + min
}

package raft

import (
	"context"
	"fmt"
	"net"

	"github.com/allankerr/packraft/protos"
	"google.golang.org/grpc"
)

type IncomingRequestEnvelope struct {
	ctx        context.Context
	msg        interface{}
	responseCh chan OutgoingResponseEnvelope
}

type OutgoingResponseEnvelope struct {
	msg interface{}
	err error
}

type Server struct {
	requestCh chan IncomingRequestEnvelope
	protos.UnsafePackRaftServiceServer
}

func NewServer(requestCh chan IncomingRequestEnvelope) *Server {
	return &Server{requestCh: requestCh}
}

func (s *Server) Serve(lis net.Listener) error {
	server := grpc.NewServer()
	if err := server.Serve(lis); err != nil {
		return err
	}
	return nil
}

func (s *Server) RequestVote(ctx context.Context, req *protos.RequestVoteRequest) (*protos.RequestVoteResponse, error) {
	inc := IncomingRequestEnvelope{
		ctx:        ctx,
		msg:        req,
		responseCh: make(chan OutgoingResponseEnvelope),
	}
	s.requestCh <- inc
	out := <-inc.responseCh
	if out.err != nil {
		return nil, out.err
	}
	if res, ok := out.msg.(*protos.RequestVoteResponse); !ok {
		return res, nil
	}
	panic(fmt.Sprintf("method RequestVote returned an unexpected response %v", out.msg))
}

func (s *Server) AppendEntries(ctx context.Context, req *protos.AppendEntriesRequest) (*protos.AppendEntriesResponse, error) {
	inc := IncomingRequestEnvelope{
		ctx:        ctx,
		msg:        req,
		responseCh: make(chan OutgoingResponseEnvelope),
	}
	s.requestCh <- inc
	out := <-inc.responseCh
	if out.err != nil {
		return nil, out.err
	}
	if res, ok := out.msg.(*protos.AppendEntriesResponse); !ok {
		return res, nil
	}
	panic(fmt.Sprintf("method AppendEntries returned an unexpected response %v", out.msg))
}

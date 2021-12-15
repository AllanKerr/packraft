package raft

import (
	"context"

	"github.com/allankerr/packraft/protos"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

var (
	ErrServerNotFound = status.Error(codes.InvalidArgument, "invalid server ID")
)

type request struct {
	serverID uint32
	ctx      context.Context
	method   string
	req      interface{}
	res      interface{}
}

type IncomingResponseEnvelope struct {
	serverID uint32
	msg      interface{}
	err      error
}

type Client struct {
	responseCh chan IncomingResponseEnvelope
	conns      map[uint32]*grpc.ClientConn
	queue      []request
}

func NewClient(responseCh chan IncomingResponseEnvelope) *Client {
	return &Client{
		responseCh: responseCh,
		conns:      make(map[uint32]*grpc.ClientConn),
	}
}

func (c *Client) Connect(serverID uint32, addr string) error {
	conn, err := grpc.Dial(addr, grpc.WithInsecure())
	if err != nil {
		return err
	}
	c.conns[serverID] = conn
	return nil
}

func (c *Client) Disconnect(serverID uint32) error {
	conn, ok := c.conns[serverID]
	if !ok {
		return ErrServerNotFound
	}
	delete(c.conns, serverID)
	return conn.Close()
}

func (c *Client) RequestVote(serverID uint32, ctx context.Context, in *protos.RequestVoteRequest, out *protos.RequestVoteResponse) {
	c.queue = append(c.queue, request{
		serverID: serverID,
		ctx:      ctx,
		method:   "/PackRaftService/RequestVote",
		req:      in,
		res:      out,
	})
}

func (c *Client) AppendEntries(serverID uint32, ctx context.Context, in *protos.AppendEntriesRequest, out *protos.AppendEntriesResponse) {
	c.queue = append(c.queue, request{
		serverID: serverID,
		ctx:      ctx,
		method:   "/PackRaftService/AppendEntries",
		req:      in,
		res:      out,
	})
}

func (c *Client) Commit() {
	for _, req := range c.queue {
		go c.invoke(req.serverID, req.ctx, req.method, req.req, req.res)
	}
}

func (c *Client) invoke(serverID uint32, ctx context.Context, method string, req interface{}, res interface{}) {
	var err error
	conn, ok := c.conns[serverID]
	if ok {
		err = conn.Invoke(ctx, method, req, res)
	} else {
		err = ErrServerNotFound
	}
	c.responseCh <- IncomingResponseEnvelope{
		serverID: serverID,
		msg:      res,
		err:      err,
	}
}

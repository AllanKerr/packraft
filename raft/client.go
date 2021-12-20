package raft

import (
	"context"
	"time"

	"github.com/allankerr/packraft/protos"
	"google.golang.org/grpc"
	"google.golang.org/grpc/backoff"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

var (
	ErrServerNotFound = status.Error(codes.InvalidArgument, "invalid server ID")
)

const (
	defaultTimeout = time.Second

	connectMultiplier = 1.6
	connectJitter     = 0.2
	baseConnectDelay  = 50 * time.Millisecond
	maxConnectDelay   = 1000 * time.Millisecond
	minConnectTimeout = 20 * time.Second
)

type request struct {
	serverID uint32
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
	timeout    time.Duration
}

func NewClient(responseCh chan IncomingResponseEnvelope) *Client {
	return &Client{
		responseCh: responseCh,
		conns:      make(map[uint32]*grpc.ClientConn),
		timeout:    defaultTimeout,
	}
}

func (c *Client) Connect(serverID uint32, addr string) error {
	config := grpc.ConnectParams{
		MinConnectTimeout: minConnectTimeout,
		Backoff: backoff.Config{
			Multiplier: connectMultiplier,
			Jitter:     connectJitter,
			BaseDelay:  baseConnectDelay,
			MaxDelay:   maxConnectDelay,
		},
	}
	conn, err := grpc.Dial(addr, grpc.WithInsecure(), grpc.WithConnectParams(config))
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

func (c *Client) RequestVote(serverID uint32, in *protos.RequestVoteRequest, out *protos.RequestVoteResponse) {
	c.queue = append(c.queue, request{
		serverID: serverID,
		method:   "/PackRaftService/RequestVote",
		req:      in,
		res:      out,
	})
}

func (c *Client) AppendEntries(serverID uint32, in *protos.AppendEntriesRequest, out *protos.AppendEntriesResponse) {
	c.queue = append(c.queue, request{
		serverID: serverID,
		method:   "/PackRaftService/AppendEntries",
		req:      in,
		res:      out,
	})
}

func (c *Client) Commit() {
	for _, req := range c.queue {
		go c.invoke(req.serverID, req.method, req.req, req.res)
	}
	c.queue = nil
}

func (c *Client) invoke(serverID uint32, method string, req interface{}, res interface{}) {
	var err error
	conn, ok := c.conns[serverID]
	if ok {
		ctx, cancel := context.WithTimeout(context.Background(), c.timeout)
		err = conn.Invoke(ctx, method, req, res)
		cancel()
	} else {
		err = ErrServerNotFound
	}
	c.responseCh <- IncomingResponseEnvelope{
		serverID: serverID,
		msg:      res,
		err:      err,
	}
}

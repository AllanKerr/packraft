package main

import (
	"flag"
	"log"
	"net"
	"os"

	"github.com/allankerr/packraft/raft"
	"strconv"
)

func uint32Flag(name string, usage string, serverID **uint32) {
	flag.Func(name, usage, func(raw string) error {
		tmp, err := strconv.ParseUint(raw, 10, 32)
		if err != nil {
			return err
		}
		val := uint32(tmp)
		*serverID = &val
		return nil
	})
}

func listenerFlag(name string, usage string, lis *net.Listener) {
	flag.Func(name, usage, func(raw string) error {
		tmp, err := net.Listen("tcp", raw)
		if err != nil {
			return err
		}
		*lis = tmp
		return nil
	})
}

func main() {

	var (
		serverID *uint32
		lis      net.Listener
	)
	uint32Flag("id", "Server ID used to identify the Raft node", &serverID)
	listenerFlag("address", "Address for the Raft node to listen at", &lis)
	flag.Parse()

	if serverID == nil {
		flag.Usage()
		os.Exit(1)
	}
	if lis == nil {
		flag.Usage()
		os.Exit(1)
	}
	rf := raft.New(*serverID)
	if err := rf.Start(lis); err != nil {
		log.Fatalf("error starting Raft node: %v", err)
	}
}

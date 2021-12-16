package main

import (
	"flag"
	"fmt"
	"log"
	"net"
	"os"

	"strconv"

	"github.com/allankerr/packraft/raft"
)

func uint32Flag(name string, usage string) **uint32 {
	var out *uint32
	flag.Func(name, usage, func(raw string) error {
		tmp, err := strconv.ParseUint(raw, 10, 32)
		if err != nil {
			return err
		}
		val := uint32(tmp)
		out = &val
		return nil
	})
	return &out
}

func listenerFlag(name string, usage string) *net.Listener {
	var lis net.Listener
	flag.Func(name, usage, func(raw string) error {
		tmp, err := net.Listen("tcp", raw)
		if err != nil {
			return err
		}
		lis = tmp
		return nil
	})
	return &lis
}

func main() {
	serverID := uint32Flag("id", "Server ID used to identify the Raft node")
	lis := listenerFlag("address", "Address for the Raft node to listen at")
	passive := flag.Bool("passive", false, "Starts the node in passive mode waiting to join an existing cluster")
	flag.Parse()

	if *serverID == nil {
		fmt.Fprintf(flag.CommandLine.Output(), "Missing required flag: id\n")
		flag.Usage()
		os.Exit(1)
	}
	if *lis == nil {
		fmt.Fprintf(flag.CommandLine.Output(), "Missing required flag: address\n")
		flag.Usage()
		os.Exit(1)
	}
	rf := raft.New(**serverID, raft.WithPassive(*passive))
	if err := rf.Start(*lis); err != nil {
		log.Fatalf("error starting Raft node: %v", err)
	}
}

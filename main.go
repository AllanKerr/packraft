package main

import (
	"context"
	"fmt"
	"log"
	"net"
	"os"
	"time"

	"github.com/allankerr/packraft/raft"
	flag "github.com/spf13/pflag"
)

func main() {
	serverID := flag.Uint("me", 0, "ID for this Raft node")
	serverIDs := flag.UintSlice("id", nil, "IDs for the Raft nodes in the cluster")
	addresses := flag.StringSlice("address", nil, "Addresses for the Raft nodes in the cluster")
	flag.Parse()

	if !flag.Lookup("id").Changed {
		fmt.Println("Missing required flag: id")
		flag.Usage()
		os.Exit(1)

	}
	if len(*serverIDs) != len(*addresses) {
		fmt.Println("There must be the same number of IDs and addresses")
		flag.Usage()
		os.Exit(1)
	}
	cluster := make(map[uint32]string)
	for i := range *serverIDs {
		id := uint32((*serverIDs)[i])
		if _, ok := cluster[id]; ok {
			fmt.Printf("Multiple addresses found for %v\n", id)
			flag.Usage()
			os.Exit(1)
		}
		cluster[id] = (*addresses)[i]
	}
	id := uint32(*serverID)
	addr, ok := cluster[id]
	if !ok {
		fmt.Printf("Missing address for %v\n", id)
		flag.Usage()
		os.Exit(1)

	}
	lis, err := net.Listen("tcp", addr)
	if err != nil {
		fmt.Printf("Error listening on address %v", err)
		flag.Usage()
		os.Exit(1)
	}
	rf := raft.New(id, cluster)
	go proposeLoop(rf)
	go applyLoop(rf)
	if err := rf.Start(lis); err != nil {
		log.Fatalf("error starting Raft node: %v", err)
	}
}

func proposeLoop(rf *raft.Raft) {
	for {
		time.Sleep(time.Second * 3)
		rf.Propose(context.Background(), []byte{1, 1, 1, 1})
	}
}

func applyLoop(rf *raft.Raft) {
	for {
		entry := <-rf.C
		log.Printf("Applied entry %v %v", entry.Index, entry.Command)
	}
}

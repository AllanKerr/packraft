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
	serverID := flag.Uint32("id", 0, "Server ID used to identify the Raft node")
	address := flag.String("address", "", "Address for the Raft node to listen at")
	passive := flag.Bool("passive", false, "Starts the node in passive mode waiting to join an existing cluster")
	flag.Parse()

	if !flag.Lookup("id").Changed {
		fmt.Println("Missing required flag: id")
		flag.Usage()
		os.Exit(1)

	}
	if !flag.Lookup("address").Changed {
		fmt.Println("Missing required flag: address")
		flag.Usage()
		os.Exit(1)
	}
	lis, err := net.Listen("tcp", *address)
	if err != nil {
		fmt.Printf("Error listening on address: %v", err)
		flag.Usage()
		os.Exit(1)
	}
	rf := raft.New(*serverID, raft.WithPassive(*passive))
	go func() {

		time.Sleep(time.Second * 3)
		rf.Propose(context.Background(), []byte{1, 1, 1, 1})
	}()
	if err := rf.Start(lis); err != nil {
		log.Fatalf("error starting Raft node: %v", err)
	}
}

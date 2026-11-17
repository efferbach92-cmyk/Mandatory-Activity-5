package main

import (
	"context"
	"fmt"
	"math/rand"
	"os"
	"time"

	proto "github.com/efferbach92-cmyk/Mandatory-Activity-5/grpc"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func main() {
	if len(os.Args) != 2 {
		fmt.Println("Usage: go run ./client <bidder>")
		return
	}
	bidder := os.Args[1]
	fmt.Println("client started:", bidder)
	fmt.Println("Auction has started. Minimum bet: 100kr.")
	addresses := []string{
		"localhost:5050",
		"localhost:5051",
		"localhost:5052",
	}
	requestID := int32(os.Getpid() * 10)
	//laver en ny klient forbindelse hver gang så den vælger en tilfældig server.
	var auctionClient proto.AuctionServiceClient
	var conn *grpc.ClientConn
	var err error
	for i := 0; i < 5; i++ {
		serverAddress := addresses[rand.Intn(len(addresses))]
		fmt.Println("Connecting to server at:", serverAddress)
		conn, err = grpc.NewClient(serverAddress, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			fmt.Println("Failed to connect:", err)
			i-- // make sure that the client tries again without counting the attempt completed bid.
			time.Sleep(time.Second) // waits on sec
			continue // starts next try and picks a server again
		}
		auctionClient = proto.NewAuctionServiceClient(conn)
		result, err := auctionClient.Result(context.Background(), &proto.Empty{})
		if err != nil {
			fmt.Println("Failed to get result:", err)
			conn.Close()
			i--
			time.Sleep(time.Second)
			continue
		}
		if result.AuctionOver {
			break
		}
		bid := result.WinningBid + 10
		if result.WinningBid == 0 {
			bid = 100
		}
		request := &proto.BidRequest{
			Bidder:    bidder,
			Bid:       bid,
			RequestID: requestID,
		}
		response, err := auctionClient.Bid(context.Background(), request)
		if err != nil {
			fmt.Println("Bid failed:", err)
			conn.Close() // connection closes
			i-- 
			time.Sleep(time.Second) // client waits 1 sec
			continue // tries again against a server 
		}
		fmt.Printf("%s bids %d kr. - Result: %s\n", bidder, bid, response.Status)
		requestID++
		// har sat en timer på imellem bud så de kan byde imod hinanden ellers sker det i en sekvens.
		if i < 4 {
			time.Sleep(time.Duration(rand.Intn(10)+1) * time.Second)
		}
	}
	for {
		result, err := auctionClient.Result(context.Background(), &proto.Empty{})
		
		if err != nil {
			fmt.Println("Server failed. Trying another server...")
        	conn.Close()
			
		
		serverAddress := addresses[rand.Intn(len(addresses))]
		conn, err = grpc.NewClient(serverAddress, grpc.WithTransportCredentials(insecure.NewCredentials()))

		if err == nil {
			auctionClient = proto.NewAuctionServiceClient(conn)
		}



		time.Sleep(time.Second)
		continue
	}

	if result.AuctionOver {
		fmt.Printf("Auction over! Winner: %s with %dkr, \n", result.Winner, result.WinningBid)
			return
	}
		time.Sleep(time.Second)
	}
}

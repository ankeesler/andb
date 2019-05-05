package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"os"
	"time"

	"github.com/ankeesler/andb/server"
	"google.golang.org/grpc"
)

func main() {
	address := flag.String("address", "127.0.0.1:8080", "Address at which the server is running")
	help := flag.Bool("help", false, "Print out the help text")

	flag.Parse()

	if *help {
		flag.Usage()
		os.Exit(1)
	}

	if flag.NArg() == 0 {
		flag.Usage()
		os.Exit(1)
	}

	var cmd func(server.ANDBClient) error
	switch flag.Arg(0) {
	case "get":
		cmd = get
	case "set":
		cmd = set
	case "delete":
		cmd = delete
	case "sync":
		cmd = sync
	}

	if cmd == nil {
		fmt.Printf("expected get or set, but got %s\n", flag.Arg(0))
		os.Exit(1)
	}

	conn, err := grpc.Dial(*address, grpc.WithInsecure())
	if err != nil {
		fmt.Printf("cannot dial server at address %s: %s\n", *address, err.Error())
		os.Exit(1)
	}
	defer conn.Close()

	client := server.NewANDBClient(conn)
	if err := cmd(client); err != nil {
		fmt.Printf("error: %s\n", err.Error())
		os.Exit(1)
	}
}

func get(client server.ANDBClient) error {
	if flag.NArg() != 2 {
		fmt.Println("usage: get <key>")
		os.Exit(1)
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*3)
	defer cancel()

	req := server.GetRequest{Key: flag.Arg(1)}

	rsp, err := client.Get(ctx, &req)
	if err != nil {
		return err
	}

	if rsp.Status != "ok" {
		return errors.New(rsp.Status)
	}

	fmt.Println(rsp.Value)

	return nil
}

func set(client server.ANDBClient) error {
	if flag.NArg() != 3 {
		fmt.Println("usage: set <key> <value>")
		os.Exit(1)
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*3)
	defer cancel()

	req := server.SetRequest{Key: flag.Arg(1), Value: flag.Arg(2)}

	rsp, err := client.Set(ctx, &req)
	if err != nil {
		return err
	}

	if rsp.Status != "ok" {
		return errors.New(rsp.Status)
	}

	return nil
}

func delete(client server.ANDBClient) error {
	if flag.NArg() != 2 {
		fmt.Println("usage: delete <key>")
		os.Exit(1)
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*3)
	defer cancel()

	req := server.DeleteRequest{Key: flag.Arg(1)}

	rsp, err := client.Delete(ctx, &req)
	if err != nil {
		return err
	}

	if rsp.Status != "ok" {
		return errors.New(rsp.Status)
	}

	return nil
}

func sync(client server.ANDBClient) error {
	if flag.NArg() != 1 {
		fmt.Println("usage: sync")
		os.Exit(1)
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*3)
	defer cancel()

	req := server.SyncRequest{}

	rsp, err := client.Sync(ctx, &req)
	if err != nil {
		return err
	}

	if rsp.Status != "ok" {
		return errors.New(rsp.Status)
	}

	return nil
}

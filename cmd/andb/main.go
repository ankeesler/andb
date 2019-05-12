package main

import (
	"flag"
	"fmt"
	"os"

	"github.com/ankeesler/andb"
)

func main() {
	address := flag.String("address", ":8080", "Address at which the server is running")
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

	var cmd func(andb.Client) error
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
		fmt.Printf("unknown comman: %s\n", flag.Arg(0))
		os.Exit(1)
	}

	client, err := andb.Dial(*address)
	if err != nil {
		fmt.Printf("cannot dial server at address %s: %s\n", *address, err.Error())
		os.Exit(1)
	}
	defer client.Close()

	if err := cmd(client); err != nil {
		fmt.Printf("error: %s\n", err.Error())
		os.Exit(1)
	}
}

func get(client andb.Client) error {
	if flag.NArg() != 2 {
		fmt.Println("usage: get <key>")
		os.Exit(1)
	}

	if value, err := client.Get(flag.Arg(1)); err != nil {
		return err
	} else {
		fmt.Println(value)
	}

	return nil
}

func set(client andb.Client) error {
	if flag.NArg() != 3 {
		fmt.Println("usage: set <key> <value>")
		os.Exit(1)
	}

	if err := client.Set(flag.Arg(1), flag.Arg(2)); err != nil {
		return err
	}

	return nil
}

func delete(client andb.Client) error {
	if flag.NArg() != 2 {
		fmt.Println("usage: delete <key>")
		os.Exit(1)
	}

	if err := client.Delete(flag.Arg(1)); err != nil {
		return err
	}

	return nil
}

func sync(client andb.Client) error {
	if flag.NArg() != 1 {
		fmt.Println("usage: sync")
		os.Exit(1)
	}

	if err := client.Sync(); err != nil {
		return err
	}

	return nil
}

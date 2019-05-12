package main

import (
	"flag"
	"fmt"
	"os"

	"github.com/ankeesler/andb"
	"github.com/tedsuo/ifrit"
)

func main() {
	logfile := flag.String("logfile", "", "The log file that this server will use")
	loglevel := flag.String("loglevel", "info", "The log level that this server will use")
	storedir := flag.String("storedir", "/tmp", "The store file that this server will use")
	port := flag.String("port", "8080", "The port that this server will listen on")
	help := flag.Bool("help", false, "Print out the help text")

	flag.Parse()

	if *help {
		flag.Usage()
		os.Exit(1)
	}

	config := andb.Config{
		LogFile:  *logfile,
		LogLevel: *loglevel,

		StoreDir: *storedir,

		Address: fmt.Sprintf(":%s", *port),
	}
	server := andb.New(&config)
	p := ifrit.Invoke(server)
	fmt.Fprintf(os.Stderr, "andb exited with error: %s", <-p.Wait())
}

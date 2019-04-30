package main

import (
	"flag"
	"log"
	"net"
	"os"
	"path/filepath"

	"google.golang.org/grpc"

	"github.com/ankeesler/andb/filestore"
	"github.com/ankeesler/andb/filestore/datastore"
	"github.com/ankeesler/andb/filestore/metastore"
	"github.com/ankeesler/andb/memstore"
	"github.com/ankeesler/andb/server"
)

func main() {
	logfile := flag.String("logfile", "", "The log file that this server will use")
	storedir := flag.String("storedir", "/tmp", "The store file that this server will use")
	help := flag.Bool("help", false, "Print out the help text")

	flag.Parse()

	if *help {
		flag.Usage()
		os.Exit(1)
	}

	log.SetFlags(log.Ldate | log.Ltime | log.Lmicroseconds | log.Lshortfile)
	if *logfile != "" {
		file, err := os.Create(*logfile)
		if err != nil {
			log.Fatal(err)
		}
		defer file.Close()

		log.SetOutput(file)
	}
	log.Print("start")

	log.Printf("storedir: %s", *storedir)

	datafile := openFile(filepath.Join(*storedir, "andbdata.bin"))
	defer datafile.Close()
	log.Printf("datafile: %s", datafile.Name())

	metafile := openFile(filepath.Join(*storedir, "andbmeta.bin"))
	defer metafile.Close()
	log.Printf("metafile: %s", metafile.Name())

	cache := memstore.New()
	ds := datastore.New(datafile)
	ms := metastore.New(metafile)
	fs := filestore.New(cache, ds, ms)

	grpcServer := grpc.NewServer()
	s := server.New(fs)
	server.RegisterANDBServer(grpcServer, s)

	listener, err := net.Listen("tcp", ":8080")
	if err != nil {
		log.Fatal(err)
	}

	log.Fatal(grpcServer.Serve(listener))
}

func openFile(filename string) *os.File {
	file, err := os.OpenFile(filename, os.O_RDWR|os.O_CREATE, 0600)
	if err != nil {
		log.Fatal(err)
	}
	return file
}

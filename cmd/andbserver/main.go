package main

import (
	"flag"
	"fmt"
	"net"
	"os"
	"path/filepath"

	"google.golang.org/grpc"

	"github.com/ankeesler/andb/filestore"
	"github.com/ankeesler/andb/filestore/datastore"
	"github.com/ankeesler/andb/filestore/metastore"
	"github.com/ankeesler/andb/memstore"
	"github.com/ankeesler/andb/server"
	log "github.com/sirupsen/logrus"
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

	setLogfile(*logfile)
	setLoglevel(*loglevel)
	log.SetFormatter(&log.TextFormatter{
		ForceColors:     true,
		FullTimestamp:   true,
		TimestampFormat: "15:04:05.000000000",
	})

	log.Info("start")

	log.Debugf("storedir: %s", *storedir)

	datafile := openFile(filepath.Join(*storedir, "andbdata.bin"))
	defer datafile.Close()
	log.Debugf("datafile: %s", datafile.Name())

	metafile := openFile(filepath.Join(*storedir, "andbmeta.bin"))
	defer metafile.Close()
	log.Debugf("metafile: %s", metafile.Name())

	cache := memstore.New()
	ds := datastore.New(datafile)
	ms := metastore.New(metafile)
	fs := filestore.New(cache, ds, ms)

	grpcServer := grpc.NewServer()
	s := server.New(fs)
	server.RegisterANDBServer(grpcServer, s)

	log.Debugf("listening on port %s", *port)
	listener, err := net.Listen("tcp", fmt.Sprintf(":%s", *port))
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

func setLogfile(logfile string) {
	if logfile != "" {
		file, err := os.Create(logfile)
		if err != nil {
			log.Fatal(err)
		}
		defer file.Close()

		log.SetOutput(file)
	}
}

func setLoglevel(loglevel string) {
	level, err := log.ParseLevel(loglevel)
	if err != nil {
		log.Fatal(err)
	}
	log.SetLevel(level)
}

package andb

import (
	"os"
	"path/filepath"

	"github.com/ankeesler/andb/filestore"
	"github.com/ankeesler/andb/filestore/datastore"
	"github.com/ankeesler/andb/filestore/metastore"
	"github.com/ankeesler/andb/memstore"
	api "github.com/ankeesler/andb/server"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
	"github.com/tedsuo/ifrit"
	"github.com/tedsuo/ifrit/grpc_server"
)

type Config struct {
	LogFile  string
	LogLevel string

	StoreDir string

	Address string
}

type server struct {
	config *Config
}

func New(config *Config) ifrit.Runner {
	return &server{config: config}
}

func (s *server) Run(signals <-chan os.Signal, ready chan<- struct{}) error {
	err, cleanup := setLogFile(s.config.LogFile)
	if err != nil {
		return errors.Wrap(err, "set log file")
	}
	defer cleanup()

	if err := setLogLevel(s.config.LogLevel); err != nil {
		return errors.Wrap(err, "set log level")
	}

	log.SetFormatter(&log.TextFormatter{
		ForceColors:     true,
		FullTimestamp:   true,
		TimestampFormat: "15:04:05.000000000",
	})

	log.Info("start")

	log.Debugf("store dir: %s", s.config.StoreDir)

	dataFile, err := openFile(filepath.Join(s.config.StoreDir, "andbdata.bin"))
	if err != nil {
		return errors.Wrap(err, "open data file")
	}
	defer dataFile.Close()
	log.Debugf("data file: %s", dataFile.Name())

	metaFile, err := openFile(filepath.Join(s.config.StoreDir, "andbmeta.bin"))
	if err != nil {
		return errors.Wrap(err, "open data file")
	}
	defer metaFile.Close()
	log.Debugf("meta file: %s", metaFile.Name())

	cache := memstore.New()
	ds := datastore.New(dataFile)
	ms := metastore.New(metaFile)
	fs := filestore.New(cache, ds, ms)

	log.Debugf("listening on address %s", s.config.Address)

	return grpc_server.NewGRPCServer(
		s.config.Address,
		nil, // tlsConfig, TODO: make this secure
		api.New(fs),
		api.RegisterANDBServer,
	).Run(signals, ready)
}

func openFile(filename string) (*os.File, error) {
	file, err := os.OpenFile(filename, os.O_RDWR|os.O_CREATE, 0600)
	if err != nil {
		return nil, errors.Wrap(err, "open file")
	}

	return file, nil
}

func setLogFile(logFile string) (error, func() error) {
	if logFile != "" {
		file, err := os.Create(logFile)
		if err != nil {
			return errors.Wrap(err, "create log file"), func() error { return nil }
		}

		log.SetOutput(file)

		return nil, file.Close
	} else {
		return nil, func() error { return nil }
	}
}

func setLogLevel(logLevel string) error {
	level, err := log.ParseLevel(logLevel)
	if err != nil {
		return errors.Wrap(err, "parse log level")
	}

	log.SetLevel(level)

	return nil
}

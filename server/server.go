package server

import (
	"context"

	log "github.com/sirupsen/logrus"
)

//go:generate protoc --go_out=plugins=grpc:. server.proto

type Store interface {
	Get(string) (string, error)
	Set(string, string) error
	Delete(string) error
	Sync() error
}

type server struct {
	store Store
}

func New(store Store) ANDBServer {
	return &server{
		store: store,
	}
}

func (s *server) Get(ctx context.Context, r *GetRequest) (*GetResponse, error) {
	log.Debugf("get %s", r.Key)

	value, err := s.store.Get(r.Key)

	var status string
	if err != nil {
		status = err.Error()
	} else {
		status = "ok"
	}

	return &GetResponse{Value: value, Status: status}, nil
}

func (s *server) Set(ctx context.Context, r *SetRequest) (*SetResponse, error) {
	log.Debugf("set %s => %s", r.Key, r.Value)

	var status string
	if err := s.store.Set(r.Key, r.Value); err != nil {
		status = err.Error()
	} else {
		status = "ok"
	}

	return &SetResponse{Status: status}, nil
}

func (s *server) Delete(ctx context.Context, r *DeleteRequest) (*DeleteResponse, error) {
	log.Debugf("delete %s", r.Key)

	var status string
	if err := s.store.Delete(r.Key); err != nil {
		status = err.Error()
	} else {
		status = "ok"
	}

	return &DeleteResponse{Status: status}, nil
}

func (s *server) Sync(ctx context.Context, r *SyncRequest) (*SyncResponse, error) {
	log.Debugf("sync")

	var status string
	if err := s.store.Sync(); err != nil {
		status = err.Error()
	} else {
		status = "ok"
	}

	return &SyncResponse{Status: status}, nil
}

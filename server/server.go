package server

import (
	"context"
	"log"
)

//go:generate protoc --go_out=plugins=grpc:. server.proto

type Store interface {
	Get(string) (string, error)
	Set(string, string) error
	Delete(string) error
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
	log.Printf("get %s", r.Key)

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
	log.Printf("set %s => %s", r.Key, r.Value)

	var status string
	if err := s.store.Set(r.Key, r.Value); err != nil {
		status = err.Error()
	} else {
		status = "ok"
	}
	return &SetResponse{Status: status}, nil
}

func (s *server) Delete(ctx context.Context, r *DeleteRequest) (*DeleteResponse, error) {
	log.Printf("delete %s", r.Key)

	var status string
	if err := s.store.Delete(r.Key); err != nil {
		status = err.Error()
	} else {
		status = "ok"
	}
	return &DeleteResponse{Status: status}, nil
}

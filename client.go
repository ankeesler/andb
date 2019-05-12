package andb

import (
	"context"
	"time"

	api "github.com/ankeesler/andb/server"
	"github.com/pkg/errors"
	"google.golang.org/grpc"
)

type Client interface {
	Get(key string) (string, error)
	Set(key, value string) error
	Delete(key string) error
	Sync() error

	Close() error
}

type client struct {
	client api.ANDBClient
	conn   *grpc.ClientConn
}

func Dial(address string) (Client, error) {
	// TODO: make this more secure!
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*3)
	defer cancel()

	conn, err := grpc.DialContext(ctx, address, grpc.WithInsecure())
	if err != nil {
		return nil, errors.Wrap(err, "grpc dial")
	}

	return &client{
		client: api.NewANDBClient(conn),
		conn:   conn,
	}, nil
}

func (c *client) Get(key string) (string, error) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*3)
	defer cancel()

	req := api.GetRequest{Key: key}

	rsp, err := c.client.Get(ctx, &req)
	if err != nil {
		return "", errors.Wrap(err, "get")
	}

	if rsp.Status != "ok" {
		return "", errors.Wrap(errors.New(rsp.Status), "get")
	}

	return rsp.Value, nil
}

func (c *client) Set(key, value string) error {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*3)
	defer cancel()

	req := api.SetRequest{Key: key, Value: value}

	rsp, err := c.client.Set(ctx, &req)
	if err != nil {
		return errors.Wrap(err, "set")
	}

	if rsp.Status != "ok" {
		return errors.Wrap(errors.New(rsp.Status), "set")
	}

	return nil
}

func (c *client) Delete(key string) error {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*3)
	defer cancel()

	req := api.DeleteRequest{Key: key}

	rsp, err := c.client.Delete(ctx, &req)
	if err != nil {
		return errors.Wrap(err, "delete")
	}

	if rsp.Status != "ok" {
		return errors.Wrap(errors.New(rsp.Status), "delete")
	}

	return nil
}

func (c *client) Sync() error {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*3)
	defer cancel()

	req := api.SyncRequest{}

	rsp, err := c.client.Sync(ctx, &req)
	if err != nil {
		return errors.Wrap(err, "sync")
	}

	if rsp.Status != "ok" {
		return errors.Wrap(errors.New(rsp.Status), "sync")
	}

	return nil
}

func (c *client) Close() error {
	return c.conn.Close()
}

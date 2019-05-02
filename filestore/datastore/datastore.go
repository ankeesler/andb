package datastore

import (
	"io"
	"log"
	"os"
	"sync"

	"github.com/pkg/errors"
)

type Datastore struct {
	file  *os.File
	mutex *sync.Mutex // TODO: this should be a file lock
}

func New(file *os.File) *Datastore {
	return &Datastore{
		file:  file,
		mutex: &sync.Mutex{},
	}
}

func (d *Datastore) WriteKeyValue(
	key, value string,
	onSuccess func(key, value string, keyOffset, valueOffset uint32),
	onError func(error),
) {
	d.mutex.Lock()
	defer d.mutex.Unlock()

	log.Printf("begin write key/value data: %s => %s", key, value)
	defer log.Printf("end write key/value data: %s => %s", key, value)

	keyOffset, err := d.file.Seek(0, 1)
	if err != nil {
		onError(errors.Wrap(err, "seek (key)"))
		return
	}

	_, err = d.file.Write([]byte(key))
	if err != nil {
		onError(errors.Wrap(err, "write (key)"))
		return
	}

	valueOffset, err := d.file.Seek(0, 1)
	if err != nil {
		onError(errors.Wrap(err, "seek (value)"))
		return
	}

	_, err = d.file.Write([]byte(value))
	if err != nil {
		onError(errors.Wrap(err, "write (value)"))
		return
	}

	if err := d.file.Sync(); err != nil {
		onError(errors.Wrap(err, "sync"))
		return
	}

	onSuccess(key, value, uint32(keyOffset), uint32(valueOffset))
}

func (d *Datastore) ReadData(offset, length uint32) (string, error) {
	d.mutex.Lock()
	defer d.mutex.Unlock()

	if _, err := d.file.Seek(int64(offset), 0); err != nil {
		return "", errors.Wrap(err, "seek")
	}

	data := make([]byte, length)
	if _, err := io.ReadFull(d.file, data); err != nil {
		return "", errors.Wrap(err, "read full")
	}

	return string(data), nil
}

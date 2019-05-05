package filestore

import (
	"fmt"
	"hash/crc32"
	"sync"
	"time"

	"github.com/ankeesler/andb/filestore/datastore"
	"github.com/ankeesler/andb/filestore/metastore"
	"github.com/ankeesler/andb/memstore"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
)

type Filestore struct {
	cache memstore.Memstore
	data  *datastore.Datastore
	meta  *metastore.Metastore
	mutex *sync.Mutex
	// TODO: this shouldn't be global
	// esp when there is locking below

	workC chan *work
}

func New(
	cache memstore.Memstore,
	data *datastore.Datastore,
	meta *metastore.Metastore,
) *Filestore {
	f := &Filestore{
		cache: cache,
		data:  data,
		meta:  meta,
		mutex: &sync.Mutex{},
	}

	f.workC = make(chan *work)
	newWorker(f.workC).start()

	return f
}

func (f *Filestore) Get(key string) (string, error) {
	f.mutex.Lock()
	defer f.mutex.Unlock()

	log.Debugf("begin get %s", key)
	defer log.Debugf("end get %s", key)

	if value, err := f.cache.Get(key); err == nil {
		return value, nil
	}

	if err := f.loadStore(); err != nil {
		return "", errors.Wrap(err, "load store")
	}

	if value, err := f.cache.Get(key); err != nil {
		return "", errors.New("not found")
	} else {
		return value, nil
	}
}

func (f *Filestore) Set(key, value string) error {
	f.mutex.Lock()
	defer f.mutex.Unlock()

	log.Debugf("begin set %s => %s", key, value)
	defer log.Debugf("end set %s => %s", key, value)

	f.workC <- &work{
		description: fmt.Sprintf("set %s => %s", key, value),
		action: func() error {
			var err error
			f.data.WriteKeyValue(
				key,
				value,
				func(key, value string, keyOffset, valueOffset uint32) {
					err = f.meta.Write(key, value, keyOffset, valueOffset)
				},
				func(err0 error) {
					err = errors.Wrap(err0, "write key/value data")
				},
			)
			return err
		},
	}

	if err := f.cache.Set(key, value); err != nil {
		return errors.Wrap(err, "cache set")
	}

	return nil
}

func (f *Filestore) Delete(key string) error {
	f.mutex.Lock()
	defer f.mutex.Unlock()

	log.Debugf("begin delete %s", key)
	defer log.Debugf("end delete %s", key)

	f.workC <- &work{
		description: fmt.Sprintf("delete %s", key),
		action: func() error {
			if err := f.meta.DeleteBlock(key); err != nil {
				return errors.Wrap(err, "delete meta block")
			}

			return nil
		},
	}

	if err := f.cache.Delete(key); err != nil {
		return errors.Wrap(err, "cache delete")
	}

	return nil
}

func (f *Filestore) Sync() error {
	for {
		if len(f.workC) == 0 {
			break
		} else {
			time.Sleep(time.Second * 3)
		}
	}
	return nil
}

func (f *Filestore) loadStore() error {
	log.Tracef("loading store")
	if err := f.meta.ForEachBlock(func(b metastore.Block) error {
		expectedBlockCRC32, err := b.CalculateCRC32()
		if err != nil {
			return errors.Wrap(err, "calculate block crc32")
		}

		if b.CRC32 != expectedBlockCRC32 {
			return fmt.Errorf(
				"incorrect block crc32 (0x%08X != 0x%08X)",
				b.CRC32,
				expectedBlockCRC32,
			)
		}

		key, err := f.data.ReadData(b.KeyOffset, b.KeyLength)
		if err != nil {
			return errors.Wrap(err, "read key data")
		}

		actualKeyCRC32 := crc32.ChecksumIEEE([]byte(key))
		if actualKeyCRC32 != b.KeyCRC32 {
			return fmt.Errorf(
				"incorrect key crc32 (0x%08X != 0x%08X)",
				actualKeyCRC32,
				b.KeyCRC32,
			)
		}

		value, err := f.data.ReadData(b.ValueOffset, b.ValueLength)
		if err != nil {
			return errors.Wrap(err, "read value data")
		}

		actualValueCRC32 := crc32.ChecksumIEEE([]byte(value))
		if actualValueCRC32 != b.ValueCRC32 {
			return fmt.Errorf(
				"incorrect value crc32 (0x%08X != 0x%08X)",
				actualValueCRC32,
				b.ValueCRC32,
			)
		}

		log.Tracef("loading %s => %s", key, value)
		if err := f.cache.Set(key, value); err != nil {
			return errors.Wrap(err, "cache set")
		}

		return nil
	}); err != nil {
		return errors.Wrap(err, "for each block")
	}

	return nil
}

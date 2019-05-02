package metastore

import (
	"encoding/binary"
	"hash/crc32"
	"io"
	"os"
	"sync"

	"github.com/pkg/errors"
)

type Metastore struct {
	file  *os.File
	mutex *sync.Mutex // TODO: this should be a file lock
}

func New(file *os.File) *Metastore {
	return &Metastore{
		file:  file,
		mutex: &sync.Mutex{},
	}
}

func (m *Metastore) Write(
	key, value string,
	keyOffset, valueOffset uint32,
) error {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	b := Block{
		Version: BlockVersion,

		KeyOffset: keyOffset,
		KeyLength: uint32(len(key)),
		KeyCRC32:  crc32.ChecksumIEEE([]byte(key)),

		ValueOffset: valueOffset,
		ValueLength: uint32(len(value)),
		ValueCRC32:  crc32.ChecksumIEEE([]byte(value)),
	}
	blockCRC32, err := b.CalculateCRC32()
	if err != nil {
		return errors.Wrap(err, "calculate crc32")
	}
	b.CRC32 = blockCRC32

	if err := binary.Write(m.file, blockByteOrder, &b); err != nil {
		return errors.Wrap(err, "write block")
	}

	return nil
}

func (m *Metastore) ForEachBlock(blockHandler func(b Block) error) error {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	_, err := m.file.Seek(0, 0)
	if err != nil {
		return errors.Wrap(err, "seek to beginning")
	}

	b := Block{}
	for {
		if err := binary.Read(m.file, blockByteOrder, &b); err != nil {
			if err == io.EOF {
				break
			} else {
				return errors.Wrap(err, "read block")
			}
		}

		if blockHandler != nil {
			if err := blockHandler(b); err != nil {
				return errors.Wrap(err, "block handler")
			}
		}
	}

	return nil
}

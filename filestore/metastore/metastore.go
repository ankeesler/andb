package metastore

import (
	"encoding/binary"
	"hash/crc32"
	"io"
	"io/ioutil"
	"log"
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

	_, err := m.file.Seek(0, 2)
	if err != nil {
		return errors.Wrap(err, "seek to end")
	}

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

	cursorFile, err := os.Open(m.file.Name())
	if err != nil {
		return errors.Wrap(err, "open cursor file")
	}
	defer cursorFile.Close()

	b := Block{}
	i := 0
	for {
		if err := binary.Read(cursorFile, blockByteOrder, &b); err != nil {
			if err == io.EOF {
				break
			} else {
				return errors.Wrap(err, "read block")
			}
		}

		log.Printf("handle block %d (%x)", i, b.KeyCRC32)
		i++

		if blockHandler != nil {
			if err := blockHandler(b); err != nil {
				return errors.Wrap(err, "block handler")
			}
		}
	}

	return nil
}

func (m *Metastore) DeleteBlock(key string) error {
	newFile, err := ioutil.TempFile("", "andbmetastore")
	if err != nil {
		return errors.Wrap(err, "temp file")
	}
	defer newFile.Close()

	if err := m.ForEachBlock(
		func(b Block) error {
			if b.KeyCRC32 != crc32.ChecksumIEEE([]byte(key)) {
				if err := binary.Write(newFile, blockByteOrder, &b); err != nil {
					return errors.Wrap(err, "write block")
				}
			} else {
				log.Printf("dropping block %x", b.KeyCRC32)
			}
			return nil
		},
	); err != nil {
		return errors.Wrap(err, "for each block")
	}

	if err := os.Rename(newFile.Name(), m.file.Name()); err != nil {
		return errors.Wrap(err, "rename")
	}

	if err := m.file.Close(); err != nil {
		return errors.Wrap(err, "close")
	}

	m.file, err = os.Open(m.file.Name())
	if err != nil {
		return errors.Wrap(err, "(re)open file")
	}

	return nil
}

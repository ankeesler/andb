package metastore

import (
	"bytes"
	"encoding/binary"
	"hash/crc32"

	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
)

type Block struct {
	Version                              uint32
	KeyOffset, KeyLength, KeyCRC32       uint32
	ValueOffset, ValueLength, ValueCRC32 uint32
	CRC32                                uint32
}

const BlockVersion = 0x01020304

var blockByteOrder = binary.BigEndian

func (b Block) CalculateCRC32() (uint32, error) {
	// TODO: this needs to be faster!
	b.CRC32 = 0

	log.Tracef("calc crc32 of %+v", b)

	buf := bytes.NewBuffer([]byte{})
	if err := binary.Write(buf, blockByteOrder, &b); err != nil {
		return 0, errors.Wrap(err, "write block")
	}

	return crc32.ChecksumIEEE(buf.Bytes()), nil
}

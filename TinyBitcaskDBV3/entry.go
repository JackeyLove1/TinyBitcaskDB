package TinyBitcaskDBV3

import (
	"encoding/binary"
	"errors"
	"hash/crc32"
)

const entryHeaderSize = 16

// Type
const (
	String uint16 = iota
	List
	Hash
	Set
	ZSet
)

// Mark
const (
	Put uint16 = iota
	Delete
)

var (
	ErrInvalidEntry = errors.New("invalid entry")
)

type meta struct {
	KeySize   uint32 // 8 -> 12
	ValueSize uint32 // 12 -> 16
	Key       []byte // 16 -> 16 + ks
	Value     []byte // 16 + ks -> 16 + ks + vs
}
type Entry struct {
	Crc  uint32 // 0 -> 4
	Type uint16 // 4 -> 6
	Mark uint16 // 6 -> 8
	Meta meta
}

func NewEntry(key, value []byte, mark, dType uint16) *Entry {
	return &Entry{
		Type: dType,
		Mark: mark,
		Meta: meta{
			KeySize:   uint32(len(key)),
			ValueSize: uint32(len(value)),
			Key:       key,
			Value:     value,
		},
	}
}

func (e *Entry) Size() int64 {
	return int64(entryHeaderSize + e.Meta.KeySize + e.Meta.ValueSize)
}

func (e *Entry) Encode() ([]byte, error) {
	if e == nil || e.Meta.Key == nil {
		return nil, ErrInvalidEntry
	}

	buf := make([]byte, e.Size())

	binary.BigEndian.PutUint16(buf[4:6], e.Type)
	binary.BigEndian.PutUint16(buf[6:8], e.Mark)
	binary.BigEndian.PutUint32(buf[8:12], e.Meta.KeySize)
	binary.BigEndian.PutUint32(buf[12:16], e.Meta.ValueSize)

	ks, vs := e.Meta.KeySize, e.Meta.ValueSize
	copy(buf[entryHeaderSize:entryHeaderSize+ks], e.Meta.Key)
	copy(buf[entryHeaderSize+ks:entryHeaderSize+ks+vs], e.Meta.Value)

	crc := crc32.ChecksumIEEE(buf[4:])
	binary.BigEndian.PutUint32(buf[0:4], crc)

	return buf, nil
}

func Decode(buf []byte) (*Entry, error) {
	crc := binary.BigEndian.Uint32(buf[0:4])
	ty := binary.BigEndian.Uint16(buf[4:6])
	mk := binary.BigEndian.Uint16(buf[6:8])
	ks := binary.BigEndian.Uint32(buf[8:12])
	vs := binary.BigEndian.Uint32(buf[12:16])

	return &Entry{
		Crc:  crc,
		Type: ty,
		Mark: mk,
		Meta: meta{
			KeySize:   ks,
			ValueSize: vs,
		},
	}, nil
}

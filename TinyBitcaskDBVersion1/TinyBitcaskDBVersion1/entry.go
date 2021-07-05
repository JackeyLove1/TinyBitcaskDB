package TinyBitcaskDB

import "encoding/binary"

const entryHeaderSize = 10

const (
	PUT uint16 = iota
	DEL
)

// TODO: CRC
type Entry struct {
	Key       []byte
	Value     []byte
	KeySize   uint32 // 4
	ValueSize uint32 // 4
	Mark      uint16 // 2
}

func NewEntry(key, value []byte, mark uint16) *Entry{
	return &Entry{
		Key: key,
		Value: value,
		KeySize: uint32(len(key)),
		ValueSize: uint32(len(value)),
		Mark: mark,
	}
}

func (e *Entry) GetSize() int64{
	return int64(entryHeaderSize + e.KeySize + e.ValueSize)
}

func (e *Entry) Encode() ([]byte, error){
	buf := make([]byte, e.GetSize())
	binary.LittleEndian.PutUint32(buf[0:4], e.KeySize)
	binary.LittleEndian.PutUint32(buf[4:8], e.ValueSize)
	binary.LittleEndian.PutUint16(buf[8:10], e.Mark)
	copy(buf[entryHeaderSize:entryHeaderSize+e.KeySize], e.Key)
	copy(buf[entryHeaderSize+e.KeySize:], e.Value)
	return buf, nil
}

func Decode(buf []byte) (*Entry, error){
	ks := binary.LittleEndian.Uint32(buf[0:4])
	vs := binary.LittleEndian.Uint32(buf[4:8])
	mark := binary.LittleEndian.Uint16(buf[8:10])
	return &Entry{KeySize: ks, ValueSize: vs, Mark: mark}, nil
}
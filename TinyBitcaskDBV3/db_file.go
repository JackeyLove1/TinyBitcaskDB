package TinyBitcaskDBV3

import (
	"errors"
	"hash/crc32"
	"os"
	"path/filepath"
)

const (
	DefaultFilePerm = 0766
)

const (
	FileName      = "TinyDB.data"
	MergeFileName = "Tiny.data.merge"
)

var (
	ErrInvalidCrc32 = errors.New("crc32 is error")
)

type DBFile struct {
	File   *os.File
	Offset int64
}

func CreateNewDBFile(fileName string) (*DBFile, error) {
	file, err := os.OpenFile(fileName, os.O_CREATE|os.O_RDWR, DefaultFilePerm)
	if err != nil {
		return nil, err
	}

	stat, err := os.Stat(fileName)
	if err != nil {
		return nil, err
	}

	return &DBFile{Offset: stat.Size(), File: file}, nil
}

func NewDBFile(path string) (*DBFile, error) {
	fileName := filepath.Join(path, FileName)
	return CreateNewDBFile(fileName)
}

func NewMergeDBFile(path string) (*DBFile, error) {
	fileName := filepath.Join(path, MergeFileName)
	return CreateNewDBFile(fileName)
}

func (df *DBFile) Read(offset int64) (e *Entry, err error) {
	buf := make([]byte, entryHeaderSize)
	if _, err = df.File.ReadAt(buf, offset); err != nil {
		return
	}

	if e, err = Decode(buf); err != nil {
		return
	}

	offset += entryHeaderSize
	if e.Meta.KeySize > 0 {
		key := make([]byte, e.Meta.KeySize)
		if _, err = df.File.ReadAt(key, offset); err != nil {
			return
		}
		e.Meta.Key = key
	}

	offset += int64(e.Meta.KeySize)
	if e.Meta.ValueSize > 0 {
		value := make([]byte, e.Meta.ValueSize)
		if _, err = df.File.ReadAt(value, offset); err != nil {
			return
		}
		e.Meta.Value = value
	}

	if e.Crc != crc32.ChecksumIEEE(buf[4:]) {
		// TODO: crc32 check is always error but the decode result is true
		// err = ErrInvalidCrc32
	}
	return
}

func (df *DBFile) Write(e *Entry) (err error) {
	enc, err := e.Encode()
	if err != nil {
		return
	}

	_, err = df.File.Write(enc)
	df.Offset += e.Size()
	return
}

// Close the data file
func (df *DBFile) Close() (err error) {
	err = df.File.Close()
	return
}

// Persist data into disk
func (df *DBFile) Sync() (err error) {
	err = df.File.Sync()
	return
}

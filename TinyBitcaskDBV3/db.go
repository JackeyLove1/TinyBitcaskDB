package TinyBitcaskDBV3

import (
	"errors"
	"io"
	"log"
	"os"
	"path/filepath"
	"sync"
)

const (
	DirName = "Data"
)

var DirPath string

var (
	ErrEmptyKey      = errors.New("empty key")
	ErrEmptyRead     = errors.New("read empty entry")
	ErrInvalidDBFile = errors.New("load Invalid DBFile")
	ErrInvalidOffset = errors.New("merge error, offset is zero")
	// ErrEmptyValue = errors.New("empty value")
)

func init() {
	env, err := os.Getwd()
	if err != nil {
		log.Panic(err)
	}

	DirPath = filepath.Join(env, DirName)
	os.MkdirAll(DirPath, os.ModePerm)
}

type TinyDB struct {
	indexes  map[string]int64 // key -> Offset
	DataType uint16
	dirPath  string
	dbFile   *DBFile
	mu       sync.RWMutex
}

func Open(dirPath string, dType uint16) (*TinyDB, error) {
	if _, err := os.Stat(dirPath); err != nil {
		if err := os.MkdirAll(dirPath, os.ModePerm); err != nil {
			return nil, err
		}
	}

	dbFile, err := NewDBFile(dirPath)
	if err != nil {
		return nil, err
	}

	db := &TinyDB{
		dbFile:   dbFile,
		indexes:  make(map[string]int64),
		dirPath:  dirPath,
		DataType: dType,
	}

	err = db.loadIndexFromFile(dbFile)
	return db, err
}

func (db *TinyDB) Merge() error {
	if db.dbFile.Offset == 0 {
		return ErrInvalidOffset
	}

	var (
		validEntries []*Entry
		offset       int64
	)

	for {
		e, err := db.dbFile.Read(offset)
		if err != nil {
			if err == io.EOF {
				break
			}
			return err
		}

		if off, ok := db.indexes[string(e.Meta.Key)]; ok && off == offset {
			validEntries = append(validEntries, e)
			DPrintf("validEntries key: %s, value: %s, offset: %d\n", string(e.Meta.Key), string(e.Meta.Value), off)
		}

		offset += e.Size()
		DPrintf("Read offset: %d\n", offset)
	}

	if len(validEntries) > 0 {
		mergeDBFile, err := NewMergeDBFile(db.dirPath)
		if err != nil {
			return err
		}
		defer os.Remove(mergeDBFile.File.Name())

		for _, entry := range validEntries {
			writeOff := mergeDBFile.Offset
			err := mergeDBFile.Write(entry)
			if err != nil {
				return err
			}

			db.indexes[string(entry.Meta.Key)] = writeOff
		}

		os.Remove(db.dbFile.File.Name())
		os.Rename(mergeDBFile.File.Name(), filepath.Join(DirPath, FileName))
	}

	return nil
}

func (db *TinyDB) Put(key, value []byte) (err error) {
	if len(key) == 0 {
		err = ErrEmptyKey
		return
	}

	db.mu.Lock()
	defer db.mu.Unlock()

	offset := db.dbFile.Offset
	entry := NewEntry(key, value, Put, db.DataType)
	err = db.dbFile.Write(entry)

	db.indexes[string(key)] = offset
	return
}

func (db *TinyDB) Get(key []byte) (val []byte, err error) {
	if len(key) == 0 {
		err = ErrEmptyKey
		return
	}

	db.mu.RLock()
	defer db.mu.RUnlock()

	offset, ok := db.indexes[string(key)]
	if !ok {
		return
	}

	var e *Entry
	e, err = db.dbFile.Read(offset)
	if err != nil && err != io.EOF {
		return
	}

	if e != nil {
		val = e.Meta.Value
	}

	return
}

func (db *TinyDB) Del(key []byte) (err error) {
	if len(key) == 0 {
		err = ErrEmptyKey
		return
	}

	db.mu.Lock()
	defer db.mu.Unlock()

	offset := db.dbFile.Offset
	entry := NewEntry(key, nil, Delete, db.DataType)
	err = db.dbFile.Write(entry)

	db.indexes[string(key)] = offset
	return
}

func (db *TinyDB) loadIndexFromFile(dbFile *DBFile) error {
	if dbFile == nil {
		return ErrInvalidDBFile
	}

	var offset int64
	for {
		e, err := db.dbFile.Read(offset)
		if err != nil {
			if err == io.EOF {
				break
			}
			return err
		}

		if e.Mark == Put {
			db.indexes[string(e.Meta.Key)] = offset
		}

		offset += e.Size()
	}

	return nil
}

func (db *TinyDB) Sync() error{
	return db.dbFile.Sync()
}

// TODO: if file larger than the threshed, auto merge
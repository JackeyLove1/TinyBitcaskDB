package TinyBitcaskDB

import (
	"io"
	"os"
	"sync"
)

type DB struct {
	indexes map[string]int64 // memory info, key -> offset
	dbFile  *DBFile          // data file
	dirPath string           //
	mu      sync.RWMutex     // RW Lock
}

func Open(dirPath string) (*DB, error) {
	if _, err := os.Stat(dirPath); os.IsNotExist(err) {
		if err = os.MkdirAll(dirPath, DefaultPerm); err != nil {
			return nil, err
		}
	}

	// load data
	dbFile, err := NewDBFile(dirPath)
	if err != nil{
		return nil, err
	}

	db := &DB{
		dbFile: dbFile,
		indexes: make(map[string]int64),
		dirPath: dirPath,
	}

	// load indexes
	db.loadIndexesFromFile(dbFile)
	return db, nil
}

// Merge files
func (db *DB) Merge() error{
	if db.dbFile.Offset == 0{
		return nil
	}

	var (
		validEntries []*Entry
		offset int64
	)

	// read entries in origin file
	for{
		e, err := db.dbFile.Read(offset)
		if err != nil{
			if err == io.EOF{
				break
			}
			return err
		}
		// compare the data in memory(latest data) with active file
		if off, ok := db.indexes[string(e.Key)]; ok && off == offset{
			validEntries = append(validEntries, e)
		}
		offset += e.GetSize()
	}

	if len(validEntries) > 0{
		// create new temp file
		mergeDBFile, err := NewMergeDBFile(db.dirPath)
		if err != nil{
			return err
		}
		defer os.Remove(mergeDBFile.File.Name())

		// rewrite valid entry
		for _, entry := range validEntries{
			writeOff := mergeDBFile.Offset
			err := mergeDBFile.Write(entry)
			if err != nil{
				return err
			}

			// update index
			db.indexes[string(entry.Key)] = writeOff
		}

		// delete old file
		os.Remove(db.dbFile.File.Name())
		os.Rename(mergeDBFile.File.Name(), db.dirPath + string(os.PathSeparator) + FileName)

		db.dbFile = mergeDBFile
		DPrintf("Merge is finished !\n")
	}
	return nil
}


// Put
func (db *DB) Put(key []byte, value []byte) (err error){
	if len(key) == 0{
		return
	}

	db.mu.Lock()
	defer db.mu.Unlock()

	offset := db.dbFile.Offset
	// make entry
	entry := NewEntry(key, value, PUT)
	// append into the file
	err = db.dbFile.Write(entry)

	// update to memory
	db.indexes[string(key)] = offset
	return
}

// Get
func (db *DB) Get(key []byte) (val []byte, err error){
	if len(key) == 0{
		return
	}

	db.mu.Lock()
	defer db.mu.Unlock()

	// get the index info from memory
	offset, ok := db.indexes[string(key)]
	// key not existed
	if !ok{
		DPrintf("get %s failed !", string(key))
		return
	}

	// read value from disk
	var e *Entry
	e, err = db.dbFile.Read(offset)
	if err != nil && err != io.EOF{
		DPrintf("Read Error: %s\n", err.Error())
		return
	}
	if err != nil{
		DPrintf("Read Error: %s\n", err.Error())
	}
	DPrintf("Find key %s, value is %s", string(key), string(val))
	val = e.Value

	return
}

// Delete: logical delete
func (db *DB)Del(key []byte) (err error){
	if len(key) == 0{
		return
	}

	db.mu.Lock()
	defer db.mu.Unlock()
	// get info from index
	_, ok := db.indexes[string(key)]
	if !ok{
		DPrintf("Del: key %s is not found \n", string(key))
		return
	}

	// make entry
	e := NewEntry(key, nil, DEL)
	err = db.dbFile.Write(e)
	if err != nil{
		return
	}

	// delete the key from the memory
	delete(db.indexes, string(key))
	DPrintf("Del: key %s is delete in logically\n", string(key))
	return
}

func (db *DB) loadIndexesFromFile(dbFile *DBFile){
	if dbFile == nil{
		return
	}

	var offset int64
	for{
		e, err := db.dbFile.Read(offset)
		if err != nil{
			if err == io.EOF{
				break
			}
			return
		}
		if e.Mark == PUT{
			// set index
			db.indexes[string(e.Key)] = offset
		}
		offset += e.GetSize()
	}
	return
}
package TinyBitcaskDB

import "os"

const (
	FileName    = "db.data"
	MergeFileName   = "db.data.merge"
	DefaultPerm = 0644
)

type DBFile struct {
	File   *os.File
	Offset int64
}

func newInternal(fileName string) (*DBFile, error) {
	file, err := os.OpenFile(fileName, os.O_CREATE|os.O_RDWR, DefaultPerm)
	if err != nil {
		return nil, err
	}

	stat, err := os.Stat(fileName)
	if err != nil {
		return nil, err
	}

	return &DBFile{
		File:   file,
		Offset: stat.Size(),
	}, nil
}

func NewDBFile(path string) (*DBFile, error) {
	fileName := path + string(os.PathSeparator) + FileName
	return newInternal(fileName)
}

func NewMergeDBFile(path string) (*DBFile, error){
	fileName := path + string(os.PathSeparator) + MergeFileName
	return newInternal(fileName)
}

// Read an Entry from Offset
func (db *DBFile) Read(offset int64) (e *Entry, err error) {
	buf := make([]byte, entryHeaderSize)
	if _, err = db.File.ReadAt(buf, offset); err != nil{
		return
	}
	if e, err = Decode(buf); err != nil{
		return
	}

	offset += entryHeaderSize
	if e.KeySize > 0 {
		key := make([]byte, e.KeySize)
		if _, err = db.File.ReadAt(key, offset); err != nil{
			return
		}
		e.Key = key
	}

	offset += int64(e.KeySize)
	if e.ValueSize > 0{
		value := make([]byte, e.ValueSize)
		if _, err = db.File.ReadAt(value, offset); err != nil{
			return
		}
		e.Value = value
	}
	return
}

// Write the entry into db.file
func (db *DBFile) Write(e *Entry) (err error){
	enc, err := e.Encode()
	if err != nil{
		return
	}
	_, err = db.File.Write(enc)
	db.Offset += e.GetSize()
	return
}
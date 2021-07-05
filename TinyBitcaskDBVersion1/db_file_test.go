package TinyBitcaskDB

import (
	"os"
	"testing"
)

var (
	path string
	err error
)

func init(){
	path, err = os.Getwd()
	if err != nil{
		panic(err)
	}
	os.MkdirAll(path, DefaultPerm)
}

func TestNewDBFile(t *testing.T) {
	_, err = NewDBFile(path)
	if err != nil{
		t.Error("new db file Error ", err)
	}

	_, err = NewMergeDBFile(path)
	if err != nil{
		t.Error("new merge file Error ", err)
	}
}
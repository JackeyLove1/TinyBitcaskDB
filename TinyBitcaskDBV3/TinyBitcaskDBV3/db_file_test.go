package TinyBitcaskDBV3

import (
	"log"
	"os"
	"path/filepath"
	"testing"
)

const (
	TmpFileName = "TmpFile"
	DefaultMark = Put
	DefaultType = String
)

var (
	TmpPath string
)

func init() {
	os.RemoveAll("./TmpFile")
	TmpEnv, err := os.Getwd()
	if err != nil {
		log.Panic(err)
	}

	TmpPath = filepath.Join(TmpEnv, TmpFileName)
	os.MkdirAll(TmpPath, os.ModePerm)

}

func TestNewDBFile(t *testing.T) {
	_, err := NewDBFile(TmpPath)
	if err != nil {
		t.Error(err)
	}
}

func TestDBFile_Sync(t *testing.T) {
	df, err := NewDBFile(TmpPath)
	if err != nil {
		t.Error(err)
	}
	err = df.Sync()
	if err != nil {
		t.Error(err)
	}
}

func TestDBFile_Close(t *testing.T) {
	df, err := NewDBFile(TmpPath)
	if err != nil {
		t.Error(err)
	}
	df.Close()
	if err != nil {
		t.Error(err)
	}
}

func TestDBFile_WriteAndRead(t *testing.T) {
	df, err := NewDBFile(TmpPath)
	if err != nil {
		t.Error(err)
	}
	k1, v1 := []byte("test_key_1"), []byte("test_value_1")
	e1 := NewEntry(k1, v1, DefaultMark, DefaultType)

	k2, v2 := []byte("test_key_2"), []byte("test_value_2")
	e2 := NewEntry(k2, v2, DefaultMark, DefaultType)

	err = df.Write(e1)
	log.Println("e1.size: ", e1.Size())   //38
	log.Println("df.offset: ", df.Offset) //38

	err = df.Write(e2)
	log.Println("e1.size: ", e1.Size())   //38
	log.Println("df.offset: ", df.Offset) //76

	if err != nil {
		t.Error("Write Data Error: ", err)
	}

	e3, err := df.Read(0)
	log.Printf("e3 key: %s, value: %s\n", string(e3.Meta.Key), string(e3.Meta.Value))
	if err != nil {
		t.Error("e3 Read Data Error: ", err)
	}

	e4, err := df.Read(38)
	log.Printf("e4 key: %s, value: %s\n", string(e4.Meta.Key), string(e4.Meta.Value))
	if err != nil {
		t.Error("e4 Read Data Error: ", err)
	}

	if string(e3.Meta.Key) != string(e1.Meta.Key) || string(e3.Meta.Value) != string(e1.Meta.Value) {
		t.Error("Read e1 Value Different !")
	}

	if string(e2.Meta.Key) != string(e4.Meta.Key) || string(e2.Meta.Value) != string(e2.Meta.Value) {
		t.Error("Read e2 Value Different !")
	}

	defer func() {
		err = df.Close()
		os.RemoveAll("./TmpFile")
	}()
}

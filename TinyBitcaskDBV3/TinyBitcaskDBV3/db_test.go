package TinyBitcaskDBV3

import (
	"log"
	"math/rand"
	"strconv"
	"testing"
	"time"
)

const (
	DefaultDataType = String
	TestNum         = 100
	TestMod         = 5
)

func TestInit(t *testing.T) {
	log.Println("DirPath: ", DirPath)
}

func TestOpen(t *testing.T) {
	db, err := Open(DirPath, DefaultDataType)
	if err != nil {
		t.Error(err)
	}
	t.Log(db)
}

func TestTinyDB_Put(t *testing.T) {
	db, err := Open(DirPath, DefaultDataType)
	if err != nil {
		t.Error(err)
	}

	rand.Seed(time.Now().UnixNano())
	keyPrefix := "test_key_"
	valPrefix := "test_value_"
	for i := 0; i < TestNum; i++ {
		key := []byte(keyPrefix + strconv.Itoa(i%TestMod))
		value := []byte(valPrefix + strconv.FormatInt(rand.Int63(), 10))
		err = db.Put(key, value)
	}

	if err != nil {
		t.Log("read err: ", err)
	}
}

func TestTinyDB_Get(t *testing.T) {
	db, err := Open(DirPath, DefaultDataType)
	if err != nil {
		t.Error(err)
	}

	getVal := func(key []byte) {
		val, err := db.Get(key)
		if err != nil {
			t.Error("Get err: ", err)
		} else {
			t.Logf("key = %s, val = %s\n", string(key), string(val))
		}
	}

	getVal([]byte("test_key_0"))
	getVal([]byte("test_key_1"))
	getVal([]byte("test_key_2"))
	getVal([]byte("test_key_3"))
	getVal([]byte("test_key_4"))
	getVal([]byte("test_key_5"))
}

func TestTinyDB_Del(t *testing.T) {
	db, err := Open(DirPath, DefaultDataType)
	if err != nil {
		t.Error(err)
	}

	key := []byte("test_key_101")
	err = db.Del(key)

	if err != nil {
		t.Error("del err: ", err)
	}
}

func TestTinyDB_Merge(t *testing.T) {
	db, err := Open(DirPath, DefaultDataType)
	if err != nil {
		t.Error(err)
	}

	err = db.Merge()
	if err != nil {
		t.Error("merge err: ", err)
	}
}

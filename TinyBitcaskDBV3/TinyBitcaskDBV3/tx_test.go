package TinyBitcaskDBV3

import "testing"

func TestTransaction(t *testing.T) {
	db, err := Open(DirPath, DefaultDataType)
	if err != nil {
		t.Error(err)
	}

	err = db.Put([]byte("key1"), []byte("value1"))
	err = db.Put([]byte("key2"), []byte("value2"))
	if err != nil {
		t.Error("Put Origin data Error", err)
	}

	tx := db.Begin()
	err = tx.Put([]byte("key3"), []byte("value3"))
	err = tx.Put([]byte("key2"), []byte("value4"))
	if err != nil {
		t.Error("tx Put Error ", err)
	}

	if v, err := tx.Get([]byte("key1")); err != nil || string(v) != "value1" {
		t.Fatalf("Expected key1=value1, got key1=%s instead", string(v))
	}

	if v, err := tx.Get([]byte("key2")); err != nil || string(v) != "value2" {
		t.Fatalf("Expected key2=value2, got key2=%s instead", string(v))
	}

	if v, err := tx.Get([]byte("key3")); err != nil || len(v) > 0 {
		t.Fatalf("Expected key3 should not get, got key3=%s instead", string(v))
	}

	// db should: key1=value1, key2=value2
	err = tx.Put([]byte("key2"), []byte("value5"))

	err = tx.Commit()
	if err != nil {
		t.Error("Commit Error: ", err)
	}
	// db should: key1=value1, key2=value5, key3=value3
	if v, err := db.Get([]byte("key1")); err != nil || string(v) != "value1" {
		t.Fatalf("Expected key1=value1, got key1=%s instead, err: %s", string(v), err.Error())
	}

	if v, err := db.Get([]byte("key2")); err != nil || string(v) != "value5" {
		t.Fatalf("Expected key2=value5, got key2=%s instead, err: %s", string(v), err.Error())
	}

	if v, err := db.Get([]byte("key3")); err != nil || string(v) != "value3" {
		t.Fatalf("Expected key3=value3, got key3=%s instead, err: %s", string(v), err.Error())
	}

}

func TestTx_RollBack(t *testing.T) {
	db, err := Open(DirPath, DefaultDataType)
	if err != nil {
		t.Error(err)
	}

	err = db.Put([]byte("key1"), []byte("value1"))
	err = db.Put([]byte("key2"), []byte("value2"))
	if err != nil {
		t.Error("Put Origin data Error", err)
	}

	tx := db.Begin()
	err = tx.Put([]byte("key3"), []byte("value3"))
	err = tx.Put([]byte("key2"), []byte("value4"))
	if err != nil {
		t.Error("tx Put Error ", err)
	}

	if v, err := tx.Get([]byte("key1")); err != nil || string(v) != "value1" {
		t.Fatalf("Expected key1=value1, got key1=%s instead", string(v))
	}

	if v, err := tx.Get([]byte("key2")); err != nil || string(v) != "value2" {
		t.Fatalf("Expected key2=value2, got key2=%s instead", string(v))
	}

	if v, err := tx.Get([]byte("key3")); err != nil || len(v) > 0 {
		t.Fatalf("Expected key3 should not get, got key3=%s instead", string(v))
	}

	// db should: key1=value1, key2=value2
	err = tx.Put([]byte("key2"), []byte("value5"))

	err = tx.RollBack()
	if err != nil {
		t.Error("RollBack Error: ", err)
	}
	// db should: key1=value1, key2=value2
	if v, err := db.Get([]byte("key1")); err != nil || string(v) != "value1" {
		t.Fatalf("Expected key1=value1, got key1=%s instead, err: %s", string(v), err.Error())
	}

	if v, err := db.Get([]byte("key2")); err != nil || string(v) != "value2" {
		t.Fatalf("Expected key2=value2, got key2=%s instead, err: %s", string(v), err.Error())
	}

	if v, err := db.Get([]byte("key3")); err != nil || len(v) > 0 {
		t.Fatalf("Expected key3=, got key3=%s instead, err: %s", string(v), err.Error())
	}

}

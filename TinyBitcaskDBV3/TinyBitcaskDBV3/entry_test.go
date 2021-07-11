package TinyBitcaskDBV3

import (
	"testing"
)

func TestNewEntry(t *testing.T) {
	key, value := []byte("test_key"), []byte("test_value")
	_ = NewEntry(key, value, Put, String)
}

func TestEncode(t *testing.T) {
	key, value := []byte("test_key"), []byte("test_value")
	e := NewEntry(key, value, Put, String)
	_, err := e.Encode()
	if err != nil {
		t.Error("Encode Error: ", err)
	}
}

func TestDecode(t *testing.T) {
	key, value := []byte("test_key"), []byte("test_value")
	e1 := NewEntry(key, value, Put, String)
	buf, err := e1.Encode()
	if err != nil {
		t.Error("Encode Error: ", err)
	}

	e2, err := Decode(buf)
	if err != nil {
		t.Error("Decode Process Error: ", err)
	}

	if e1.Meta.KeySize != e2.Meta.KeySize || e1.Meta.ValueSize != e2.Meta.ValueSize {
		// log.Printf("decode key: %s, value: %s\n", string(e2.Meta.Key), string(e2.Meta.Value))
		t.Error("Decode Value Error: ", err)
	}
}

func BenchmarkEncode(b *testing.B) {
	key, value := []byte("test_key"), []byte("test_value")
	for i := 0; i < b.N; i++{
		e1 := NewEntry(key, value, Put, String)
		_, err := e1.Encode()
		if err != nil{
			b.Error("Encode Error: ", err)
		}
	}
}
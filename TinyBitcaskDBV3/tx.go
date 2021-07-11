package TinyBitcaskDBV3

import (
	"errors"
	"sync"
	"sync/atomic"
	"time"
)

type Value []byte

// Begin starts a new transaction
func (db *TinyDB) Begin() *Tx {
	return &Tx{
		db:        db,
		done:      0,
		txKeyDir:  make(map[string]Value),
		txEntries: make([]TxEntry, 0),
	}
}

// Tx is a transaction
// Last write wins
type TxEntry struct {
	ts    time.Time //
	mark  uint16
	key   []byte
	value []byte
}

type Tx struct {
	db        *TinyDB
	mu        sync.RWMutex
	done      uint32
	txKeyDir  map[string]Value
	txEntries []TxEntry
	//ts       time.Time // start time for transaction
}

const (
	maxKeyLen   = int(^uint32(0) - 1)
	maxValueLen = int(^uint32(0) - 1)
)

var (
	ErrTxDone       = errors.New("transaction done")
	ErrKeyTooLong   = errors.New("key is too long")
	ErrValueTooLong = errors.New("value is too long")
)

// Get returns the store value of the specified key. It takes key-value pairs
// that will upload in the current transaction into account
func (t *Tx) Get(key []byte) ([]byte, error) {

	if atomic.LoadUint32(&t.done) == 1 {
		return nil, ErrTxDone
	}

	if len(key) > maxKeyLen {
		return nil, ErrKeyTooLong
	}

	t.mu.RLock()
	defer t.mu.RUnlock()
	v1, ok := t.txKeyDir[string(key)]
	if !ok {
		v2, err := t.db.Get(key)
		if err != nil {
			return nil, err
		}
		v1 = v2
		t.txKeyDir[string(key)] = v1
	}

	return v1, nil
}

func (t *Tx) Put(key, value []byte) error {

	if atomic.LoadUint32(&t.done) == 1 {
		return ErrTxDone
	}

	if len(key) > maxKeyLen {
		return ErrKeyTooLong
	}

	if len(value) > maxValueLen {
		return ErrValueTooLong
	}

	t.mu.Lock()
	t.txEntries = append(t.txEntries, TxEntry{key: key, value: value, mark: Put, ts: time.Now()})
	t.mu.Unlock()
	return nil
}

// delete
func (t *Tx) Delete(key []byte) error {

	if atomic.LoadUint32(&t.done) == 1 {
		return ErrTxDone
	}

	if len(key) > maxKeyLen {
		return ErrKeyTooLong
	}

	t.mu.Lock()
	defer t.mu.Unlock()
	delete(t.txKeyDir, string(key))
	return nil
}

// Commit commits the transaction
func (t *Tx) Commit() error {

	if atomic.LoadUint32(&t.done) == 1 {
		return ErrTxDone
	}

	//
	t.mu.Lock()
	defer t.mu.Unlock()
	m := make(map[string]TxEntry)
	for _, txEntry := range t.txEntries {
		key, ts := txEntry.key, txEntry.ts
		if mTxEntry, ok := m[string(key)]; !ok {
			m[string(key)] = txEntry
		} else {
			if ts.After(mTxEntry.ts) {
				m[string(key)] = txEntry
			}
		}
	}

	for k, txEntry := range m {
		if txEntry.mark == Put {
			t.db.Put([]byte(k), txEntry.value)
		} else {
			t.db.Del([]byte(k))
		}
	}

	atomic.CompareAndSwapUint32(&t.done, 0, 1)

	return t.db.Sync() // Write into disk
}

// RollBack discard all changes of the current transaction
func (t *Tx) RollBack() error {

	if atomic.LoadUint32(&t.done) == 1 {
		return ErrTxDone
	}

	t.mu.Lock()
	defer t.mu.Unlock()
	t.txKeyDir = nil
	t.txEntries = nil

	atomic.CompareAndSwapUint32(&t.done, 0, 1)

	return nil
}

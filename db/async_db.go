package db

import (
	"fmt"
	"sync"
)

// AsyncDB is a database that spawns a goroutine to handle database writes. In the case of a read coming in that is currently being written,
// AsyncDB will allow only one goroutine to write at a time, however. This essentially offloads the writing into batch writes.
// Note that there are two key functionalities: keeping a write cache until the number of flushes exceeds flushLimit, and streaming the
// resulting batch to memory.

// Note that when data is transferred between sources, both sources are locked from read and write. When it is done these locks are
// released. When data goes from db.toWrite => db.inWrite, data before exists in db.toWrite and after exists in db.inWrite.
// When data goes from db.inWrite => db.db, the data may exist in both places, but is deleted once all of db.inWrite has been
// persisted to disk.

type AsyncDB struct {
	db            DB
	mtx           sync.Mutex // mutex for accessing toWrite
	baseMtx       sync.Mutex // mutex for accessing inWrite. Note that only WriteRoutine should alter inWrite
	flushMtx      sync.Mutex // mutex for flushing.
	toWrite       map[string][]byte
	inWrite       map[string][]byte
	flushLimit    int
	writeTrigChan chan bool

	itrCtr            int
	itrCtrReleaseChan chan bool
	itrCtrMtx         sync.Mutex
}

func init() {
	println("REGISTERING ASYNC DB")
	dbCreator := func(name string, dir string) (DB, error) {
		return NewDB(name, "goleveldb", dir), nil
	}
	registerDBCreator(AsyncDBBackend, dbCreator, false)
}

func NewAsyncDB(name string, dir string, dbBackendType DBBackendType) (*AsyncDB, error) {
	db := &AsyncDB{
		db:                NewDB(name, dbBackendType, dir),
		toWrite:           make(map[string][]byte),
		inWrite:           make(map[string][]byte),
		flushLimit:        10000,
		writeTrigChan:     make(chan bool),
		itrCtr:            0,
		itrCtrReleaseChan: make(chan bool)}
	go db.WriteRoutine()
	return db, nil
}

// Mutate iterator count. DO NOT USE UNLESS YOU KNOW WHAT YOU ARE DOING! Can lead to large unintended memory leaks
func (db *AsyncDB) _ChangeIterCount(numIters int) {
	db.itrCtrMtx.Lock()
	db.itrCtr += numIters
	db.itrCtrMtx.Unlock()
}

func (db *AsyncDB) _TryFlush() {
	if len(db.toWrite) > db.flushLimit {
		db.writeTrigChan <- true
	}
}

func (db *AsyncDB) _WriteRoutine(sync bool) {
	db.flushMtx.Lock()
	db.itrCtrMtx.Lock()
	for db.itrCtr > 0 {
		db.itrCtrMtx.Unlock()
		<-db.itrCtrReleaseChan // While waiting for iterator release channel to trigger, we keep it unlocked
		db.itrCtrMtx.Lock()
	}
	defer db.itrCtrMtx.Unlock()
	defer db.flushMtx.Unlock()

	if len(db.inWrite) > 0 {
		panic(fmt.Sprintf("Invariant violated: len(AsyncDB.inWrite) should be 0, but is actually %d", len(db.inWrite)))
	}

	// Loop active, so write a batch. db.inWrite should be set to db.toWrite, and db.toWrite should be refreshed
	// Note that inWrite should not be altered until the write has been complete.
	db.mtx.Lock()
	db.baseMtx.Lock()
	db.inWrite = db.toWrite
	db.toWrite = make(map[string][]byte)
	db.mtx.Unlock()
	db.baseMtx.Unlock()

	// If there has been a request for more than one write while this routine was blocked, the above mutation will have batched it all into a single write.
	// db.inWrite will therefore be empty and it is safe to unlock db.inWrite and continue.
	if len(db.inWrite) == 0 {
		return
	}

	// Finally perform write onto underlying DB
	batch := db.db.NewBatch()
	db.baseMtx.Lock()
	for k, v := range db.inWrite {
		if v == nil {
			batch.Delete([]byte(k))
		} else {
			batch.Set([]byte(k), db.inWrite[k])
		}
	}
	db.baseMtx.Unlock()
	if sync {
		batch.WriteSync()
	} else {
		batch.Write()
	}

	// End by refreshing inWrite
	db.baseMtx.Lock()
	db.inWrite = make(map[string][]byte)
	db.baseMtx.Unlock()
}

func (db *AsyncDB) WriteRoutine() {
	// Each iteration of this loop is a write to disk.
	// INVARIANTS: inWrite should be empty unless the loop is active.
	for _ = range db.writeTrigChan {

		db._WriteRoutine(false)
	}
}

// Check flush cache, check currently writing, then finally check DB
func (db *AsyncDB) Get(key []byte) []byte {
	key = nonNilBytes(key)
	k := string(key)

	db.mtx.Lock()
	if val, ok := db.toWrite[k]; ok && val != nil {
		db.mtx.Unlock()
		return val
	}
	db.mtx.Unlock()

	db.baseMtx.Lock()
	if val, ok := db.inWrite[k]; ok && val != nil {
		db.baseMtx.Unlock()
		return val
	}
	db.baseMtx.Unlock()

	return db.db.Get(key)
}

func (db *AsyncDB) Has(key []byte) bool {
	key = nonNilBytes(key)
	k := string(key)

	db.mtx.Lock()
	if val, ok := db.toWrite[k]; ok && val != nil {
		db.mtx.Unlock()
		return true
	}
	db.mtx.Unlock()

	db.baseMtx.Lock()
	if val, ok := db.inWrite[k]; ok && val != nil {
		db.baseMtx.Unlock()
		return true
	}
	db.baseMtx.Unlock()

	return db.db.Has(key)
}

func (db *AsyncDB) Set(key, val []byte) {
	key = nonNilBytes(key)
	val = nonNilBytes(val)
	k := string(key)

	db.mtx.Lock()
	db.toWrite[k] = val
	db.mtx.Unlock()

	db._TryFlush()
}

// First, trigger a set
// Two cases:
// Flush in progress: you must wait for current flush to end, then start a new flush. This can happen simply by calling
// the write routine
// Flush not in progress: trigger new flush.
// Either way, the lock handles it automatically.
func (db *AsyncDB) SetSync(key, val []byte) {
	db.Set(key, val)
	db._WriteRoutine(true)
}

func (db *AsyncDB) Delete(key []byte) {
	key = nonNilBytes(key)
	k := string(key)

	db.mtx.Lock()
	db.toWrite[k] = nil
	db.mtx.Unlock()

	db._TryFlush()
}

func (db *AsyncDB) DeleteSync(key []byte) {
	db.Delete(key)
	db._WriteRoutine(true)
}

func (db *AsyncDB) Iterator(start, end []byte) {

}

type asyncDBIterator struct {
	toWriteKeys       []string
	curToWrite        int
	db                *AsyncDB
	backendDBIterator Iterator
	isReverse         bool
	start             []byte
	end               []byte
	closed            bool
}

// Note that the iterator mutex is held during the entire write process, so an iterator cannot be created during mutation

func newAsyncDBIterator(db *AsyncDB, toWriteKeys []string, start, end []byte, isReverse bool) (*asyncDBIterator) {
	var backendDBIterator Iterator

	db._ChangeIterCount(1)

	if len(db.inWrite) > 0 {
		panic(fmt.Sprintf("AsyncDBIterator invariant violated: len(AsyncDB.inWrite) should be 0, but is actually %d", len(db.inWrite)))
	}

	// Reverse memKeys if needed and set backend iterator to expected order.
	if isReverse {
		nkeys := len(toWriteKeys)
		for i := 0; i < nkeys/2; i++ {
			temp := toWriteKeys[i]
			toWriteKeys[i] = toWriteKeys[nkeys-i-1]
			toWriteKeys[nkeys-i-1] = temp
		}
		backendDBIterator = db.db.ReverseIterator(start, end)
	} else {
		backendDBIterator = db.db.Iterator(start, end)
	}
	return &asyncDBIterator{
		toWriteKeys:           toWriteKeys,
		curToWrite:            0,
		db:         	   db,
		backendDBIterator: backendDBIterator,
		isReverse:         isReverse,
		start:             start,
		end:               end,
		closed:            false,
	}
}

// Implements Iterator
func (itr *asyncDBIterator) Domain() ([]byte, []byte) {
	return itr.start, itr.end
}

// Implements Iterator
func (itr *asyncDBIterator) Valid() bool {
	// Either there are more memKeys, or there are more values in the backend iterator, or both
	valid := (0 <= itr.curToWrite && itr.curToWrite < len(itr.toWriteKeys)) || itr.backendDBIterator.Valid()
	return valid
}

// Implements Iterator
func (itr *asyncDBIterator) Next() {
	itr.assertIsValid()
	// Choose which source is the current source of iterator, and increment it
	if itr.isWriteCurr() {
		itr.curToWrite++
	} else {
		itr.backendDBIterator.Next()
	}
}

// Implements Iterator
func (itr *asyncDBIterator) Key() []byte {
	itr.assertIsValid()
	if itr.isWriteCurr() {
		return []byte(itr.toWriteKeys[itr.curToWrite])
	} else {
		return itr.backendDBIterator.Key()
	}
}

// Implements Iterator
func (itr *asyncDBIterator) Value() []byte {
	itr.assertIsValid()
	curKey := itr.Key()
	return itr.db.Get(curKey)
}

// Implements Iterator
func (itr *asyncDBIterator) Close() {
	if !itr.closed {
		itr.closed = true
		itr.db._ChangeIterCount(-1)
	}
}

// Returns whether the current value is stored in memory or in iterator
func (itr *asyncDBIterator) isWriteCurr() bool {
	// If itr.backendDBIterator is not valid, then itr.memKeys must be the one storing the current key
	if !itr.backendDBIterator.Valid() {
		return true

		// If itr.memKeys is not valid, must be itr.backendDBIterator that is valid.
	} else if itr.curToWrite >= len(itr.toWriteKeys) {
		return false

		// If both valid, then compare
	} else if itr.isReverse {

		// If itr is reversed, larger of two must be iterated first
		return itr.toWriteKeys[itr.curToWrite] > string(itr.backendDBIterator.Key())
	} else {

		// If itr is not reversed, smaller of two must be iterated first
		return itr.toWriteKeys[itr.curToWrite] < string(itr.backendDBIterator.Key())
	}
}

func (itr *asyncDBIterator) assertIsValid() {
	if !itr.Valid() {
		panic("cacheDBIterator is invalid")
	}
}

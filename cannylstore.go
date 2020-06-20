package kvbench

import (
	"sync"

	"github.com/thesues/cannyls-go/block"
	"github.com/thesues/cannyls-go/lump"
	cannyls "github.com/thesues/cannyls-go/storage"
)

type CannylsStore struct {
	sync.Mutex
	db    *cannyls.Storage
	ab    *block.AlignedBytes
	fsync bool
}

func NewCannylsStore(path string, fsync bool) (Store, error) {
	db, err := cannyls.CreateCannylsStorage(path, 8<<30, 0.3)
	if err != nil {
		return nil, err
	}
	return &CannylsStore{
		db:    db,
		fsync: fsync,
		ab:    block.NewAlignedBytes(512, block.Min()),
	}, nil

}

func (s *CannylsStore) Close() error {
	s.db.Close()
	return nil
}

func (s *CannylsStore) PSet(keys, vals [][]byte) error {
	s.Lock()
	defer s.Unlock()
	for i := range keys {
		err := s.set(keys[i], vals[i])
		if err != nil {
			return err
		}
	}
	s.flushDB()
	return nil
}

func (s *CannylsStore) PGet(keys [][]byte) ([][]byte, []bool, error) {
	panic("not implemented")

}

func (s *CannylsStore) Set(key, value []byte) error {
	s.Lock()
	defer s.Unlock()
	return s.set(key, value)

}
func (s *CannylsStore) set(key, value []byte) error {
	if len(key) > 8 {
		panic("not implemented")
	}
	id, _ := lump.FromBytes(key)
	s.ab.Resize(uint32(len(value)))
	lumpData := lump.NewLumpDataWithAb(s.ab)
	s.db.Put(id, lumpData)
	//s.db.PutEmbed(id, value)
	if s.fsync {
		s.db.JournalSync()
	}
	return nil
}

func (s *CannylsStore) Get(key []byte) ([]byte, bool, error) {
	s.Lock()
	defer s.Unlock()
	return s.get(key)

}
func (s *CannylsStore) get(key []byte) ([]byte, bool, error) {
	if len(key) > 8 {
		panic("not implemented")
	}
	id, _ := lump.FromBytes(key)
	data, err := s.db.Get(id)
	return data, data != nil, err
}

func (s *CannylsStore) Del(key []byte) (bool, error) {
	s.Lock()
	defer s.Unlock()
	return s.del(key)

}
func (s *CannylsStore) del(key []byte) (bool, error) {
	if len(key) > 8 {
		panic("not implemented")
	}
	id, _ := lump.FromBytes(key)
	deleted, _, err := s.db.Delete(id)
	return deleted, err
}

func (s *CannylsStore) Keys(pattern []byte, limit int, withvals bool) ([][]byte, [][]byte, error) {
	panic("not implemented")

}

func (s *CannylsStore) FlushDB() error {
	s.Lock()
	defer s.Unlock()
	return s.flushDB()
}
func (s *CannylsStore) flushDB() error {
	s.db.JournalSync()
	return nil
}

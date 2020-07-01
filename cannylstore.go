package kvbench

import (
	lru "github.com/hashicorp/golang-lru"
	"github.com/thesues/cannyls-go/block"
	"github.com/thesues/cannyls-go/lump"
	cannyls "github.com/thesues/cannyls-go/storage"
)

type CannylsStore struct {
	db        *cannyls.Storage
	ab        *block.AlignedBytes
	readCache *lru.Cache
	fsync     bool
}

func NewCannylsStore(path string, fsync bool) (Store, error) {
	db, err := cannyls.CreateCannylsStorage(path, 8<<30, 0.3)
	if err != nil {
		return nil, err
	}
	cache, err := lru.New(250000)
	return &CannylsStore{
		db:        db,
		fsync:     fsync,
		ab:        block.NewAlignedBytes(512, block.Min()),
		readCache: cache,
	}, nil

}

func (s *CannylsStore) Close() error {
	s.db.Close()
	return nil
}

func (s *CannylsStore) PSet(keys, vals [][]byte) error {
	ab := block.NewAlignedBytes(512, block.Min())
	for i := range keys {
		id, _ := lump.FromBytes(keys[i])
		ab.Resize(uint32(len(vals[i])))
		lumpData := lump.NewLumpDataWithAb(ab)
		s.db.Put(id, lumpData)
		//s.db.PutEmbed(id, value)
		if s.fsync {
			s.db.Sync()
		}
	}
	s.FlushDB()
	return nil
}

func (s *CannylsStore) PGet(keys [][]byte) ([][]byte, []bool, error) {
	panic("not implemented")

}

func (s *CannylsStore) Set(key, value []byte) error {
	if len(key) > 8 {
		panic("not implemented")
	}
	id, _ := lump.FromBytes(key)
	lumpData := lump.NewLumpDataWithAb(block.FromBytes(value, block.Min()))
	/*
		s.ab.Resize(uint32(len(value)))
		lumpData := lump.NewLumpDataWithAb(s.ab)
	*/
	s.db.Put(id, lumpData)
	s.readCache.Add(id.U64(), lumpData)
	if s.fsync {
		s.db.Sync()
	}
	return nil
}

func (s *CannylsStore) Get(key []byte) ([]byte, bool, error) {
	if len(key) > 8 {
		panic("not implemented")
	}
	id, _ := lump.FromBytes(key)
	if v, ok := s.readCache.Get(id.U64()); ok {

		r := v.(lump.LumpData)
		return r.Inner.AsBytes(), true, nil
	}
	data, err := s.db.Get(id)
	return data, data != nil, err
}

func (s *CannylsStore) Del(key []byte) (bool, error) {
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
	s.db.Flush()
	return nil
}
func (s *CannylsStore) flushDB() error {
	s.db.Sync()
	return nil
}

package kvbench

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"os"
)

type FileStore struct {
	fsync bool
	path  string
}

func (s *FileStore) toFileName(key []byte) string {
	n := binary.BigEndian.Uint64(key)
	return fmt.Sprintf("%s/%d", s.path, n)
}

func NewFileStore(path string, fsync bool) (Store, error) {
	os.Mkdir(path, 0755)
	return &FileStore{
		fsync: fsync,
		path:  path,
	}, nil
}

func (s *FileStore) Close() error {
	return nil
}

func (s *FileStore) PSet(keys, vals [][]byte) error {
	for i := range keys {
		err := s.Set(keys[i], vals[i])
		if err != nil {
			return err
		}
	}
	return nil
}

func (s *FileStore) PGet(keys [][]byte) ([][]byte, []bool, error) {
	panic("not implemented")
}
func (s *FileStore) Set(key, value []byte) error {
	flag := os.O_CREATE | os.O_RDWR
	if s.fsync {
		flag |= os.O_SYNC
	}
	file, err := os.OpenFile(s.toFileName(key), flag, 0644)
	if err != nil {
		return err
	}
	defer file.Close()
	_, err = file.Write(value)
	if err != nil {
		return err
	}
	return nil
}

func (s *FileStore) Get(key []byte) ([]byte, bool, error) {
	file, err := os.OpenFile(s.toFileName(key), os.O_RDONLY, 0644)
	if err != nil {
		return nil, false, err
	}
	defer file.Close()
	buf := new(bytes.Buffer)
	_, err = buf.ReadFrom(file)
	if err != nil {
		return nil, false, err
	}
	return buf.Bytes(), true, nil

}

func (s *FileStore) Del(key []byte) (bool, error) {
	err := os.Remove(s.toFileName(key))
	if err != nil {
		return false, err
	}
	return true, nil
}

func (s *FileStore) Keys(pattern []byte, limit int, withvals bool) ([][]byte, [][]byte, error) {
	panic("not implemented")
}
func (s *FileStore) FlushDB() error {
	return nil
}

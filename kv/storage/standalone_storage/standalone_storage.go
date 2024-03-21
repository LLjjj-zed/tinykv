package standalone_storage

import (
	"errors"
	"fmt"
	"github.com/Connor1996/badger"
	"github.com/pingcap-incubator/tinykv/kv/config"
	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
)

// StandAloneStorage is an implementation of `Storage` for a single-node TinyKV instance. It does not
// communicate with other nodes and all data is stored locally.
type StandAloneStorage struct {
	// Your Data Here (1).
	data *badger.DB
}

func NewStandAloneStorage(conf *config.Config) *StandAloneStorage {
	// Your Code Here (1).
	opts := badger.DefaultOptions
	opts.Dir = conf.DBPath
	opts.ValueDir = conf.DBPath
	db, err := badger.Open(opts)
	if err != nil {
		panic(err)
	}
	return &StandAloneStorage{data: db}
}

func (s *StandAloneStorage) Start() error {
	return nil
}

func (s *StandAloneStorage) Stop() error {
	// Your Code Here (1).
	err := s.data.Close()
	return err
}

func (s *StandAloneStorage) Reader(ctx *kvrpcpb.Context) (storage.StorageReader, error) {
	// Your Code Here (1).
	return &StandAloneStorageReader{txn: s.data.NewTransaction(false)}, nil
}

func (s *StandAloneStorage) Write(ctx *kvrpcpb.Context, batch []storage.Modify) error {
	// Your Code Here (1).
	txn := s.data.NewTransaction(true)
	for _, m := range batch {
		switch m.Data.(type) {
		case storage.Put:
			put := m.Data.(storage.Put)
			fmt.Println("Put", put.Cf, put.Key, put.Value)
			err := txn.Set(engine_util.KeyWithCF(put.Cf, put.Key), put.Value)
			if err != nil {
				return err
			}
		case storage.Delete:
			del := m.Data.(storage.Delete)
			err := txn.Delete(engine_util.KeyWithCF(del.Cf, del.Key))
			if err != nil {
				return err
			}
		}
	}
	err := txn.Commit()
	if err != nil {
		return err
	}
	return nil
}

type StandAloneStorageReader struct {
	txn *badger.Txn
}

func (s StandAloneStorageReader) GetCF(cf string, key []byte) ([]byte, error) {
	val, err := engine_util.GetCFFromTxn(s.txn, cf, key)
	if err != nil {
		if errors.Is(err, badger.ErrKeyNotFound) {
			return nil, nil
		}
		return nil, err
	}
	return val, nil
}

func (s StandAloneStorageReader) IterCF(cf string) engine_util.DBIterator {
	iter := engine_util.NewCFIterator(cf, s.txn)
	return iter
}

func (s StandAloneStorageReader) Close() {
	err := s.txn.Commit()
	if err != nil {
		fmt.Println("Failed to commit txn")
	}
}

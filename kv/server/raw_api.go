package server

import (
	"context"
	"fmt"
	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
)

// The functions below are Server's Raw API. (implements TinyKvServer).
// Some helper methods can be found in sever.go in the current directory

// RawGet return the corresponding Get response based on RawGetRequest's CF and Key fields
func (server *Server) RawGet(_ context.Context, req *kvrpcpb.RawGetRequest) (*kvrpcpb.RawGetResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using reader.GetCF
	reader, err := server.storage.Reader(req.GetContext())
	if err != nil {
		return &kvrpcpb.RawGetResponse{
			Error:    "",
			Value:    nil,
			NotFound: true,
		}, err
	}
	val, err := reader.GetCF(req.GetCf(), req.GetKey())
	if err != nil {
		return &kvrpcpb.RawGetResponse{
			Error:    "",
			Value:    nil,
			NotFound: false,
		}, err
	}
	if val == nil {
		return &kvrpcpb.RawGetResponse{
			Error:    "",
			Value:    nil,
			NotFound: true,
		}, nil
	}
	return &kvrpcpb.RawGetResponse{
		Error:    "",
		Value:    val,
		NotFound: false,
	}, nil
}

// RawPut puts the target data into storage and returns the corresponding response
func (server *Server) RawPut(_ context.Context, req *kvrpcpb.RawPutRequest) (*kvrpcpb.RawPutResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using Storage.Modify to store data to be modified
	fmt.Println("RawPut", req.GetCf(), req.GetKey(), req.GetValue())
	err := server.storage.Write(req.GetContext(), []storage.Modify{
		{
			Data: storage.Put{
				Cf:    req.GetCf(),
				Key:   req.GetKey(),
				Value: req.GetValue(),
			},
		},
	})
	if err != nil {
		return &kvrpcpb.RawPutResponse{
			Error: err.Error(),
		}, err
	}

	return nil, nil
}

// RawDelete delete the target data from storage and returns the corresponding response
func (server *Server) RawDelete(_ context.Context, req *kvrpcpb.RawDeleteRequest) (*kvrpcpb.RawDeleteResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using Storage.Modify to store data to be deleted
	err := server.storage.Write(req.GetContext(), []storage.Modify{
		{
			Data: storage.Delete{
				Cf:  req.GetCf(),
				Key: req.GetKey(),
			},
		},
	})
	if err != nil {
		return &kvrpcpb.RawDeleteResponse{
			Error: err.Error(),
		}, err
	}
	return &kvrpcpb.RawDeleteResponse{}, nil
}

// RawScan scan the data starting from the start key up to limit. and return the corresponding result
func (server *Server) RawScan(_ context.Context, req *kvrpcpb.RawScanRequest) (*kvrpcpb.RawScanResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using reader.IterCF
	reader, err := server.storage.Reader(req.GetContext())
	if err != nil {
		return &kvrpcpb.RawScanResponse{
			Error: err.Error(),
		}, err
	}
	iter := reader.IterCF(req.GetCf())
	defer iter.Close()
	var kvs []*kvrpcpb.KvPair
	iter.Seek(req.GetStartKey())
	for i := 0; i < int(req.GetLimit()) && iter.Valid(); i++ {
		item := iter.Item()
		val, err := item.Value()
		if err != nil {
			return &kvrpcpb.RawScanResponse{
				Error: err.Error(),
			}, err
		}
		kvs = append(kvs, &kvrpcpb.KvPair{
			Key:   item.KeyCopy(nil),
			Value: val,
		})
		iter.Next()
	}
	return &kvrpcpb.RawScanResponse{
		Kvs: kvs,
	}, nil
}

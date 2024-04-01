// Copyright 2015 The etcd Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package raft

import (
	"fmt"
	pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
	"github.com/pingcap/log"
	"github.com/pkg/errors"
)

// RaftLog manage the log entries, its struct look like:
//
// 管理日志条目，其结构如下：
//
//	snapshot/first.....applied....committed....stabled.....last
//	--------|------------------------------------------------|
//	                          log entries
//
// for simplify the RaftLog implement should manage all log entries
// that not truncated
//
// 为了简化RaftLog实现，应该管理所有未截断的日志条目
type RaftLog struct {
	// storage contains all stable entries since the last snapshot.
	// storage包含自上次快照以来的所有稳定条目(以commit)。
	storage Storage

	// committed is the highest log position that is known to be in
	// stable storage on a quorum of nodes.
	// committed是已知在大多数节点上稳定存储的最高日志位置。
	committed uint64

	// applied is the highest log position that the application has
	// been instructed to apply to its state machine.
	// Invariant: applied <= committed
	// applied是应用程序被指示应用于其状态机的最高日志位置。
	applied uint64

	// log entries with index <= stabled are persisted to storage.
	// It is used to record the logs that are not persisted by storage yet.
	// Everytime handling `Ready`, the unstabled logs will be included.
	// stabled是已持久化到存储的日志条目。
	stabled uint64

	// all entries that have not yet compact.
	// 所有尚未压缩的条目。也就是 snapshot 之后所有的数据都在 entries 里面，其包含了持久化和未持久化的数据
	// 所以当你新建一个 RaftLog 的时候，你需要先从上层 storage 获取所有的未被持久化的 entries
	entries []pb.Entry

	// the incoming unstable snapshot, if any.
	// (Used in 2C)
	// 传入的不稳定快照(如果有)。
	pendingSnapshot *pb.Snapshot

	// Your Data Here (2A).
	dummyIndex uint64
}

// newLog returns log using the given storage. It recovers the log
// to the state that it just commits and applies the latest snapshot.
// 返回一个使用给定storage的 RaftLog 实例。它恢复日志到刚刚提交并应用最新快照的状态。
func newLog(storage Storage) *RaftLog {
	// Your Code Here (2A).
	if storage == nil {
		log.Panic("storage must not be nil")
	}

	snapshot, err := storage.Snapshot()
	if err != nil {
		log.Panic(err.Error())
	}

	firstIndex, err := storage.FirstIndex()
	if err != nil {
		panic(err)
	}

	lastIndex, err := storage.LastIndex()
	if err != nil {
		panic(err)
	}

	entries, err := storage.Entries(firstIndex, lastIndex+1)
	if err != nil {
		panic(err)
	}

	return &RaftLog{
		storage:         storage,
		committed:       firstIndex - 1,
		applied:         firstIndex - 1,
		pendingSnapshot: &snapshot,
		entries:         entries,
		dummyIndex:      firstIndex - 1,
	}
}

// 设置 applied时，需要保证applied <= committed
func (l *RaftLog) appliedTo(i uint64) {
	if i == 0 {
		return
	}
	if l.committed < i || i < l.applied {
		sprintf := fmt.Sprintf("applied(%d) is out of range [prevApplied(%d), committed(%d)]", i, l.applied, l.committed)
		log.Panic(sprintf)
	}
	l.applied = i
}

// We need to compact the log entries in some point of time like
// storage compact stabled log entries prevent the log entries
// grow unlimitedly in memory
func (l *RaftLog) maybeCompact() {
	// Your Code Here (2C).
}

// allEntries return all the entries not compacted.
// note, exclude any dummy entries from the return value.
// note, this is one of the test stub functions you need to implement.
// 返回所有未压缩的条目。返回结果中不应包含任何虚拟条目
func (l *RaftLog) allEntries() []pb.Entry {
	// Your Code Here (2A).
	first, err := l.storage.FirstIndex()
	if err != nil {
		log.Panic(err.Error())
	}
	return l.entries[first:]
}

// unstableEntries return all the unstable entries
func (l *RaftLog) unstableEntries() []pb.Entry {
	// Your Code Here (2A).
	return l.entries[l.stabled+1:]
}

// nextEnts returns all the committed but not applied entries
func (l *RaftLog) nextEnts() (ents []pb.Entry) {
	// Your Code Here (2A).
	return l.entries[l.applied+1 : l.committed+1]
}

// LastIndex return the last index of the log entries
func (l *RaftLog) LastIndex() uint64 {
	// Your Code Here (2A).
	return uint64(len(l.entries) - 1)
}

// Term return the term of the entry in the given index
func (l *RaftLog) Term(i uint64) (uint64, error) {
	// Your Code Here (2A).
	if i < l.dummyIndex || i > l.LastIndex() {
		return 0, errors.Errorf("index is out of range , i:%d", i)
	}
	return l.entries[i].Term, nil
}

func (l *RaftLog) getEntries(start, end uint64) []pb.Entry {
	if start > l.LastIndex() {
		return nil
	}
	if end == -1 {
		return l.entries[start:]
	}
	return l.entries[start:end]
}

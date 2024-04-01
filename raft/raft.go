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
	"errors"

	pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
)

// None is a placeholder node ID used when there is no leader.
// None是一个占位符节点ID，用于没有领导者的情况。
const None uint64 = 0

// StateType represents the role of a node in a cluster.
// StateType表示集群中节点的角色。
type StateType uint64

const (
	StateFollower StateType = iota
	StateCandidate
	StateLeader
)

// stmap is a map for StateType to string.
// stmap是StateType到字符串的映射。
var stmap = [...]string{
	"StateFollower",
	"StateCandidate",
	"StateLeader",
}

func (st StateType) String() string {
	return stmap[uint64(st)]
}

// ErrProposalDropped is returned when the proposal is ignored by some cases,
// so that the proposer can be notified and fail fast.
// ErrProposalDropped 返回当选举被某些情况忽略时，开启选举的节点可以被通知并快速失败。
var ErrProposalDropped = errors.New("raft proposal dropped")

// Config contains the parameters to start a raft.
// Config包含启动raft所需的参数。
type Config struct {
	// ID is the identity of the local raft. ID cannot be 0.
	// ID是本地raft的标识。ID不能为0。
	ID uint64

	// peers contains the IDs of all nodes (including self) in the raft cluster. It
	// should only be set when starting a new raft cluster. Restarting raft from
	// previous configuration will panic if peers is set. peer is private and only
	// used for testing right now.
	// peers包含raft集群中所有节点(包括自身)的ID。仅在启动新的raft集群时才应设置它。
	//如果设置了peers，则从以前的配置重新启动raft将导致panic。peer是私有的，目前仅用于测试。
	peers []uint64

	// ElectionTick is the number of Node.Tick invocations that must pass between
	// elections. That is, if a follower does not receive any message from the
	// leader of current term before ElectionTick has elapsed, it will become
	// candidate and start an election. ElectionTick must be greater than
	// HeartbeatTick. We suggest ElectionTick = 10 * HeartbeatTick to avoid
	// unnecessary leader switching.
	// ElectionTick 是指必须通过的 Node.Tick 调用次数，这两次调用之间的间隔用于进行选举。
	// 换句话说，如果一个追随者（follower）在当前任期（term）内没有在 ElectionTick 指定的时间内从领导者（leader）那里收到任何消息，
	// 那么它将成为候选者（candidate）并开始进行选举。
	// 为了确保系统的稳定性和避免频繁的不必要的领导者切换，ElectionTick 必须大于 HeartbeatTick。
	// HeartbeatTick 是领导者向追随者发送心跳消息的频率。 建议的设置是 ElectionTick = 10 * HeartbeatTick。
	// 这样，即使领导者因为某种原因暂时无法发送心跳，追随者也不会立即启动选举。而是会等待相对较长的一段时间，这有助于减少因网络抖动或其他临时问题导致的非必要领导者切换。
	ElectionTick int
	// HeartbeatTick is the number of Node.Tick invocations that must pass between
	// heartbeats. That is, a leader sends heartbeat messages to maintain its
	// leadership every HeartbeatTick ticks.
	// HeartbeatTick 是指必须在两次心跳消息之间通过的 Node.Tick 调用次数。在 Raft 一致性算法中，
	//领导者（leader）通过定期发送心跳消息来维持其领导地位。每经过 HeartbeatTick 次 Node.Tick 调用，
	//领导者就会向所有追随者（followers）发送一次心跳消息。
	HeartbeatTick int

	// Storage is the storage for raft. raft generates entries and states to be
	// stored in storage. raft reads the persisted entries and states out of
	// Storage when it needs. raft reads out the previous state and configuration
	// out of storage when restarting.
	// Storage 是 Raft 算法的存储组件。在 Raft 系统中，日志条目（entries）和状态（states）被生成并存储在 Storage 中。
	// 当 Raft 需要这些数据时，它会从 Storage 中读取这些已持久化的日志条目和状态。在 Raft 重启时，它也会从 Storage 中读取之前的状态和配置信息。
	// 更具体地说，Storage 组件负责提供以下功能：
	// 持久化存储：当 Raft 算法生成新的日志条目或更新状态时，Storage 负责将这些数据持久化到存储介质（如磁盘）上，以防止数据丢失。
	// 读取数据：在 Raft 算法的运行过程中，比如在进行日志复制、提交日志条目或处理选举时，需要读取之前存储的日志条目和状态。Storage 提供接口供 Raft 算法读取这些数据。
	// 恢复状态：当 Raft 节点重启时，它需要恢复之前的状态和配置信息，以便能够继续参与集群的运行。Storage 提供了从持久化存储中读取这些信息的机制。
	Storage Storage
	// Applied is the last applied index. It should only be set when restarting
	// raft. raft will not return entries to the application smaller or equal to
	// Applied. If Applied is unset when restarting, raft might return previous
	// applied entries. This is a very application dependent configuration.
	// Applied 是指最后应用的索引。它通常只在 Raft 重启时设置。Raft 算法不会将小于或等于 Applied 的条目返回给应用程序。
	// 如果在重启时没有设置 Applied，Raft 可能会返回之前已经应用过的条目。这个配置非常依赖于具体的应用程序。
	// 具体来说，Applied 的作用在于告诉 Raft 哪些日志条目已经被上层应用（即使用 Raft 的服务或系统）处理过。
	// 在 Raft 中，日志条目首先是被领导者（leader）复制到所有的追随者（followers）上，然后在一个安全的时刻（即这些条目被足够多的节点持久化后）被提交。
	// 提交之后，这些条目就可以被应用到上层应用中，以实现状态机的更新。
	// 当 Raft 节点重启时，它需要从存储中恢复状态，包括已提交的日志条目和最后应用的索引 Applied。如果 Applied 被正确设置，
	// Raft 会知道哪些条目已经被应用过，从而避免重复应用这些条目。如果没有设置 Applied 或者设置错误，
	// Raft 可能会错误地将已经应用过的条目再次返回给上层应用，这可能导致状态机的不一致或其他问题。
	Applied uint64
}

// validate returns an error if the config is invalid
func (c *Config) validate() error {
	if c.ID == None {
		return errors.New("cannot use none as id")
	}

	if c.HeartbeatTick <= 0 {
		return errors.New("heartbeat tick must be greater than 0")
	}

	if c.ElectionTick <= c.HeartbeatTick {
		return errors.New("election tick must be greater than heartbeat tick")
	}

	if c.Storage == nil {
		return errors.New("storage cannot be nil")
	}

	return nil
}

// Progress represents a follower’s progress in the view of the leader. Leader maintains
// progresses of all followers, and sends entries to the follower based on its progress.
// 在 Raft 一致性算法中代表领导者（leader）视角下的某个追随者（follower）的进度。
// 领导者维护着所有追随者的进度，并根据这些进度来发送日志条目（entries）给相应的追随者。
type Progress struct {
	Match, Next uint64
	// Match：表示leader已知的、已经被该follower复制并持久化到其日志中的最高日志条目的索引。
	// 换句话说，Match 索引之前的所有日志条目都已经被该追随者接受并认为是一致的。
	// Next：表示leader下一次将要发送给该follower的日志条目的索引。在leader向follower发送日志条目时，
	// 它会从 Next 索引开始发送，直到follower确认接收并持久化这些条目，然后更新 Match 和 Next 的值。
}

type Raft struct {
	id uint64

	// Term 任期
	Term uint64
	// Vote 该节点收到的投票数，通常用于领导选举时跟踪
	Vote uint64

	// the log
	RaftLog *RaftLog

	// log replication progress of each peers
	// 映射，用于跟踪每个peer（对等节点）的日志复制进度。
	Prs map[uint64]*Progress

	// this peer's role
	// 该节点的角色(leader, candidate, follower)
	State StateType

	// votes records
	// 记录投票的map
	votes map[uint64]bool

	// msgs need to send
	// 需要发送的消息
	msgs []pb.Message

	// the leader id
	Lead uint64

	// heartbeat interval, should send
	// 心跳间隔
	heartbeatTimeout int
	// baseline of election interval
	// 选举间隔
	electionTimeout int
	// number of ticks since it reached last heartbeatTimeout.
	// only leader keeps heartbeatElapsed.
	// 节点自上次发送心跳消息以来经过的时间（以tick为单位）
	// 只有leader节点会保持heartbeatElapsed
	heartbeatElapsed int
	// Ticks since it reached last electionTimeout when it is leader or candidate.
	// Number of ticks since it reached last electionTimeout or received a
	// valid message from current leader when it is a follower.
	// 自上次选举超时以来经过的时间（以tick为单位）
	// 当节点是leader或candidate时，表示自上次选举超时以来经过的时间
	// 当节点是follower时，表示自上次选举超时或者收到当前leader的有效消息以来经过的时间
	electionElapsed int

	// leadTransferee is id of the leader transfer target when its value is not zero.
	// Follow the procedure defined in section 3.10 of Raft phd thesis.
	// (https://web.stanford.edu/~ouster/cgi-bin/papers/OngaroPhD.pdf)
	// (Used in 3A leader transfer)
	// leadTransferee 是leader转移目标的id，当其值不为零时。
	leadTransferee uint64

	// Only one conf change may be pending (in the log, but not yet
	// applied) at a time. This is enforced via PendingConfIndex, which
	// is set to a value >= the log index of the latest pending
	// configuration change (if any). Config changes are only allowed to
	// be proposed if the leader's applied index is greater than this
	// value.
	// (Used in 3A conf change)
	// 在同一时间只能有一个配置更改处于挂起状态（在日志中，但尚未应用）。这是通过 PendingConfIndex 强制执行的，
	// 该值设置为大于最新挂起的配置更改（如果有的话）的日志索引。
	// 只有当leader的应用索引大于此值时，才允许提议配置更改。
	PendingConfIndex uint64

	logger Logger
}

// newRaft return a raft peer with the given config
// newRaft 返回一个具有给定配置的raft节点
func newRaft(c *Config) *Raft {
	if err := c.validate(); err != nil {
		panic(err.Error())
	}
	// Your Code Here (2A).
	return &Raft{
		id:               c.ID,
		Term:             0,
		Vote:             0,
		RaftLog:          newLog(c.Storage),
		Prs:              make(map[uint64]*Progress),
		State:            0,
		votes:            make(map[uint64]bool),
		msgs:             make([]pb.Message, 0),
		Lead:             0,
		heartbeatTimeout: c.HeartbeatTick,
		electionTimeout:  c.ElectionTick,
		heartbeatElapsed: 0,
		electionElapsed:  0,
		leadTransferee:   0,
		PendingConfIndex: 0,
	}
}

// sendAppend sends an append RPC with new entries (if any) and the
// current commit index to the given peer. Returns true if a message was sent.
// sendAppend 向指定的 peer（对等节点）发送一个 Append RPC（远程过程调用），该 RPC 包含了新的日志条目（如果有的话）以及当前的提交索引
func (r *Raft) sendAppend(to uint64) bool {
	// Your Code Here (2A).
	// todo snapshot
	if to == r.id {
		return false
	}
	pr := r.Prs[to]
	lastIndex, NextIndex := pr.Next-1, pr.Next
	lastTerm, err := r.RaftLog.Term(lastIndex)
	ents := r.RaftLog.getEntries(NextIndex, -1)
	if err != nil || len(ents) == 0 {
		return false
	}

	r.send(pb.Message{
		MsgType: pb.MessageType_MsgAppend,
		To:      to,
		From:    r.id,
		LogTerm: lastTerm,
		Index:   lastIndex,
		Entries: nil,
	})
	return true
}

// sendHeartbeat sends a heartbeat RPC to the given peer.
// sendHeartbeat 向指定的 peer（对等节点）发送一个心跳 RPC（远程过程调用）
func (r *Raft) sendHeartbeat(to uint64) {
	// Your Code Here (2A).
	r.send(pb.Message{
		MsgType: pb.MessageType_MsgHeartbeat,
		To:      to,
		Term:    r.Term,
		Commit:  r.RaftLog.committed,
	})
}

func (r *Raft) send(m pb.Message) {
	if m.From == 0 {
		m.From = r.id
	}
	r.msgs = append(r.msgs, m)
}

// tick advances the internal logical clock by a single tick.
// tick 通过一个tick来推进内部逻辑时钟
func (r *Raft) tick() {
	// Your Code Here (2A).
	r.electionElapsed++
	r.heartbeatElapsed++
	if r.State != StateLeader {
		return
	}
	if r.heartbeatElapsed >= r.heartbeatTimeout {
		r.heartbeatElapsed = 0
		if err := r.Step(pb.Message{MsgType: pb.MessageType_MsgBeat}); err != nil {
			r.logger.Debugf("error occurred during checking sending heartbeat: %v", err)
		}
	}
}

func (r *Raft) reset(term uint64) {
	if r.Term != term {
		r.Term = term
		r.Vote = None
	}
	r.Lead = None

	r.electionElapsed = 0
	r.heartbeatElapsed = 0
	//r.resetRandomizedElectionTimeout()

	//r.abortLeaderTransfer()

	r.votes = map[uint64]bool{}

	for id := range r.Prs {
		ps := &Progress{
			Match: 0,
			Next:  r.RaftLog.LastIndex() + 1,
		}
		if id == r.id {
			r.Prs[id].Match = r.RaftLog.LastIndex()
		}
		r.Prs[id] = ps
	}
}

// becomeFollower transform this peer's state to Follower
// becomeFollower 将该节点的状态转换为Follower
func (r *Raft) becomeFollower(term uint64, lead uint64) {
	// Your Code Here (2A).
	r.reset(term)
	r.Lead = lead
	r.State = StateFollower
	r.logger.Infof("%x became follower at term %d", r.id, r.Term)
}

// becomeCandidate transform this peer's state to candidate
// becomeCandidate 将该节点的状态转换为Candidate
func (r *Raft) becomeCandidate() {
	// Your Code Here (2A).
	if r.State == StateLeader {
		panic("invalid transition [leader -> candidate]")
	}
	r.reset(r.Term + 1)
	r.Vote = r.id
	r.State = StateCandidate
	r.logger.Infof("%x became candidate at term %d", r.id, r.Term)
}

// becomeLeader transform this peer's state to leader
// becomeLeader 将该节点的状态转换为Leader
func (r *Raft) becomeLeader() {
	// Your Code Here (2A).
	// NOTE: Leader should propose a noop entry on its term
	// 注意：Leader应该在其任期上提议一个空操作日志条目
	if r.State == StateFollower {
		panic("invalid transition [follower -> leader]")
	}
	r.reset(r.Term)
	r.Lead = r.id
	r.State = StateLeader
	if err := r.Step(pb.Message{
		MsgType: pb.MessageType_MsgPropose,
		To:      r.id,
		Entries: nil,
	}); err != nil {
		r.logger.Debugf("error occurred during proposing noop entry: %v", err)
	}
	r.logger.Infof("%x became leader at term %d", r.id, r.Term)
}

// Step the entrance of handle message, see `MessageType`
// on `eraftpb.proto` for what msgs should be handled
func (r *Raft) Step(m pb.Message) error {
	// Your Code Here (2A).
	switch r.State {
	case StateFollower:
		switch m.MsgType {

		}
	case StateCandidate:
		switch m.MsgType {

		}
	case StateLeader:
		switch m.MsgType {

		}
	}
	return nil
}

// handleAppendEntries handle AppendEntries RPC request
func (r *Raft) handleAppendEntries(m pb.Message) {
	// Your Code Here (2A).
}

// handleHeartbeat handle Heartbeat RPC request
func (r *Raft) handleHeartbeat(m pb.Message) {
	// Your Code Here (2A).
}

// handleSnapshot handle Snapshot RPC request
func (r *Raft) handleSnapshot(m pb.Message) {
	// Your Code Here (2C).
}

// addNode add a new node to raft group
func (r *Raft) addNode(id uint64) {
	// Your Code Here (3A).
}

// removeNode remove a node from raft group
func (r *Raft) removeNode(id uint64) {
	// Your Code Here (3A).
}

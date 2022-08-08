package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	//	"bytes"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.824/labgob"
	"6.824/labrpc"
)

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
//

const (
	Follower = iota
	Candidate
	Leader
)

// 时间关系？
const (
	ElectionTime  = 1000
	HeartbeatTime = 100
)

type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

type Entry struct{}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	state int

	// 持久性状态
	currentTerm int
	votedFor    int
	log         []Entry

	// 易失性状态
	commitIndex int
	lastApplied int

	// leader 状态
	nextIndex  []int
	matchIndex []int

	// 选举 心跳计时器
	electionTimer  *time.Timer
	heartbeatTimer *time.Timer
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	rf.mu.Lock()
	defer rf.mu.Unlock()
	var term int
	var isleader bool
	// Your code here (2A).
	term = rf.currentTerm
	isleader = rf.state == Leader

	return term, isleader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
}

//
// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
//
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

	// Your code here (2D).

	return true
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
}

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	LeaderCommit int
	Entries      []Entry
}

type AppendEntriesReply struct {
	Term    int
	Success bool
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {

	DPrintf("{peer %v} get RequestVote from {peer %v}", rf.me, args.CandidateId)
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// 在一个leader挂掉后，重新选举时，原来的follower的votedFor保留
	// 已经给同一个term的Candidated 投票后，拒绝其他投票
	if args.Term < rf.currentTerm || (args.Term == rf.currentTerm && rf.votedFor != -1 && rf.votedFor != args.CandidateId) {
		reply.Term, reply.VoteGranted = rf.currentTerm, false
		DPrintf("{peer %v} reply to {peer %v} false {Term %v} {votedFor %v currentTerm %v}", rf.me, args.CandidateId, args.Term, rf.votedFor, rf.currentTerm)
		return
	}

	if args.Term > rf.currentTerm {
		rf.currentTerm, rf.votedFor = args.Term, args.CandidateId
		rf.changState(Follower)
	}

	reply.Term, reply.VoteGranted = rf.currentTerm, true
	DPrintf("{peer %v} reply to {peer %v} true {Term %v}", rf.me, args.CandidateId, reply.Term)
	rf.electionTimer.Reset(RandomizeElectionTime())

}

func (rf *Raft) AppendEntry(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.currentTerm > args.Term {
		reply.Success, reply.Term = false, rf.currentTerm
		return
	}

	if rf.currentTerm < args.Term {
		rf.currentTerm, rf.votedFor = args.Term, -1
		rf.changState(Follower)
	}

	reply.Success, reply.Term = true, rf.currentTerm
	rf.electionTimer.Reset(RandomizeElectionTime())

}

func (rf *Raft) handlerAppendReply(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// 收到回复时 如果不是leader 丢弃
	if rf.state == Leader && rf.currentTerm == args.Term {
		if reply.Success {

		} else {
			if reply.Term > rf.currentTerm {
				rf.changState(Follower)
				rf.currentTerm, rf.votedFor = reply.Term, -1
			}
		}
	}
}

//
// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	DPrintf("{peer %v} send RequestVote to {peer %v},{Term %v}", rf.me, server, args.Term)
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) sendRequestAppend(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntry", args, reply)
	return ok
}

//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).

	return index, term, isLeader
}

//
// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
//
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

// 状态机
func (rf *Raft) changState(state int) {
	if state == rf.state {
		return
	}

	rf.state = state
	switch state {
	case Follower:
		// 停止发送心跳
		// 重置选举超时时间
		DPrintf("{peer %v} from {%v} to Follower in {Term %v}", rf.me, rf.state, rf.currentTerm)
		rf.heartbeatTimer.Stop()
		rf.electionTimer.Reset(RandomizeElectionTime())
	case Candidate:
		DPrintf("{peer %v} from {%v} to Candidate in {Term %v}", rf.me, rf.state, rf.currentTerm)
	case Leader:
		// 停止选举计时器
		// 开启心跳计时器
		DPrintf("{peer %v} from {%v} to Leader in {Term %v}", rf.me, rf.state, rf.currentTerm)
		rf.electionTimer.Stop()
		rf.heartbeatTimer.Reset(StableHeartbeatTime())
	}
}

func RandomizeElectionTime() time.Duration {
	// 随机种子
	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	t := time.Duration(ElectionTime+r.Intn(ElectionTime)) * time.Millisecond
	//DPrintf("election time %v", t)
	return t
}

func StableHeartbeatTime() time.Duration {
	return time.Duration(HeartbeatTime) * time.Millisecond
}

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {
	for rf.killed() == false {
		select {
		// 选举超时 改变状态为Candidate
		case <-rf.electionTimer.C:
			rf.mu.Lock()
			rf.changState(Candidate)
			rf.currentTerm += 1
			rf.startElection()
			rf.electionTimer.Reset(RandomizeElectionTime())
			rf.mu.Unlock()
		// 心跳超时
		case <-rf.heartbeatTimer.C:
			rf.mu.Lock()
			if rf.state == Leader {
				rf.broadCastHeartBeat()
				rf.heartbeatTimer.Reset(StableHeartbeatTime())
			}
			rf.mu.Unlock()
		}

	}
}

func (rf *Raft) getElectionArgs() *RequestVoteArgs {
	return &RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateId:  rf.me,
		LastLogIndex: -1,
		LastLogTerm:  -1,
	}
}

// 开始选举
func (rf *Raft) startElection() {
	DPrintf("{peer %v} start election,{Term %v}", rf.me, rf.currentTerm)

	args := rf.getElectionArgs()

	rf.votedFor = rf.me
	votes := 1

	for peer := range rf.peers {
		if peer == rf.me {
			continue
		}
		go func(peer int) {
			reply := new(RequestVoteReply)
			if rf.sendRequestVote(peer, args, reply) {
				rf.mu.Lock()
				defer rf.mu.Unlock()
				// 防止一些节点存在延时，在Candidate 状态改变后返回
				DPrintf("{peer %v}----{currentTerm %v, state: %v}---{term %v}", rf.me, rf.currentTerm, rf.state, args.Term)
				if rf.currentTerm == args.Term && rf.state == Candidate {
					if reply.VoteGranted {
						votes += 1
						if votes > len(rf.peers)/2 {
							rf.changState(Leader)
							rf.broadCastHeartBeat()
						}
					} else if rf.currentTerm < reply.Term {
						rf.changState(Follower)
						rf.currentTerm, rf.votedFor = reply.Term, -1
					}
				}
			}

		}(peer)
	}
}

func (rf *Raft) getAppendEntryArgs() *AppendEntriesArgs {
	entries := make([]Entry, len(rf.peers))
	return &AppendEntriesArgs{
		Term:         rf.currentTerm,
		LeaderId:     rf.me,
		PrevLogIndex: -1,
		PrevLogTerm:  -1,
		LeaderCommit: rf.commitIndex,
		Entries:      entries,
	}
}

// 广播心跳
func (rf *Raft) broadCastHeartBeat() {
	DPrintf("{peer %v} start send heartsbeats,{Term %v}", rf.me, rf.currentTerm)

	args := rf.getAppendEntryArgs()

	for peer := range rf.peers {
		if peer == rf.me {
			continue
		}

		go func(peer int) {
			reply := new(AppendEntriesReply)
			if rf.sendRequestAppend(peer, args, reply) {
				rf.handlerAppendReply(peer, args, reply)
			}
		}(peer)
	}
}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{
		peers:     peers,
		persister: persister,
		me:        me,
		dead:      0,

		currentTerm: 0,
		votedFor:    -1,
		log:         make([]Entry, 1),

		commitIndex: 0,
		lastApplied: 0,

		nextIndex:  make([]int, len(peers)),
		matchIndex: make([]int, len(peers)),

		state:          Follower,
		electionTimer:  time.NewTimer(RandomizeElectionTime()),
		heartbeatTimer: time.NewTimer(StableHeartbeatTime()),
	}

	// Your initialization code here (2A, 2B, 2C).

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	DPrintf("Init Success {id : %v}", me)
	return rf
}

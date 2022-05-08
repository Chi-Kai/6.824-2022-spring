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
	"log"
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

const (
	Follower = iota
	Candidate
	Leader

	ElectionTimeout  = 300
	HeartbeatTimeout = time.Duration(100) * time.Millisecond
)

type LogEntry struct {
	Term    int         // 日志周期
	Index   int         // 日志index
	Command interface{} // 日志内容
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// persiste state
	currentTerm int        //当前任期
	votedFor    int        // 投票对象
	logs        []LogEntry // 日志

	// volatile state

	commitIndex int //最新提交index
	lastApplied int // 最新应用到的节点

	nextIndex  []int // 每个节点的下一个日志index
	matchIndex []int // 每个节点最新的日志index

	role   int // 节点角色
	lenLog int // 日志长度

	electionTimer   *time.Timer //选举计时器
	heartsbeatTimer *time.Timer //心跳计时器

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
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
	Term         int //候选人任期
	CandidateId  int //拉票的候选人id
	LastLogIndex int //候选人最新的log index
	LastLogTerm  int // 候选人最新的log term
	// Your data here (2A, 2B).
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	Term        int  //返回自己的term
	VoteGranted bool //是否投票
	// Your data here (2A).
}

//
// example RequestVote RPC handler.
//处理拉票请求
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// 如果拉票的任期小于自己的任期或者 已经投票，则直接返回
	if args.Term < rf.currentTerm || rf.votedFor != -1 {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	}

	// 对于所有peer ，如果收到大于自己任期的rpc 则转换为Follower

	// 如果拉票的任期大于自己的任期，则更新自己的任期,
	// 转换为Follower
	//并且投票给候选人,
	//重置自己的electionTimer
	if args.Term > rf.currentTerm {

		rf.currentTerm = args.Term
		rf.votedFor = args.CandidateId
		rf.changeRoleWithLock(Follower)

		reply.Term = args.Term
		reply.VoteGranted = true

		rf.electionTimer.Reset(randomElectionTimeout())
		log.Printf("%d 投票给 %d,重置选举时间", rf.me, args.CandidateId)
		return
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
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

// appendEntries RPC arguments structure.

type AppendEntryRequest struct {
	Term         int        // Leader 的任期
	LeaderId     int        // Leader 的id
	PreLogIndex  int        // Leader 的最新的log index
	PreLogTerm   int        // Leader 的最新的log term
	Entries      []LogEntry // 要存储的新的log entries
	LeaderCommit int        // Leader 的commit index
}

// appendEntries RPC reply structure.

type AppendEntryReply struct {
	Term    int  // follower 的任期
	Success bool // 是否成功
}

// appendEntries RPC handler.
// 心跳处理
func (rf *Raft) AppendEntry(args *AppendEntryRequest, reply *AppendEntryReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// 如果领导人任期小于接受者任期 返回flase
	if rf.currentTerm > args.Term {
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}

	// 如果任期大于等于接受者

	// 如果收到心跳 转变为Follower
	if args.Entries == nil {
		log.Printf("%d 收到心跳", rf.me)
		rf.changeRoleWithLock(Follower)
		reply.Term = rf.currentTerm
		reply.Success = true
	}

	//重置选举时间
	rf.electionTimer.Reset(randomElectionTimeout())
	return
}

// send AppendEntry RPC
func (rf *Raft) sendAppendEntry(server int, args *AppendEntryRequest, reply *AppendEntryReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntry", args, reply)
	return ok
}

// 角色切换 以及一些绑定的操作
// 变为Candidate 时，要增加任期，投票给自己，重置选举时间
// 变为Leader 时，要马上广播心跳
func (rf *Raft) changeRoleWithLock(role int) {
	switch role {
	case Follower:
		rf.role = Follower
		log.Printf("%d 转换为Follower", rf.me)
	case Candidate:
		rf.role = Candidate
		rf.currentTerm++
		rf.votedFor = rf.me
		rf.electionTimer.Reset(randomElectionTimeout())
	case Leader:
		rf.role = Leader
		log.Printf("%d 转换为Leader", rf.me)
		rf.BroadcastHeartbeat()
	}
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

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.

// 两个计时器，一个是心跳计时器，一个是选举计时器

func (rf *Raft) ticker() {
	for rf.killed() == false {
		select {
		case <-rf.electionTimer.C:
			log.Printf("%d 选举时间到", rf.me)
			rf.mu.Lock()
			rf.changeRoleWithLock(Candidate)
			rf.mu.Unlock()
			rf.startElection()
		case <-rf.heartsbeatTimer.C:
			rf.mu.Lock()
			if rf.role == Leader {
				rf.BroadcastHeartbeat()
				//rf.heartsbeatTimer.Reset(HeartbeatTimeout)
			}
			rf.mu.Unlock()
		}

	}
}

// 随机时间，用于选举计时器
func randomElectionTimeout() time.Duration {
	rand.Seed(time.Now().UnixNano()) // 随机种子
	return time.Duration(rand.Intn(ElectionTimeout)+ElectionTimeout) * time.Millisecond
}

// 开始选举
func (rf *Raft) startElection() {
	// 发送选举请求
	log.Printf("%d 开始选举", rf.me)
	countmu := sync.Mutex{}
	voteCount := 1

	for i := range rf.peers {
		if i != rf.me {
			go func(server int) {

				rf.mu.Lock()
				defer rf.mu.Unlock()

				args := &RequestVoteArgs{
					Term:         rf.currentTerm,
					CandidateId:  rf.me,
					LastLogIndex: rf.getLastLogIndex(),
					LastLogTerm:  rf.getLastLogTerm(),
				}
				reply := &RequestVoteReply{}
				// 如果获得响应 同时此时还是Candidate 角色 (防止过期回应的干扰)
				if rf.sendRequestVote(server, args, reply) && rf.role == Candidate {
					// 如果返回的term比自己大，则更新自己的term,变回Follower
					if reply.Term > rf.currentTerm {
						rf.changeRoleWithLock(Follower)
						rf.currentTerm = reply.Term
						rf.votedFor = -1
						return
					}
					// 如果得到的响应是true，则投票数+1
					if reply.VoteGranted {
						countmu.Lock()
						voteCount++
						countmu.Unlock()
					}
					// 如果投票数超过半数，则成为leader,并发送心跳
					if voteCount > rf.lenLog/2 {
						rf.changeRoleWithLock(Leader)
						return
					}

				}
			}(i)
		}
	}
}

// 发送心跳
func (rf *Raft) BroadcastHeartbeat() {
	log.Printf("%d 节点开始广播心跳", rf.me)
	for i := range rf.peers {
		if i != rf.me {
			go func(server int) {
				args := &AppendEntryRequest{
					Term:         rf.currentTerm,
					LeaderId:     rf.me,
					PreLogIndex:  rf.getLastLogIndex(),
					PreLogTerm:   rf.getLastLogTerm(),
					Entries:      nil,
					LeaderCommit: rf.commitIndex,
				}
				reply := &AppendEntryReply{}
				rf.sendAppendEntry(server, args, reply)
			}(i)
		}
	}
}

// 得到最新日志的索引
func (rf *Raft) getLastLogIndex() int {
	if len(rf.logs) == 0 {
		return 0
	}
	return len(rf.logs) - 1
}

// 得到最新日志的Term
func (rf *Raft) getLastLogTerm() int {
	if len(rf.logs) == 0 {
		return rf.currentTerm
	}
	return rf.logs[len(rf.logs)-1].Term
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
		mu:        sync.Mutex{},
		peers:     peers,
		persister: persister,
		me:        me,
		dead:      0,

		currentTerm: 0,
		votedFor:    -1,
		logs:        make([]LogEntry, 0),

		role: Follower,

		heartsbeatTimer: time.NewTimer(HeartbeatTimeout),
		electionTimer:   time.NewTimer(randomElectionTimeout()),
	}
	rf.nextIndex = make([]int, rf.lenLog)
	rf.matchIndex = make([]int, rf.lenLog)

	// Your initialization code here (2A, 2B, 2C).

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	log.Printf("创建节点 %d", rf.me)

	return rf
}

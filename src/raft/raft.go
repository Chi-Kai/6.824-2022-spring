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

// state name of the server
//0:follower, 1:candidate, 2:leader
const (
	Follower = iota
	Candidate
	Leader
)

// log
// 日志struct
type LogEntry struct {
	// 日志任期
	Term int
	// 日志索引
	Index int
	// 日志内容
	Command interface{}
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
	state     int                 // 0:follower, 1:candidate, 2:leader

	// persistent state for all state
	currentTerm int        // 当前任期
	votedFor    int        // 当前选举的leader
	log         []LogEntry // 日志

	//volatile state on all servers
	commitIndex int // 已经提交的日志的最大索引
	lastApplied int // 已经应用的日志的最大索引

	// volatile state on leaders
	nextIndex  []int // 对于每个节点，记录下一个待提交的日志索引
	matchIndex []int // 对于每个节点，记录下一个已经提交的日志索引

	// 超时时间
	// Leader 是心跳超时时间
	// Follower 和 Candidate 是选举超时时间
	timeout time.Duration
	// timenum 重置超时时间的随机范围
	timenum int
	// ticker 函数定时发送心跳给通道
	tickc chan struct{}
	// 相应通道 用来重置超时时间
	done chan struct{}
	// 每个state对应不同的ticker处理函数
	// 在leader状态下，每隔一段时间，发送心跳给其他节点
	// 在follower状态下，每隔一段时间，检查是否心跳超时,如果超时，则变为candidate状态
	// 在candidate状态下，每隔一段时间，检查自己是否被选举为leader
	tickerHandler func()
	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

}

// return currentTerm and whether this server
// believes it is the leader.
// 当前任期和是否是leader
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	rf.mu.Lock()
	defer rf.mu.Unlock()
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
// Raft 通过比较两份日志中最后一条日志条目的索引值和任期号定义谁的日志比较新。
// 如果两份日志最后的条目的任期号不同，那么任期号大的日志更加新。
// 如果两份日志最后的条目任期号相同，那么日志比较长的那个就更加新。
// 投票人会拒绝掉那些日志没有自己新的投票请求
type RequestVoteArgs struct {
	Term         int // candidate's term
	CandidateId  int // candidate requesting vote
	LastLogIndex int // index of candidate's last log entry
	LastLogTerm  int // term of candidate's last log entry
	// Your data here (2A, 2B).
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	Term        int  // currentTerm, for candidate to update itself
	VoteGranted bool // 候选人是否收到投票
	// Your data here (2A).
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// 处理投票请求
	// 读取当前节点的状态
	rf.mu.Lock()
	term := rf.currentTerm
	votedFor := rf.votedFor
	lastLogIndex := rf.getLastLogIndex()
	lastLogTerm := rf.getLastLogTerm()
	rf.mu.Unlock()

	// 如果已经投票，则直接返回
	if votedFor != -1 {
		reply.Term = term
		reply.VoteGranted = false
		return
	}

	// 如果候选人的任期号小于当前节点的任期号，则拒绝投票
	if args.Term < term {
		reply.Term = term
		reply.VoteGranted = false
		return
	}

	// 比较投票人的日志条目的索引值和任期号
	// 如果候选人的日志term < 当前节点的日志term，则拒绝投票
	// 如果候选人的日志term > 当前节点的日志term，投票同时更新状态
	// 如果候选人的日志term == 当前节点的日志term，比较候选人的日志条目的索引值
	if args.LastLogTerm < lastLogTerm {
		reply.Term = term
		reply.VoteGranted = false
	} else if args.LastLogTerm > lastLogTerm {
		reply.Term = args.Term
		reply.VoteGranted = true
		// 更新Follower的状态
		rf.becomeFollower(args.Term, args.CandidateId)
		// 重置心跳时间
		rf.done <- struct{}{}
	} else {
		if args.LastLogIndex < lastLogIndex {
			reply.Term = term
			reply.VoteGranted = false
			return
		} else {
			reply.Term = term
			reply.VoteGranted = true
			rf.becomeFollower(args.Term, args.CandidateId)
			// 重置心跳时间
			rf.done <- struct{}{}

		}
	}
	return
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

// AppendEntries args
type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

// AppendEntries reply
type AppendEntriesReply struct {
	Term    int
	Success bool
}

// AppendEntries RPC handler
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	// 读取一下当前的状态

	rf.mu.Lock()
	state := rf.state
	term := rf.currentTerm
	rf.mu.Unlock()

	// 对于所有角色 term > args.Term 则拒绝
	if term > args.Term {
		reply.Term = term
		reply.Success = false
		return
	}

	// Follower 回复当前的term
	if state == Follower {
		// 如果entries为空，那么就是心跳包,重置选举计时器
		if args.Entries == nil {
			log.Printf("%d 收到心跳", rf.me)
			reply.Term = term
			reply.Success = true
			// 发送包 重置时间
			rf.done <- struct{}{}
			return
		}

		// 如果entries不为空，那么就是普通的 AppendEntries

	}

	// Candidate / Leader 回复当前的term
	// 如果收到的term比自己的小，那么就回复自己的term 并拒绝
	// 如果收到的term大于等于自己的term，那么就更新自己的term 并且转换为Follower

	if state == Candidate || state == Leader {
		reply.Term = args.Term
		reply.Success = true
		rf.becomeFollower(args.Term, -1)
		rf.resetTimer()
		return
	}
}

// 发送AppendEntries
func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
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

// guide 为啥说用sleep 计时? 觉得没办法重置时间

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {

	for rf.killed() == false {

		// 读取一下当前的状态
		rf.mu.Lock()
		t := rf.timeout
		state := rf.state
		rf.mu.Unlock()
		a := time.After(t)
		select {
		// 超时，开始执行tickerHandler
		// 此时, Leader 发送心跳给其他节点
		// Follower 变成 Candidate 开始选举
		// Candidate 开始新的选举
		case <-a:
			if state == Leader {
				log.Printf("%d 发送心跳", rf.me)
			} else if state == Follower {
				log.Printf("%d 心跳超时 %d,转换为Candidate", rf.me, t)
			} else {
				log.Printf("%d 选举超时,开始新一轮选举", rf.me)
			}
			rf.tickerHandler()
		// 收到心跳，更新时间戳
		case <-rf.done:
			rf.resetTimer()
		}
	}
}

// 重置定时器
// 既可以用来重置选举时间，也可以用来重置心跳时间,根据参数不同
func (rf *Raft) resetTimer() {
	rf.mu.Lock()

	rf.timeout = time.Duration(rand.Intn(rf.timenum)+rf.timenum) * time.Millisecond
	rf.mu.Unlock()
	if rf.state == Candidate {
		log.Printf("%d 任期为%d 重置选举超时时间 为 %d", rf.me, rf.currentTerm, rf.timeout)
	} else if rf.state == Follower {
		log.Printf("%d 任期为%d 重置心跳超时时间 为 %d", rf.me, rf.currentTerm, rf.timeout)
	}
}

// 选举leader
// 发起选举
// 先将自己的状态变为 candidate
// 再将term++ 和 voteFor = 自己的in
// 再发送消息给其他节点
func (rf *Raft) startElection() {
	rf.becomeCandidate()
	rf.resetTimer()
	// 发送消息给其他节点
	log.Printf("%d 开始竞选", rf.me)
	args := &RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateId:  rf.me,
		LastLogIndex: rf.getLastLogIndex(),
		LastLogTerm:  rf.getLastLogTerm(),
	}
	// 统计得票数
	voteCount := 1
	// 保护锁
	countmu := sync.Mutex{}

	for i, _ := range rf.peers {
		if i != rf.me {
			// 发送消息给其他节点
			go func(server int) {
				reply := &RequestVoteReply{}
				rf.sendRequestVote(server, args, reply)

				// 读取状态信息
				rf.mu.Lock()
				state := rf.state
				numpeers := len(rf.peers)
				rf.mu.Unlock()
				// 先判断是否还是candidate
				if state != Candidate {
					return
				}

				countmu.Lock()
				// 如果此时已经收到半数的消息，则变成leader
				if voteCount > numpeers/2 {
					rf.becomeLeader()
					rf.sendHeartbeat()
					countmu.Unlock()
					return
				}
				if reply.VoteGranted {
					voteCount++
				}
				countmu.Unlock()
			}(i)
		}
	}
}

// 得到最后一条日志的索引
func (rf *Raft) getLastLogIndex() int {
	if len(rf.log) == 0 {
		return 0
	}
	return rf.log[len(rf.log)-1].Index
}

// 得到最后一条日志的Term
func (rf *Raft) getLastLogTerm() int {
	if len(rf.log) == 0 {
		return rf.currentTerm
	}
	return rf.log[len(rf.log)-1].Term
}

// prs

// 发送心跳
func (rf *Raft) sendHeartbeat() {
	args := &AppendEntriesArgs{
		Term:         rf.currentTerm,
		LeaderId:     rf.me,
		PrevLogIndex: rf.getLastLogIndex(),
		PrevLogTerm:  rf.getLastLogTerm(),
		// 发送空的日志条目作为心跳
		Entries:      []LogEntry{},
		LeaderCommit: rf.commitIndex,
	}

	for i, _ := range rf.peers {
		if i != rf.me {
			go rf.sendAppendEntries(i, args, &AppendEntriesReply{})
		}
	}

}

// 变成Candidate
// 转变state为candidate
// 为自己投票
// 任期号加1
// 改变tickerHandler 为 发起新的选举
// 改变timeout 为 新的选举超时时间
// 重置定时器
// 改变tickerHandler 为 startElection
func (rf *Raft) becomeCandidate() {
	rf.mu.Lock()
	rf.state = Candidate
	rf.votedFor = rf.me
	rf.currentTerm++
	rf.tickerHandler = rf.startElection
	rf.mu.Unlock()
	log.Printf("%d 节点转变为Candidate", rf.me)
}

// 变成follower
// 转变state为Follower
// 更新自己的任期号
// 清空votedFor
// 改变tickerHandler 为 发起新的选举
// 改变timenum 为 选举超时时间
// 重置定时器
func (rf *Raft) becomeFollower(term int, voted int) {
	rf.mu.Lock()
	rf.state = Follower
	rf.currentTerm = term
	rf.votedFor = voted
	rf.timenum = 200
	rf.tickerHandler = rf.startElection
	rf.mu.Unlock()
	log.Printf("%d 节点转变为Follower", rf.me)
}

// 变成leader
// 转变state为Leader
// votedFor改为自己
// 更新日志
// 改变tickerHandler 为 发送心跳
// 改变timenum 为 心跳超时时间
// 重置定时器 (心跳定时器)
func (rf *Raft) becomeLeader() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.state = Leader
	rf.votedFor = rf.me
	rf.tickerHandler = rf.sendHeartbeat
	// 每秒发送10次左右
	rf.timeout = time.Duration(100) * time.Millisecond
	log.Printf("%d 节点转变为Leader", rf.me)
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

	// 初始化
	rf := &Raft{
		mu:        sync.Mutex{},
		peers:     peers,
		persister: persister,
		me:        me,

		log:           []LogEntry{},
		commitIndex:   0,
		lastApplied:   0,
		nextIndex:     make([]int, len(peers)),
		matchIndex:    make([]int, len(peers)),
		tickerHandler: nil,
		done:          make(chan struct{}),
	}
	// 转变为Follower 任期为1 投票为-1
	rf.becomeFollower(1, -1)
	// 初始化心跳定时器
	rf.resetTimer()

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	log.Printf("创建节点%d成功", me)

	return rf
}

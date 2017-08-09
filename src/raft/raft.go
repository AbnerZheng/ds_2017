package raft

//
// this is an outline of the API that rf must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new logs entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the logs, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import "sync"
import (
	"labrpc"
	"time"
	"math/rand"
	"fmt"
	"log"
	"bytes"
	"encoding/gob"
)

const ELECTION_TIMEOUT_MIN = 500
const ELECTION_TIMEOUT_RANGE = 500

const HEARTBEAT_INTERVAL = time.Duration(125) * time.Millisecond

// import "bytes"
// import "encoding/gob"

//
// as each Raft peer becomes aware that successive logs entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make().
//
type ApplyMsg struct {
	Index       int
	Command     interface{}
	UseSnapshot bool   // ignore for lab2; only used in lab3
	Snapshot    []byte // ignore for lab2; only used in lab3
}

type State int8

const (
	FOLLOWER  State = iota + 1
	LEADER
	CANDIDATE
)

type Log struct {
	Command interface{}
	Term    int
	Index   int // 可能因为checkpoint的存在而可以缩减日志
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	// persistent state on all servers
	currentTerm int // latest term server has seen (initialized to 0 on first boot, increases monotonically
	voteFor     int // 投票给谁， 不投的时候选择-1
	voteCount   int // 拿到票数的数量
	appendCount int
	logs        []Log

	// volatile state on all servers
	commitIndex int // index of highest log entry known to be committed (initialized to 0, increases monotonically)
	lastApplied int // index of highest log entry apply to state machine (initialized to 0 , increases monotonically)

	// volatile state on leaders (Reinitialized after election)
	nextIndex  []int // for each server, index of highest log entry to send to that server
	matchIndex []int // for each server, index of highest log entry known to be replicated on server

	state            State
	htChan           chan bool // 检验是否收到heart beat
	rvChan           chan bool // 检验是否收到requestVote
	cmChan           chan bool // 检验是否要apply command
	applyCh          chan ApplyMsg
	becomeLeaderChan chan bool

	died      bool // 用于调试
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	var term int
	var isleader bool
	term = rf.currentTerm
	isleader = rf.state == LEADER
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
	w := new(bytes.Buffer)
	e := gob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.voteFor)
	e.Encode(rf.logs)
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	// Your code here (2C).
	// Example:
	r := bytes.NewBuffer(data)
	d := gob.NewDecoder(r)
	d.Decode(&rf.currentTerm)
	d.Decode(&rf.voteFor)
	d.Decode(&rf.logs)
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
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
	Term         int // leader’s term
	LeaderId     int // so follower can redirect clients
	PrevLogIndex int // index of log entry immediately preceding new ones

	PrevLogTerm  int   // term of prevLogIndex entry
	Entries      []Log // log entries to store (empty for heartbeat; may send more than one for efficiency)
	LeaderCommit int   // leader’s commitIndex
}

type AppendEntriesReply struct {
	Term    int
	Success bool
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()
	reply.Success = true
	reply.Term = rf.currentTerm

	rf.Println("prevLogIndex:%d, prevLogTerm:%d entries: %s", args.PrevLogIndex, args.PrevLogTerm, args.Entries)
	if args.Term < rf.currentTerm {
		reply.Success = false
		return
	}

	if args.Term > rf.currentTerm { // rule for all servers: if RPC request or response contain term T > currentTerm
		rf.currentTerm = args.Term // set currentTerm = T
		rf.state = FOLLOWER        // convert to follower
		rf.voteFor = -1
	}

	rf.htChan <- true

	if args.PrevLogIndex > rf.lastLogIndex() {
		rf.Println("append false")
		reply.Success = false
		return
	}

	baseIndex := rf.logs[0].Index
	if args.PrevLogIndex > baseIndex {
		term := rf.logs[args.PrevLogIndex-baseIndex].Term
		if args.PrevLogTerm != term {
			reply.Success = false
			return
		}
	} else if args.PrevLogIndex < baseIndex {

	}
	if args.PrevLogIndex < rf.lastLogIndex() {
		rf.logs = rf.logs[0: args.PrevLogIndex-baseIndex+1]
	}

	if len(args.Entries) > 0 {
		rf.logs = append(rf.logs, args.Entries...)
	}

	rf.Println("append log, now is %s", rf.logs)
	reply.Success = true
	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = min(args.LeaderCommit, rf.lastLogIndex())
		rf.cmChan <- true
	}
	return
}

func min(x, y int) int {
	if x < y {
		return x
	}
	return y
}

//
// example RequestVote RPC handler.
// 在无leader的情况下， 各个candidate向其他node发送该RPC以进行竞选leader
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()
	reply.Term = rf.currentTerm
	if args.Term < rf.currentTerm { // 如果自己的term比过来拉票的server还高的话，直接否决
		reply.VoteGranted = false // figure 2
		return
	}
	if args.Term > rf.currentTerm { // $5.1的基本要求
		rf.voteFor = -1 // 置为还未投票
		rf.state = FOLLOWER
		rf.currentTerm = args.Term
	}

	if rf.voteFor == -1 || rf.voteFor == args.CandidateId { // 还未投票或者上次就把票投给该candidate
		if rf.lastLogTerm() < args.LastLogTerm {
			reply.VoteGranted = true
		} else if rf.lastLogTerm() == args.LastLogTerm && rf.lastLogIndex() <= args.LastLogIndex {
			reply.VoteGranted = true
		} else {
			reply.VoteGranted = false
		}
	}
	if reply.VoteGranted {
		rf.state = FOLLOWER
		rf.voteFor = args.CandidateId
		rf.rvChan <- true
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

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's logs. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft logs, since the leader
// may fail or lose an election.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.Println("receive request from application.")
	log_index := rf.lastLogIndex() + 1
	term := rf.currentTerm
	// Your code here (2B).
	isLeader := false
	if rf.state == LEADER {
		isLeader = true
		log := Log{command, term, log_index}
		rf.logs = append(rf.logs, log)
	}
	return log_index, term, isLeader
}

func (rf *Raft) lastLogIndex() int {
	len2 := len(rf.logs)
	if len2 == 0 {
		return -1
	}
	return rf.logs[len2-1].Index
}

func (rf *Raft) lastLogTerm() int {
	len2 := len(rf.logs)
	if len2 == 0 {
		return -1
	}
	return rf.logs[len2-1].Term
}

//
// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (rf *Raft) Kill() {
	rf.died = true
	// Your code here, if desired.
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
//
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.commitIndex = -1
	rf.me = me
	rf.applyCh = applyCh

	// Your initialization code here (2A, 2B, 2C).

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	if len(rf.logs) < 1{
		rf.logs = append(rf.logs, Log{Term: 0}) // 占位符，减少判断
	}
	rf.state = FOLLOWER
	rf.htChan = make(chan bool, 10)
	rf.rvChan = make(chan bool)
	rf.cmChan = make(chan bool)
	rf.becomeLeaderChan = make(chan bool)

	go rf.StateMachine()
	go rf.ApplyCommand()
	return rf
}
func (rf *Raft) ApplyCommand() {
	for {
		select {
		case <-rf.cmChan:
			rf.Println("apply command")
			rf.mu.Lock()
			commitIndex := rf.commitIndex
			baseIndex := rf.logs[0].Index
			for i := rf.lastApplied + 1; i <= commitIndex; i++ {
				msg := ApplyMsg{Index: i, Command: rf.logs[i-baseIndex].Command}
				rf.applyCh <- msg
				rf.lastApplied = i
			}
			rf.mu.Unlock()
		}
	}
}

func (rf *Raft) Println(format string, a ...interface{}) {
	if (Debug <= 0 || rf.died) {
		return
	}
	t := fmt.Sprintf("current term is %d, I'm %d:", rf.currentTerm, rf.me)
	format = t + format + "\n"
	fmt.Printf(format, a...)
}

func (rf *Raft) broadcastRequestVotes() {
	rf.mu.Lock()
	args := RequestVoteArgs{rf.currentTerm, rf.me, rf.lastLogIndex(), rf.lastLogTerm()}
	rf.mu.Unlock()

	for i := range rf.peers {
		if rf.me != i && rf.state == CANDIDATE {
			go func(index int) {
				rf.Println("ask vote from %d", index)
				var res RequestVoteReply
				ok := rf.sendRequestVote(index, &args, &res)
				rf.Println("ask vote from %d, result is %t", index, res.VoteGranted)
				if !ok {
					rf.Println("rpc error send to %d", index)
				}
				rf.mu.Lock()
				defer rf.mu.Unlock()
				if rf.state != CANDIDATE || args.Term != rf.currentTerm {
					return
				}
				if res.VoteGranted {
					rf.voteCount ++
					if rf.voteCount*2 > len(rf.peers) && rf.state == CANDIDATE {
						rf.state = LEADER
						rf.becomeLeaderChan <- true
					}
				} else if res.Term > rf.currentTerm {
					rf.currentTerm = res.Term
					rf.state = FOLLOWER
					rf.voteFor = -1
					defer rf.persist()
				}
			}(i)
		}
	}
}

func (rf *Raft) broadcastAppendEntries() {
	rf.Println("start to broadcast append")
	prevLogIndex := rf.lastLogIndex()
	N := rf.commitIndex

	for i := rf.commitIndex + 1; i <= prevLogIndex; i++ {
		num := 1
		for j := range rf.peers {
			if j != rf.me && rf.matchIndex[j] >= i && rf.logs[i-rf.logs[0].Index].Term == rf.currentTerm {
				num ++
			}
			if 2*num > len(rf.peers) {
				N = i
			}
		}
	}
	if N != rf.commitIndex {
		rf.Println("dead lock hear")
		rf.mu.Lock()
		rf.commitIndex = N
		rf.mu.Unlock()
		rf.cmChan <- true
	}
	baseIndex := rf.logs[0].Index

	for i := range rf.peers {
		if rf.me != i && rf.state == LEADER {
			rf.mu.Lock()
			args := AppendEntriesArgs{
				rf.currentTerm,
				rf.me,
				rf.nextIndex[i] - 1,
				rf.logs[rf.nextIndex[i]-1-baseIndex].Term,
				nil, rf.commitIndex}

			args.Entries = make([]Log, len(rf.logs[rf.nextIndex[i]-baseIndex:]))
			copy(args.Entries, rf.logs[rf.nextIndex[i]-baseIndex:])
			rf.mu.Unlock()
			rf.Println("heart beats sent to %d", i)
			go func(index int, entriesArgs AppendEntriesArgs) {
				res := AppendEntriesReply{}
				rf.Println("to %d is: %s", index, entriesArgs)
				ok := rf.sendAppendEntries(index, &entriesArgs, &res)
				if !ok {
					rf.Println("heart beats rpc error send to %d", index)
					return
				}
				if !res.Success && res.Term > rf.currentTerm {
					rf.Println("change from leader to follow")
					rf.mu.Lock()
					rf.currentTerm = res.Term
					rf.state = FOLLOWER
					rf.mu.Unlock()
					defer rf.persist()
				} else if res.Success && len(entriesArgs.Entries) > 0 {
					temp := entriesArgs.Entries[len(entriesArgs.Entries)-1]
					rf.mu.Lock()
					rf.nextIndex[index] = temp.Index + 1
					rf.matchIndex[index] = temp.Index
					rf.mu.Unlock()
					rf.Println("now nextIndex is %s", rf.nextIndex)
				} else if !res.Success {
					rf.mu.Lock()
					rf.nextIndex[index] = entriesArgs.PrevLogIndex
					rf.mu.Unlock()
				}
			}(i, args)
		}
	}
}

func (rf *Raft) StateMachine() {
	for {
		switch rf.state {
		case FOLLOWER:
			rf.Println("enter follow")
			select {
			case _ = <-rf.htChan: // 收到, 不处理就好
				rf.Println("receive heartbeat")
			case <-rf.rvChan:
				rf.Println("I have voted %d", rf.voteFor)
			case <-time.After(time.Duration(ELECTION_TIMEOUT_MIN+rand.Intn(ELECTION_TIMEOUT_RANGE)) * time.Millisecond):
				rf.state = CANDIDATE
			}
		case CANDIDATE:
			rf.mu.Lock()
			rf.currentTerm ++  // increment currentTerm
			rf.voteFor = rf.me // vote for self
			rf.voteCount = 1
			rf.persist()
			rf.mu.Unlock()
			rf.Println("enter candidate")
			//$5.1: issues requestVotes in parallel
			go rf.broadcastRequestVotes()

			// A candidate continue in this state until one of three thing happens:
			select {
			case <-rf.becomeLeaderChan:
				// a: it wins the election
				rf.Println("I'm the leader")
				rf.mu.Lock()
				rf.state = LEADER
				rf.nextIndex = make([]int, len(rf.peers))
				rf.matchIndex = make([]int, len(rf.peers))
				for i := range rf.peers {
					// figure 2
					rf.nextIndex[i] = rf.lastLogIndex() + 1 // initialized to leader last log index + 1
					rf.matchIndex[i] = 0                    // initialized to 0
				}
				rf.mu.Unlock()
			case <-rf.htChan:
				// b: another server establishes itself as leader
				rf.Println("receive vote from others, change from candidate to follower")
				rf.mu.Lock()
				rf.state = FOLLOWER
				rf.mu.Unlock()
			case <-time.After(time.Duration(ELECTION_TIMEOUT_MIN+rand.Intn(ELECTION_TIMEOUT_RANGE)) * time.Millisecond):
				rf.Println("time over, become candidate next time")
			}
		case LEADER:
			rf.Println("enter leader, start heartbeat")
			rf.broadcastAppendEntries()
			time.Sleep(HEARTBEAT_INTERVAL)
		default:
			log.Fatal("wrong")
		}

	}
}

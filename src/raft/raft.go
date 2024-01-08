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
	"bytes"
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.824/labgob"
	"6.824/labgob"
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
	heartBeatTimeOut     = time.Millisecond * 100
	electionBaseTimeOut  = 400
	electionTimeOutRange = 600
)

//
// A Go object implementing a single Raft peer.
//
const (
	follower  = 1
	candidate = 2
	leader    = 3
)

type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()
	state     int

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	electionTime time.Time
	receiveVote  int
	// persistent state on all servers
	currentTerm int
	voteFor     int
	logs        []LogEntry

	// 2D also persistent state on all servers
	lastIncludeIndex int
	lastIncludedTerm int
	logsStartIndex   int

	// 2D
	// snapShot []byte

	applyCh chan ApplyMsg

	// volatile state on all servers
	commitIndex int
	lastApplied int
	// volatile state on leaders
	nextIndex  []int
	matchIndex []int
}

type LogEntry struct {
	Term    int
	Command interface{}
}

func (rf *Raft) resetElectionTime() {
	// need to acquire the lock before the func
	rand.Seed(time.Now().UnixNano())
	randomNumber := rand.Intn(electionTimeOutRange) + electionBaseTimeOut
	rf.electionTime = time.Now().Add(time.Duration(randomNumber) * time.Millisecond)
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	term = rf.currentTerm
	isleader = (rf.state == leader)
	return term, isleader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.voteFor)
	e.Encode(rf.logs)
	e.Encode(rf.lastIncludeIndex)
	e.Encode(rf.lastIncludedTerm)
	e.Encode(rf.logsStartIndex)
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var currentTerm int
	var voteFor int
	var logs []LogEntry
	var lastIncludeIndex int
	var lastIncludedTerm int
	var logsStartIndex int
	if d.Decode(&currentTerm) != nil ||
		d.Decode(&voteFor) != nil ||
		d.Decode(&logs) != nil ||
		d.Decode(&lastIncludeIndex) != nil ||
		d.Decode(&lastIncludedTerm) != nil ||
		d.Decode(&logsStartIndex) != nil {
		fmt.Printf("readPersist decode error\n")
	} else {
		rf.currentTerm = currentTerm
		rf.voteFor = voteFor
		rf.logs = logs
		rf.lastIncludeIndex = lastIncludeIndex
		rf.lastIncludedTerm = lastIncludedTerm
		rf.logsStartIndex = logsStartIndex
	}
	DPrintf("raft server %d reboot from persistence at term %d\n", rf.me, rf.currentTerm)
	DPrintf("raft server %d read voteFor %d lastIncludeIndex %d lastIncludeTerm %d logsStartIndex %d\n", rf.me, voteFor, lastIncludeIndex, lastIncludedTerm, logsStartIndex)
}

//
// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
//
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludeIndex int, snapshot []byte) bool {

	// Your code here (2D).

	return true
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	DPrintf("raft server %d call Snapshot at index %d with len %d logs %v\n", rf.me, index, len(rf.logs), rf.logs)
	if index < rf.logsStartIndex || index > rf.logsStartIndex+len(rf.logs)-1 {
		DPrintf("raft server %d call Snapshot index error\n", rf.me)
		return
	}
	rf.lastIncludeIndex = index
	rf.lastIncludedTerm = rf.logs[index-rf.logsStartIndex].Term
	rf.logs = rf.logs[index+1-rf.logsStartIndex:]
	rf.logsStartIndex = index + 1
	// rf.snapShot = snapshot
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.voteFor)
	e.Encode(rf.logs)
	e.Encode(rf.lastIncludeIndex)
	e.Encode(rf.lastIncludedTerm)
	e.Encode(rf.logsStartIndex)
	state := w.Bytes()

	rf.persister.SaveStateAndSnapshot(state, snapshot)
	DPrintf("raft server %d finish call snapshot at index %d with len %d new logs %v\n", rf.me, index, len(rf.logs), rf.logs)
}

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}
type AppendEntriesReply struct {
	Term    int
	Success bool
	// ConflictTerm  int
	// ConflictIndex int
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if args.Term < rf.currentTerm {
		reply.Success = false
		reply.Term = rf.currentTerm
		return
	} else {
		if args.Term > rf.currentTerm {
			rf.currentTerm = args.Term
			rf.state = follower
			rf.voteFor = -1
			rf.persist()
		}
		if rf.state == leader {
			reply.Success = false
			reply.Term = rf.currentTerm
			return
		}
		var lastLogIndex int
		var prevLogTerm int
		lastLogIndex = rf.logsStartIndex + len(rf.logs) - 1
		if lastLogIndex >= args.PrevLogIndex {
			if args.PrevLogIndex-rf.logsStartIndex < 0 && args.PrevLogIndex == rf.lastIncludeIndex {
				prevLogTerm = rf.lastIncludedTerm
			} else {
				if args.PrevLogIndex-rf.logsStartIndex >= 0 {
					prevLogTerm = rf.logs[args.PrevLogIndex-rf.logsStartIndex].Term
				} else {
					prevLogTerm = -1
				}
			}
		}
		if lastLogIndex < args.PrevLogIndex || prevLogTerm != args.PrevLogTerm {
			reply.Success = false
		} else {
			reply.Success = true
			i := 0
			j := args.PrevLogIndex + 1 - rf.logsStartIndex
			for i < len(args.Entries) && j < len(rf.logs) {
				if args.Entries[i].Term == rf.logs[j].Term {
					i++
					j++
				} else {
					rf.logs = rf.logs[:j]
					rf.persist()
					break
				}
			}
			for ; i < len(args.Entries); i++ {
				rf.logs = append(rf.logs, args.Entries[i])
				rf.persist()
				DPrintf("raft server %d append command %v index %d at term %d at %v\n", rf.me, args.Entries[i].Command, len(rf.logs)-1+rf.logsStartIndex, rf.currentTerm, time.Now())
			}
			if args.LeaderCommit > rf.commitIndex {
				lastNewEntryIndex := args.PrevLogIndex + len(args.Entries)
				if lastNewEntryIndex < args.LeaderCommit {
					rf.commitIndex = lastNewEntryIndex
					DPrintf("raft follower %d set commitIndex to %d at term %d\n", rf.me, rf.commitIndex, rf.currentTerm)
				} else {
					rf.commitIndex = args.LeaderCommit
					DPrintf("raft follower %d set commitIndex to %d at term %d\n", rf.me, rf.commitIndex, rf.currentTerm)
				}
			}
		}
		reply.Term = rf.currentTerm
		rf.resetElectionTime()
		return
	}
}

func (rf *Raft) callAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	DPrintf("raft leader %d call AppendEntries to server %d at term %d with args %v\n", args.LeaderId, server, args.Term, args)
	return ok
}

func (rf *Raft) paralCallAppendEntries(server int, args AppendEntriesArgs) {
	reply := AppendEntriesReply{}
	ok := rf.callAppendEntries(server, &args, &reply)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.state != leader {
		return
	}
	if !ok {
		DPrintf("node %d AppendEntries RPC to node %d failed\n", rf.me, server)
		return
	} else {
		if reply.Term != args.Term {
			return
		} else {
			if reply.Success {
				rf.nextIndex[server] = args.PrevLogIndex + len(args.Entries) + 1
				rf.matchIndex[server] = args.PrevLogIndex + len(args.Entries)
				return
			} else {
				if rf.nextIndex[server] <= 1 || rf.nextIndex[server] > len(rf.logs)+rf.logsStartIndex {
					return
				}
				rf.nextIndex[server] = rf.nextIndex[server] / 2
				// rf.nextIndex[server] = rf.nextIndex[server] - 1
				if rf.nextIndex[server] > rf.logsStartIndex {
					newArgs := AppendEntriesArgs{
						Term:         rf.currentTerm,
						LeaderId:     rf.me,
						PrevLogIndex: rf.nextIndex[server] - 1,
						PrevLogTerm:  rf.logs[rf.nextIndex[server]-1-rf.logsStartIndex].Term,
						Entries:      append([]LogEntry(nil), rf.logs[rf.nextIndex[server]-rf.logsStartIndex:]...),
						LeaderCommit: rf.commitIndex}
					go rf.paralCallAppendEntries(server, newArgs)
				} else if rf.nextIndex[server] == rf.logsStartIndex {
					newArgs := AppendEntriesArgs{
						Term:         rf.currentTerm,
						LeaderId:     rf.me,
						PrevLogIndex: rf.nextIndex[server] - 1,
						PrevLogTerm:  rf.lastIncludedTerm,
						Entries:      append([]LogEntry(nil), rf.logs[rf.nextIndex[server]-rf.logsStartIndex:]...),
						LeaderCommit: rf.commitIndex}
					go rf.paralCallAppendEntries(server, newArgs)
				} else {
					args := InstallSnapshotArgs{
						Term:             rf.currentTerm,
						LeaderId:         rf.me,
						LastIncludeIndex: rf.lastIncludeIndex,
						LastIncludeTerm:  rf.lastIncludedTerm,
						Data:             rf.persister.ReadSnapshot(),
					}
					DPrintf("raft leader %d call installSnapshot to server %d because of appendentries backup\n", rf.me, server)
					go rf.paralCallInstallSnapshot(server, &args)
					return
				}
			}
		}
	}
}

//
// example  RPC arguments structure.
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

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.VoteGranted = false
	reply.Term = rf.currentTerm
	if args.Term >= rf.currentTerm {
		if args.Term > rf.currentTerm {
			rf.currentTerm = args.Term
			rf.state = follower
			rf.voteFor = -1
			rf.persist()
		}
		reply.Term = rf.currentTerm
		var lastLogTerm, lastLogIndex int
		if len(rf.logs) == 0 {
			lastLogIndex = rf.lastIncludeIndex
			lastLogTerm = rf.lastIncludedTerm
		} else {
			lastLogIndex = len(rf.logs) - 1 + rf.logsStartIndex
			lastLogTerm = rf.logs[len(rf.logs)-1].Term
		}
		if rf.voteFor == args.CandidateId || rf.voteFor == -1 {
			if args.LastLogTerm > lastLogTerm ||
				(args.LastLogTerm == lastLogTerm && args.LastLogIndex >= lastLogIndex) {
				rf.voteFor = args.CandidateId
				rf.persist()
				reply.VoteGranted = true
				rf.resetElectionTime()
				DPrintf("raft server %d vote for server %d in term %d\n", rf.me, args.CandidateId, rf.currentTerm)
				return
			}
		}
	}
	DPrintf("raft server %d reject vote for server %d in term %d\n", rf.me, args.CandidateId, rf.currentTerm)
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
func (rf *Raft) paralRequestVote(server int, args *RequestVoteArgs) {
	reply := RequestVoteReply{}
	ok := rf.sendRequestVote(server, args, &reply)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if ok {
		if reply.Term != args.Term || reply.Term != rf.currentTerm {
			return
		}
		if reply.VoteGranted {
			if rf.state == candidate {
				rf.receiveVote++
				DPrintf("raft server %d get a vote from server %d\n", args.CandidateId, server)
				if rf.receiveVote*2 > len(rf.peers) {
					// elected to be the leader
					rf.state = leader
					for i := 0; i < len(rf.peers); i++ {
						if i == rf.me {
							continue
						}
						rf.nextIndex[i] = len(rf.logs) + rf.logsStartIndex
						rf.matchIndex[i] = 0
					}
					DPrintf("raft server %d become the leader at term %d\n", args.CandidateId, args.Term)
				}
			}
		} else {
			if reply.Term > rf.currentTerm {
				rf.state = follower
				rf.currentTerm = reply.Term
				rf.voteFor = -1
				rf.persist()
			}
		}
	} else {
		DPrintf("raft server %d RequestVote RPC to server %d failed\n", rf.me, server)
	}
}

type InstallSnapshotArgs struct {
	Term             int
	LeaderId         int
	LastIncludeIndex int
	LastIncludeTerm  int
	Data             []byte
}
type InstallSnapshotReply struct {
	Term             int
	LastIncludeIndex int
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	DPrintf("raft server %d start Install Snapshot handle with SnapshotIndex %d\n", rf.me, args.LastIncludeIndex)
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		return
	} else {
		rf.state = follower
		if args.Term > rf.currentTerm {
			rf.currentTerm = args.Term
			rf.persist()
		}
		reply.Term = rf.currentTerm
		if rf.state == follower {
			if rf.lastIncludeIndex >= args.LastIncludeIndex {
				DPrintf("raft server %d return Install Snapshot because of bigger or same lastIncludeIndex rf: %d, args: %d \n", rf.me, rf.lastIncludeIndex, args.LastIncludeIndex)
				reply.LastIncludeIndex = rf.lastIncludeIndex
				return
			}
			if len(rf.logs) > 0 &&
				rf.logsStartIndex+len(rf.logs) > args.LastIncludeIndex &&
				rf.logsStartIndex <= args.LastIncludeIndex &&
				rf.logs[args.LastIncludeIndex-rf.logsStartIndex].Term == args.LastIncludeTerm {
				DPrintf("raft server %d return Install Snapshot because of having same log as the args.lastIncludeIndex\n", rf.me)
				return
			} else {
				rf.logs = make([]LogEntry, 0)
				rf.lastIncludeIndex = args.LastIncludeIndex
				rf.lastIncludedTerm = args.LastIncludeTerm
				rf.logsStartIndex = args.LastIncludeIndex + 1
				rf.lastApplied = args.LastIncludeIndex
				rf.commitIndex = args.LastIncludeIndex
				w := new(bytes.Buffer)
				e := labgob.NewEncoder(w)
				e.Encode(rf.currentTerm)
				e.Encode(rf.voteFor)
				e.Encode(rf.logs)
				e.Encode(rf.lastIncludeIndex)
				e.Encode(rf.lastIncludedTerm)
				e.Encode(rf.logsStartIndex)
				state := w.Bytes()
				rf.persister.SaveStateAndSnapshot(state, args.Data)
				applyMsg := ApplyMsg{
					CommandValid:  false,
					SnapshotValid: true,
					SnapshotIndex: args.LastIncludeIndex,
					SnapshotTerm:  args.LastIncludeTerm,
					Snapshot:      args.Data,
				}
				DPrintf("raft server %d ready for the InstallSnapshot with SnapshotIndex %d and SnapshotTerm %d\n", rf.me, applyMsg.SnapshotIndex, applyMsg.SnapshotTerm)
				rf.mu.Unlock()
				rf.applyCh <- applyMsg
				rf.mu.Lock()
				DPrintf("raft server %d  Install Snapshot with SnapshotIndex %d and SnapshotTerm %d\n", rf.me, applyMsg.SnapshotIndex, applyMsg.SnapshotTerm)

			}
		}
	}
}

func (rf *Raft) callInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
	return ok
}
func (rf *Raft) paralCallInstallSnapshot(server int, args *InstallSnapshotArgs) {
	reply := InstallSnapshotReply{}
	ok := rf.callInstallSnapshot(server, args, &reply)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	DPrintf("raft leader %d call installSnapshot to server %d with args %v\n", rf.me, server, args)
	if !ok {
		DPrintf("InstallSnapshot RPC leader %d to server %d failed\n", rf.me, server)
		return
	} else {
		if args.Term != reply.Term {
			return
		} else {
			if reply.LastIncludeIndex >= args.LastIncludeIndex {
				rf.nextIndex[server] = reply.LastIncludeIndex + 1
				rf.matchIndex[server] = reply.LastIncludeIndex
			} else {
				rf.nextIndex[server] = args.LastIncludeIndex + 1
				rf.matchIndex[server] = args.LastIncludeIndex
			}
		}
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
	isLeader := false

	// Your code here (2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.state == leader {
		isLeader = true
		term = rf.currentTerm
		index = rf.logsStartIndex + len(rf.logs)
		rf.logs = append(rf.logs, LogEntry{Term: term, Command: command})
		rf.persist()
		DPrintf("raft leader %d start the command %v index %d at term %d at %v\n", rf.me, command, index, rf.currentTerm, time.Now())
	}

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
func (rf *Raft) ticker() {
	// make the ticker goroutines of every raft server start at a random time
	rand.Seed(time.Now().UnixNano())
	randomNumber := rand.Intn(100)
	time.Sleep(time.Duration(randomNumber) * time.Millisecond)

	for rf.killed() == false {

		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		time.Sleep(heartBeatTimeOut)
		rf.mu.Lock()
		if time.Now().After(rf.electionTime) {
			// election timeout
			if rf.state == follower || rf.state == candidate {
				// start leader election
				rf.state = candidate
				rf.currentTerm++
				DPrintf("raft server %d start election term %d at %v\n", rf.me, rf.currentTerm, time.Now())
				rf.voteFor = rf.me
				rf.persist()
				candidateId := rf.me
				rf.receiveVote = 1
				var args RequestVoteArgs
				if len(rf.logs) == 0 {
					args = RequestVoteArgs{rf.currentTerm, rf.me, rf.lastIncludeIndex, rf.lastIncludedTerm}
				} else {
					args = RequestVoteArgs{rf.currentTerm, rf.me, len(rf.logs) - 1 + rf.logsStartIndex, rf.logs[len(rf.logs)-1].Term}
				}
				n := len(rf.peers)
				rf.resetElectionTime()
				for i := 0; i < n; i++ {
					if i == candidateId {
						continue
					}
					go rf.paralRequestVote(i, &args)
				}
			}
		}
		rf.mu.Unlock()
	}
}
func (rf *Raft) sendHeartBeat() {
	for rf.killed() == false {
		rf.mu.Lock()
		if rf.state == leader {
			// send heart beat to every follwer
			for i := 0; i < len(rf.peers); i++ {
				if i == rf.me {
					continue
				}
				if rf.nextIndex[i] > rf.logsStartIndex {
					if rf.nextIndex[i]-1-rf.logsStartIndex < len(rf.logs) {
						args := AppendEntriesArgs{
							Term:         rf.currentTerm,
							LeaderId:     rf.me,
							PrevLogIndex: rf.nextIndex[i] - 1,
							PrevLogTerm:  rf.logs[rf.nextIndex[i]-1-rf.logsStartIndex].Term,
							Entries:      []LogEntry{},
							LeaderCommit: rf.commitIndex}
						go rf.paralCallAppendEntries(i, args)
					}
				} else if rf.nextIndex[i] == rf.logsStartIndex {
					args := AppendEntriesArgs{
						Term:         rf.currentTerm,
						LeaderId:     rf.me,
						PrevLogIndex: rf.nextIndex[i] - 1,
						PrevLogTerm:  rf.lastIncludedTerm,
						Entries:      []LogEntry{},
						LeaderCommit: rf.commitIndex}
					go rf.paralCallAppendEntries(i, args)
				} else {
					args := InstallSnapshotArgs{
						Term:             rf.currentTerm,
						LeaderId:         rf.me,
						LastIncludeIndex: rf.lastIncludeIndex,
						LastIncludeTerm:  rf.lastIncludedTerm,
						Data:             rf.persister.ReadSnapshot(),
					}
					DPrintf("raft leader %d call installSnapshot to server %d because of heartbeat\n", rf.me, i)
					go rf.paralCallInstallSnapshot(i, &args)
				}
			}
		}
		rf.mu.Unlock()
		time.Sleep(heartBeatTimeOut)
	}
}

func (rf *Raft) apply(applyCh chan ApplyMsg) {
	for rf.killed() == false {
		rf.mu.Lock()
		for rf.commitIndex > rf.lastApplied && len(rf.logs)-1+rf.logsStartIndex > rf.lastApplied && rf.lastApplied+1-rf.logsStartIndex >= 0 {
			newApplyMsg := ApplyMsg{CommandValid: true, Command: rf.logs[rf.lastApplied+1-rf.logsStartIndex].Command, CommandIndex: rf.lastApplied + 1}
			rf.lastApplied++
			rf.mu.Unlock()
			applyCh <- newApplyMsg
			rf.mu.Lock()
		}
		rf.mu.Unlock()
		time.Sleep(10 * time.Millisecond)
	}
}

func (rf *Raft) sendNewLog() {
	for rf.killed() == false {
		rf.mu.Lock()
		if rf.state == leader {
			for i := 0; i < len(rf.peers); i++ {
				if i == rf.me {
					continue
				}
				if len(rf.logs)-1+rf.logsStartIndex >= rf.nextIndex[i] && rf.nextIndex[i] > 0 {
					if rf.nextIndex[i] > rf.logsStartIndex {
						args := AppendEntriesArgs{
							Term:         rf.currentTerm,
							LeaderId:     rf.me,
							PrevLogIndex: rf.nextIndex[i] - 1,
							PrevLogTerm:  rf.logs[rf.nextIndex[i]-1-rf.logsStartIndex].Term,
							Entries:      append([]LogEntry(nil), rf.logs[rf.nextIndex[i]-rf.logsStartIndex:]...),
							LeaderCommit: rf.commitIndex}
						DPrintf("raft leader %d send new log to server %d at term %d with args %v\n", rf.me, i, rf.currentTerm, args)
						go rf.paralCallAppendEntries(i, args)
					} else if rf.nextIndex[i] == rf.logsStartIndex {
						args := AppendEntriesArgs{
							Term:         rf.currentTerm,
							LeaderId:     rf.me,
							PrevLogIndex: rf.nextIndex[i] - 1,
							PrevLogTerm:  rf.lastIncludedTerm,
							Entries:      append([]LogEntry(nil), rf.logs[rf.nextIndex[i]-rf.logsStartIndex:]...),
							LeaderCommit: rf.commitIndex}
						DPrintf("raft leader %d send new log to server %d at term %d with args %v\n", rf.me, i, rf.currentTerm, args)
						go rf.paralCallAppendEntries(i, args)
					} else if rf.nextIndex[i] < rf.logsStartIndex {
						args := InstallSnapshotArgs{
							Term:             rf.currentTerm,
							LeaderId:         rf.me,
							LastIncludeIndex: rf.lastIncludeIndex,
							LastIncludeTerm:  rf.lastIncludedTerm,
							Data:             rf.persister.ReadSnapshot(),
						}
						DPrintf("raft leader %d call installSnapshot to server %d because of send new log\n", rf.me, i)
						go rf.paralCallInstallSnapshot(i, &args)
					}
				}
			}
		}
		rf.mu.Unlock()
		time.Sleep(10 * time.Millisecond)
	}
}

func (rf *Raft) updateCommitIndex() {
	for rf.killed() == false {
		rf.mu.Lock()
		if rf.state == leader {
			for n := rf.commitIndex + 1; n < len(rf.logs)+rf.logsStartIndex; n++ {
				if rf.logs[n-rf.logsStartIndex].Term == rf.currentTerm {
					matchNum := 1
					for i := 0; i < len(rf.peers); i++ {
						if i == rf.me {
							continue
						}
						if rf.matchIndex[i] >= n {
							matchNum++
						}
					}
					if matchNum*2 > len(rf.peers) {
						rf.commitIndex = n
						DPrintf("raft leader %d set the commitIndex to %d at term %d at %v\n", rf.me, n, rf.currentTerm, time.Now())
						break
					}
				}
			}
		}
		rf.mu.Unlock()
		time.Sleep(10 * time.Millisecond)
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
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.applyCh = applyCh

	// Your initialization code here (2A, 2B, 2C).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.dead = 0
	rf.state = follower
	rf.receiveVote = 0
	rf.currentTerm = 0
	rf.voteFor = -1
	rf.logs = append(rf.logs, LogEntry{Term: 0})
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.nextIndex = make([]int, len(peers))
	rf.matchIndex = make([]int, len(peers))
	rf.lastIncludeIndex = -1
	rf.lastIncludedTerm = -1
	rf.logsStartIndex = 0

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	if rf.lastIncludeIndex != -1 {
		// if the server recorver from the snap, should set the commitIndex and lastApplied to lastIncludeIndex
		rf.commitIndex = rf.lastIncludeIndex
		rf.lastApplied = rf.lastIncludeIndex
	}

	rf.resetElectionTime()

	// start ticker goroutine to start elections
	go rf.ticker()

	// start goroutine to send heartbeat
	go rf.sendHeartBeat()

	// apply the new command
	go rf.apply(applyCh)

	// leader send new log to peers
	go rf.sendNewLog()

	// leader update the commitIndex
	go rf.updateCommitIndex()

	return rf
}

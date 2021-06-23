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
	"bytes"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"6.824/labgob"
	"6.824/labrpc"
)

type Role int

const (
	LEADER    = 1
	CANDIDATE = 2
	FOLLOWER  = 3
)

// feature flags

const (
	PREVOTE          = true
	LEADER_STICKNESS = false
	NOOP_LOG         = false
)

const HEARTBEAT_INTERVAL = 100

const ELECTION_TIMEOUT_MIN = 250
const ELECTION_TIMEOUT_MAX = 400

type Log struct {
	Term    int
	Index   int
	Command interface{}
}

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

	role Role

	currentTerm int
	votedFor    int
	log         []Log

	// index of highest log entry known to be committed
	commitIndex int

	// index of highest log entry applied to state machine
	lastApplied int

	nextIndex  []int
	matchIndex []int

	leader              int
	lastHeardTime       time.Time
	electionTimeoutTime time.Time

	lastBroadcastTime time.Time

	// Reset at AppendEntries
	// Can only vote or prevote after this time
	nextVoteTime time.Time

	applyCh chan ApplyMsg

	// snapshots
	snapshotLastTerm  int
	snapshotLastIndex int
}

func (rf *Raft) resetLastHeard() {
	rf.lastHeardTime = time.Now()
	rf.electionTimeoutTime = rf.lastHeardTime.Add(getElectionTimeout())
}

type PrevLogInfo struct {
	Index int
	Term  int
}

func (rf *Raft) lastLog() PrevLogInfo {
	if len(rf.log) > 1 {
		lastLog := rf.log[len(rf.log)-1]
		return PrevLogInfo{
			Index: lastLog.Index,
			Term:  lastLog.Term,
		}
	} else {
		return PrevLogInfo{
			Index: rf.snapshotLastIndex,
			Term:  rf.snapshotLastTerm,
		}
	}
}
func (rf *Raft) prevLogInfo(indexPreviousOf int) PrevLogInfo {
	actualIndex := indexPreviousOf - rf.snapshotLastIndex
	if actualIndex == 1 {
		return PrevLogInfo{
			Index: rf.snapshotLastIndex,
			Term:  rf.snapshotLastTerm,
		}
	} else {
		lastLog := rf.log[actualIndex-1]
		return PrevLogInfo{
			Index: lastLog.Index,
			Term:  lastLog.Term,
		}
	}
}

func (rf *Raft) getLogAt(index int) *Log {
	actualIndex := index - rf.snapshotLastIndex

	return &rf.log[actualIndex]
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.currentTerm, rf.role == LEADER
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
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	e.Encode(rf.snapshotLastIndex)
	e.Encode(rf.snapshotLastTerm)

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
	// Example:
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)

	var currentTerm, votedFor, snapshotLastIndex, snapshotLastTerm int
	var log []Log

	if d.Decode(&currentTerm) != nil || d.Decode(&votedFor) != nil || d.Decode(&log) != nil || d.Decode(&snapshotLastIndex) != nil || d.Decode(&snapshotLastTerm) != nil {
		panic("Error during readPersist.")
	}

	rf.currentTerm = currentTerm
	rf.votedFor = votedFor
	rf.snapshotLastIndex = snapshotLastIndex
	rf.snapshotLastTerm = snapshotLastTerm
	rf.log = log
	rf.lastApplied = rf.snapshotLastIndex
	rf.commitIndex = rf.snapshotLastIndex

}

//
// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
//
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

	// Your code here (2D).

	rf.mu.Lock()
	defer rf.mu.Unlock()

	return rf.snapshotLastIndex <= lastIncludedIndex && rf.snapshotLastTerm <= lastIncludedTerm
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

	rf.mu.Lock()
	defer rf.mu.Unlock()

	rf.dprint("Handling snapshot to index %d", index)

	// find the index from the back
	i := len(rf.log) - 1
	for ; i > 0; i-- {
		if rf.log[i].Index == index {
			break
		}
	}

	// the first element should be preserved
	firstElement := rf.log[0]

	if i == 0 {
		rf.dprint("No log with index %d is found. Removing all logs", index)
		rf.log = []Log{firstElement}
		rf.snapshotLastIndex = index
	} else {
		rf.dprint("Removing logs until index %d", index)
		rf.snapshotLastIndex = rf.log[i].Index
		rf.snapshotLastTerm = rf.log[i].Term
		rf.log = append([]Log{firstElement}, rf.log[i+1:]...)
	}

	// rf.lastApplied = index
	// rf.commitIndex = index

	rf.persist()
	rf.persister.SaveStateAndSnapshot(rf.persister.ReadRaftState(), snapshot)
}

// InstallSnapshot
type InstallSnapshotArgs struct {
	Term              int
	LeaderId          int
	LastIncludedIndex int
	LastIncludedTerm  int
	Data              []byte
}

type InstallSnapshotReply struct {
	Term int
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	rf.dprint("Handling InstallSnapshot")

	reply.Term = rf.currentTerm

	if args.Term < rf.currentTerm {
		rf.resetLastHeard()
		return
	}

	if args.Term > rf.currentTerm {
		rf.toFollower(args.Term)
	}

	if args.LastIncludedIndex < rf.snapshotLastIndex {
		rf.dprint("Received a outdated snapshot. Last index %d < %d", args.LastIncludedIndex, rf.snapshotLastIndex)
		return
	}

	// find the index from the back
	i := 0
	for ; i >= 0; i-- {
		if rf.log[i].Index == args.LastIncludedIndex {
			break
		}
	}

	firstElement := rf.log[0]
	if i <= 0 {
		if rf.snapshotLastIndex == args.LastIncludedIndex {
			if rf.snapshotLastTerm == args.LastIncludedTerm {
				rf.dprint("The snapshot to %d is already taken.", rf.snapshotLastIndex)
			} else {
				rf.dprint("Snapshot index matches, but term doesn't match.")
				panic("shouldn't happen for now")
			}
		} else {
			rf.dprint("Log with index %d is not found. Removing all logs.", args.LastIncludedIndex)
			rf.snapshotLastIndex = args.LastIncludedIndex
			rf.snapshotLastTerm = args.LastIncludedTerm
			rf.log = []Log{firstElement}
		}
	} else {
		rf.dprint("Removing logs until index %d", args.LastIncludedIndex)
		rf.snapshotLastIndex = rf.log[i].Index
		rf.snapshotLastTerm = rf.log[i].Term
		rf.log = append([]Log{firstElement}, rf.log[i+1:]...)
	}

	rf.lastApplied = args.LastIncludedIndex
	rf.commitIndex = args.LastIncludedIndex

	go func() {
		rf.applyCh <- ApplyMsg{
			CommandValid:  false,
			SnapshotValid: true,
			Snapshot:      args.Data,
			SnapshotTerm:  args.LastIncludedTerm,
			SnapshotIndex: args.LastIncludedIndex,
		}
	}()

	rf.dprint("Snapshot installed to %d", args.LastIncludedIndex)

	rf.persist()
	rf.persister.SaveStateAndSnapshot(rf.persister.ReadRaftState(), args.Data)
	rf.resetLastHeard()

}

func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
	return ok
}

// AppendEntries
type AppendEntriesArgs struct {
	// leader's term
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []Log
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	Success bool

	ConflictingEntryTerm int
	FirstIndexForTerm    int
}

func intMin(a int, b int) int {
	if a > b {
		return b
	} else {
		return a
	}
}

func intMax(a int, b int) int {
	if a < b {
		return b
	} else {
		return a
	}
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {

	rf.mu.Lock()
	defer rf.mu.Unlock()

	rf.dprint("Handle AppendEntries from leader %d. Log size %d", args.LeaderId, len(args.Entries))

	reply.Term = rf.currentTerm

	if args.Term < rf.currentTerm {
		reply.Success = false
		reply.ConflictingEntryTerm = -1
		reply.FirstIndexForTerm = -1
		rf.dprint("Incoming AppendEntries has lower term %d < %d. Rejected.", args.Term, rf.currentTerm)
		return
	}

	if LEADER_STICKNESS {
		rf.nextVoteTime = time.Now().Add(getElectionTimeout())
	}

	rf.resetLastHeard()

	// the incoming args have higher term
	// change term, and become follower
	if args.Term > rf.currentTerm {
		rf.toFollower(args.Term)
	}

	rf.leader = args.LeaderId

	//
	// 0 1 1 1 1 1 4
	// 0 1 2 2

	// if log doesn't contain an entry at prevLogIndex, or
	// if log contais the entry, but term doesn't matches prevLogTerm
	// return false

	// Since there are a dummy log index 0 in each server,
	// the index for each actual log matches the index of log in the array
	// so, if args.PrevLogIndex == i, we should check if there are i+1 elements in the array

	// if there are snapshot, add the length of snapshot

	if len(rf.log)+rf.snapshotLastIndex <= args.PrevLogIndex {
		rf.dprint("Doesn't have PrevLogIndex. Rejects")
		reply.Success = false

		// if log doesn't have such key,
		// return the latest log's index + 1 (next position)
		reply.ConflictingEntryTerm = -1
		reply.FirstIndexForTerm = rf.lastLog().Index + 1
		return
	}

	lastLogIndex := args.PrevLogIndex - rf.snapshotLastIndex

	var lastLogTerm int
	if lastLogIndex == 0 {
		lastLogTerm = rf.snapshotLastTerm
	} else {
		lastLogTerm = rf.log[lastLogIndex].Term
	}

	if lastLogTerm != args.PrevLogTerm {

		rf.dprint("Log inconsistency. Rejected")

		reply.Success = false

		// has such key, but term doesn't match
		reply.ConflictingEntryTerm = lastLogTerm
		reply.FirstIndexForTerm = args.PrevLogIndex

		// find the first key of that term
		for ; reply.FirstIndexForTerm >= rf.snapshotLastIndex && rf.log[reply.FirstIndexForTerm-rf.snapshotLastIndex-1].Term == reply.ConflictingEntryTerm; reply.FirstIndexForTerm-- {

		}

		return
	}

	// rf.dprint("%+v", rf.log)

	for i, log := range args.Entries {
		// if rf.log doesn't contain log.Index, append the rest
		// <= for the first log is 0
		// if the latest rf.log has index 1, the len(rf.log) is 2, but log has index 2
		if len(rf.log)+rf.snapshotLastIndex <= log.Index {
			rf.log = append(rf.log, args.Entries[i:]...)
			rf.persist()
			break
		} else {
			// rf.log has the same index
			existing := rf.log[log.Index-rf.snapshotLastIndex]

			if existing.Term == log.Term {
				// ignore the entry, continue loop
			} else {
				// delete the entry and all following it
				rf.log = rf.log[:log.Index-rf.snapshotLastIndex]
				// append the rest
				rf.log = append(rf.log, args.Entries[i:]...)
				rf.persist()
				break
			}
		}
	}

	rf.dprint("LeaderCommit %d, commitIndex %d", args.LeaderCommit, rf.commitIndex)

	if args.LeaderCommit > rf.commitIndex {
		N := intMin(args.LeaderCommit, rf.lastLog().Index)
		rf.commitMessages(N)
	}

	reply.Success = true

}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

// requires lock
func (rf *Raft) isUptoDate(lastLogIndex, lastLogTerm int) bool {
	lastLog := rf.lastLog()
	return (lastLogTerm > lastLog.Term) || (lastLogTerm == lastLog.Term && lastLogIndex >= lastLog.Index)
}

// PreVote RPC

type PreVoteArgs struct {
	NextTerm     int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

type PreVoteReply struct {
	Term        int
	VoteGranted bool
}

func (rf *Raft) PreVote(args *PreVoteArgs, reply *PreVoteReply) {

	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Term = rf.currentTerm
	reply.VoteGranted = false

	// leader stickness
	if LEADER_STICKNESS && time.Now().Before(rf.nextVoteTime) {
		return
	}

	if args.NextTerm < rf.currentTerm {
		return
	}

	if args.NextTerm > rf.currentTerm+1 {
		rf.toFollower(args.NextTerm - 1)
	}

	if rf.isUptoDate(args.LastLogIndex, args.LastLogTerm) {
		reply.VoteGranted = true
	}

}

func (rf *Raft) sendPreVote(server int, args *PreVoteArgs, reply *PreVoteReply) bool {
	ok := rf.peers[server].Call("Raft.PreVote", args, reply)
	return ok
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

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {

	// Your code here (2A, 2B).

	rf.mu.Lock()
	defer rf.mu.Unlock()

	rf.dprint("Received RequestVote from %d", args.CandidateId)

	reply.Term = rf.currentTerm
	reply.VoteGranted = false

	if args.Term < rf.currentTerm {
		rf.dprint("Rejected RequestVote from %d due to term", args.CandidateId)
		return
	}

	// leader stickness
	if LEADER_STICKNESS && time.Now().Before(rf.nextVoteTime) {
		return
	}

	if args.Term > rf.currentTerm {
		rf.dprint("Received higher term %d > %d. Become follower", args.Term, rf.currentTerm)
		rf.toFollower(args.Term)
	}

	if rf.votedFor == -1 || rf.votedFor == args.CandidateId {
		if rf.isUptoDate(args.LastLogIndex, args.LastLogTerm) {
			rf.dprint("Granted RequestVote from %d", args.CandidateId)
			reply.VoteGranted = true

			rf.votedFor = args.CandidateId
			rf.persist()

			rf.role = FOLLOWER

			rf.resetLastHeard()
		} else {
			rf.dprint("Reject RequestVote from %d due to not update", args.CandidateId)
		}

	} else {
		rf.dprint("Reject RequestVote from %d. Has voted %d for term %d.", args.CandidateId, rf.votedFor, args.Term)
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

func (rf *Raft) addCommand(command interface{}) (int, int) {
	log := Log{
		Index:   rf.lastLog().Index + 1,
		Term:    rf.currentTerm,
		Command: command,
	}

	rf.log = append(rf.log, log)

	rf.persist()

	// update nextIndex for me
	rf.nextIndex[rf.me] = log.Index + 1
	rf.matchIndex[rf.me] = log.Index

	return log.Index, log.Term
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

	// Your code here (2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.role != LEADER {
		return -1, -1, false
	}

	index, term := rf.addCommand(command)

	rf.dprint("Created new log (%d, %d, %+v)", index, term, command)

	return index, term, true
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

func (rf *Raft) toLeader() {
	rf.role = LEADER

	// reset volatile states
	rf.nextIndex = make([]int, len(rf.peers))

	lastLogIndex := rf.lastLog().Index + 1
	for i := 0; i < len(rf.peers); i++ {
		rf.nextIndex[i] = lastLogIndex
	}

	rf.matchIndex = make([]int, len(rf.peers))

	// update the commitIndex
	if NOOP_LOG {
		rf.addCommand(-1)
	}

	// broadcast immediately
	rf.broadcast()
}

func (rf *Raft) toFollower(term int) {

	rf.role = FOLLOWER

	rf.currentTerm = term
	rf.votedFor = -1
	rf.persist()

	rf.leader = -1

}

func getElectionTimeout() time.Duration {
	return time.Duration(rand.Intn(ELECTION_TIMEOUT_MAX-ELECTION_TIMEOUT_MIN)+ELECTION_TIMEOUT_MIN) * time.Millisecond
}

func (rf *Raft) dprint(format string, a ...interface{}) {
	DPrintf("[%d, %d] "+format, append([]interface{}{rf.me, rf.currentTerm}, a...)...)
}

// locked version of dprint
func (rf *Raft) ldprint(format string, a ...interface{}) {
	rf.mu.Lock()
	rf.dprint(format, a...)
	rf.mu.Unlock()
}

func (rf *Raft) runPreVote() bool {
	rf.dprint("Start prevote")

	rf.resetLastHeard()

	timeout := rf.electionTimeoutTime

	voteResultChan := make(chan *PreVoteReply, len(rf.peers))

	lastLog := rf.lastLog()

	args := PreVoteArgs{
		NextTerm:     rf.currentTerm + 1,
		CandidateId:  rf.me,
		LastLogIndex: lastLog.Index,
		LastLogTerm:  lastLog.Term,
	}

	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		go func(id int) {
			reply := PreVoteReply{}
			if ok := rf.sendPreVote(id, &args, &reply); ok {
				voteResultChan <- &reply
			} else {
				voteResultChan <- nil
			}
		}(i)
	}

	rf.mu.Unlock()

	type Result struct {
		finishCount int
		voteCount   int
		maxTerm     int
	}

	resultChan := make(chan *Result, 1)

	go func() {
		finishCount := 1
		voteCount := 1
		maxTerm := 0

		for voteResult := range voteResultChan {
			finishCount += 1
			if voteResult != nil {
				if voteResult.VoteGranted {
					voteCount += 1
				}
				// record max term
				if voteResult.Term > maxTerm {
					maxTerm = voteResult.Term
				}
			}
			// if all send completes, or majority of votes has been received, end
			if finishCount == len(rf.peers) || voteCount > len(rf.peers)/2 {
				break
			}
		}

		resultChan <- &Result{
			finishCount: finishCount,
			voteCount:   voteCount,
			maxTerm:     maxTerm,
		}
	}()

	var result *Result

	select {
	case r := <-resultChan:
		result = r
		rf.ldprint("PreVote completed")
	case <-time.After(time.Until(timeout)):
		rf.ldprint("PreVote timeout. Restart.")
		return false
	}

	shouldContinue := false

	if result != nil {
		rf.mu.Lock()

		// if role or term changes, ignore result
		if rf.role != FOLLOWER || rf.currentTerm+1 != args.NextTerm {
			rf.dprint("Role changed (now %d) or term changed (%d+1 != %d). Failed", rf.role, rf.currentTerm, args.NextTerm)
			rf.mu.Unlock()
			return false
		}

		// received higher term, this vote should be ignored
		if result.maxTerm > rf.currentTerm {
			rf.toFollower(result.maxTerm)
		} else {
			if result.voteCount > len(rf.peers)/2 {
				rf.dprint("Majority reached. To actual RequestVote.")
				shouldContinue = true
			} else {
				rf.dprint("No majority reached. Restart election")
				rf.resetLastHeard()
			}
		}
		rf.mu.Unlock()
	}

	return shouldContinue

}

func (rf *Raft) electionLoop() {
	for !rf.killed() {

		time.Sleep(10 * time.Millisecond)

		rf.mu.Lock()

		if rf.role == LEADER {
			rf.mu.Unlock()
			continue
		}
		if time.Now().Before(rf.electionTimeoutTime) {
			rf.mu.Unlock()
			continue
		} else {
			rf.dprint("Election started.")
		}

		// // For follower, run PreVote before becoming candidate
		if PREVOTE && rf.role == FOLLOWER {
			if !rf.runPreVote() {
				rf.ldprint("PreVote failed. Restart election.")
				continue
			} else {
				// lock is released after runPreVote, relock
				rf.mu.Lock()
			}
		}

		// Run actual request vote
		rf.role = CANDIDATE
		rf.currentTerm++
		rf.votedFor = rf.me
		rf.persist()

		rf.resetLastHeard()

		timeout := rf.electionTimeoutTime

		rf.dprint("Start requesting votes for term %d", rf.currentTerm)

		lastLog := rf.lastLog()

		args := RequestVoteArgs{
			Term:         rf.currentTerm,
			CandidateId:  rf.me,
			LastLogIndex: lastLog.Index,
			LastLogTerm:  lastLog.Term,
		}

		voteResultChan := make(chan *RequestVoteReply, len(rf.peers))

		// send requests to all
		for i := range rf.peers {
			if i == rf.me {
				continue
			}
			go func(id int) {
				resp := RequestVoteReply{}
				if ok := rf.sendRequestVote(id, &args, &resp); ok {
					voteResultChan <- &resp
				} else {
					voteResultChan <- nil
				}
			}(i)
		}

		rf.mu.Unlock()

		type Result struct {
			finishCount int
			voteCount   int
			maxTerm     int
		}

		resultChan := make(chan *Result, 1)

		go func() {
			finishCount := 1
			voteCount := 1
			maxTerm := 0

			for voteResult := range voteResultChan {
				finishCount += 1
				if voteResult != nil {
					if voteResult.VoteGranted {
						voteCount += 1
					}
					// record max term
					if voteResult.Term > maxTerm {
						maxTerm = voteResult.Term
					}
				}
				// if all send completes, or majority of votes has been received, end
				if finishCount == len(rf.peers) || voteCount > len(rf.peers)/2 {
					break
				}
			}

			resultChan <- &Result{
				finishCount: finishCount,
				voteCount:   voteCount,
				maxTerm:     maxTerm,
			}
		}()

		var result *Result

		select {
		case r := <-resultChan:
			result = r
			rf.ldprint("Election completed")
		case <-time.After(time.Until(timeout)):
			rf.ldprint("Election timeout. Back to follower and restart.")
		}

		if result != nil {
			rf.mu.Lock()

			// if role or term changes, ignore result
			if rf.role != CANDIDATE || rf.currentTerm != args.Term {
				rf.mu.Unlock()
				continue
			}

			// received higher term, this vote should be ignored
			if result.maxTerm > rf.currentTerm {
				rf.toFollower(result.maxTerm)
			} else {
				if result.voteCount > len(rf.peers)/2 {
					rf.dprint("Majority reached. Become leader.")
					rf.toLeader()
				} else {
					rf.dprint("No majority reached. Back to follower and restart election")
				}
			}
			rf.mu.Unlock()
		}
	}

}

func (rf *Raft) tryCommit() {
	N := rf.lastLog().Index

	for ; N >= rf.commitIndex+1; N-- {

		log := rf.getLogAt(N)

		rf.dprint("Log[%d].Term = %d, rf.commitIndex == %d", log.Index, log.Term, rf.commitIndex)

		// check if log[N].term == currentTerm
		if log.Term != rf.currentTerm {
			continue
		}

		// check if a majority of matchIndex[i] >= N
		count := 0
		for i := range rf.matchIndex {
			if rf.matchIndex[i] >= N {
				count++
			}
		}
		rf.dprint("for N == %d, %d matchIndexes >= N", N, count)
		if count > len(rf.matchIndex)/2 {
			break
		}
	}

	rf.dprint("N == %d, rf.commitIndex == %d", N, rf.commitIndex)

	if N >= rf.commitIndex+1 {
		rf.commitMessages(N)
	}
}

// requires lock
func (rf *Raft) broadcast() {

	rf.lastBroadcastTime = time.Now()

	for i := range rf.peers {
		if i == rf.me {
			continue
		}

		// if the followers is lagging behind, InstallSnapshot
		if rf.nextIndex[i]-1 < rf.snapshotLastIndex {
			rf.dprint("%d is too lag %d < %d. InstallSnapshot", i, rf.nextIndex[i]-1, rf.snapshotLastIndex)

			lastIncludedIndex := rf.snapshotLastIndex
			lastIncludedTerm := rf.snapshotLastTerm

			args := InstallSnapshotArgs{
				Term:              rf.currentTerm,
				LeaderId:          rf.me,
				LastIncludedIndex: lastIncludedIndex,
				LastIncludedTerm:  lastIncludedTerm,
				Data:              rf.persister.ReadSnapshot(),
			}
			go func(i int) {
				reply := InstallSnapshotReply{}
				if ok := rf.sendInstallSnapshot(i, &args, &reply); ok {
					rf.mu.Lock()
					if reply.Term > rf.currentTerm {
						rf.toFollower(reply.Term)
					}

					rf.nextIndex[i] = lastIncludedIndex + 1
					rf.matchIndex[i] = lastIncludedIndex
					rf.tryCommit()
					rf.mu.Unlock()
				}
			}(i)
			continue
		}

		prevLog := rf.prevLogInfo(rf.nextIndex[i])

		rf.dprint("leader commitIndex %d", rf.commitIndex)

		args := AppendEntriesArgs{
			Term:         rf.currentTerm,
			LeaderId:     rf.me,
			Entries:      make([]Log, 0),
			LeaderCommit: rf.commitIndex,
			// even there is no new entries,
			// set prevLogIndex and prevLogTerm to check for consistency
			PrevLogIndex: prevLog.Index,
			PrevLogTerm:  prevLog.Term,
		}

		if rf.lastLog().Index >= rf.nextIndex[i] {
			rf.dprint("New entries to append for %d. %d >= %d", i, rf.lastLog().Index, rf.nextIndex[i])
			args.Entries = rf.log[rf.nextIndex[i]-rf.snapshotLastIndex:]
		}

		go func(i int) {
			reply := AppendEntriesReply{}
			if ok := rf.sendAppendEntries(i, &args, &reply); ok {
				rf.mu.Lock()

				if reply.Term > rf.currentTerm {
					rf.dprint("Received reply with higher term %d > %d. To follower", reply.Term, rf.currentTerm)
					rf.toFollower(reply.Term)

					rf.mu.Unlock()
					return
				}

				// is heartbeat, ignore
				// if len(args.Entries) == 0 {
				// 	rf.mu.Unlock()
				// 	return
				// }

				// if rf is not leader or term has changed, ignore
				if rf.role != LEADER || rf.currentTerm != args.Term {
					rf.dprint("Is not leader or term changes (%d != %d). Ignore future handling.", rf.currentTerm, args.Term)
					rf.mu.Unlock()
					return
				}

				if reply.Success {
					rf.dprint("AppendEntries to %d successful", i)
					if len(args.Entries) > 0 {
						rf.nextIndex[i] = args.Entries[len(args.Entries)-1].Index + 1
						rf.matchIndex[i] = rf.nextIndex[i] - 1
						rf.dprint("nextIndex, matchIndex for %d: %d, %d", i, rf.nextIndex[i], rf.matchIndex[i])

						rf.tryCommit()

					}
				} else {

					// if rejected

					// if ConflictingEntryTerm is set
					if reply.ConflictingEntryTerm != -1 {
						// term conflict, remove all the entries
						rf.nextIndex[i] = reply.FirstIndexForTerm
					} else {
						// doesn't have the key
						if reply.FirstIndexForTerm != -1 {
							rf.nextIndex[i] = reply.FirstIndexForTerm
						} else {
							// the rf should be follower now
						}

					}
				}

				rf.mu.Unlock()
			}
		}(i)

	}
}

func (rf *Raft) appendEntriesLoop() {
	for !rf.killed() {

		time.Sleep(10 * time.Millisecond)

		rf.mu.Lock()

		if rf.role != LEADER {
			rf.mu.Unlock()
			continue
		}

		if time.Since(rf.lastBroadcastTime) < HEARTBEAT_INTERVAL*time.Millisecond {
			rf.mu.Unlock()
			continue
		}

		rf.broadcast()

		rf.mu.Unlock()

	}

}

// requires lock
func (rf *Raft) commitMessages(to int) {

	rf.commitIndex = to
	rf.dprint("commitIndex to %d", to)

	// if there are uncommitted messages,
	// create a goroutine to commit them
	if rf.commitIndex > rf.lastApplied {

		// collect messages to apply
		messages := make([]ApplyMsg, rf.commitIndex-rf.lastApplied)

		applyCh := rf.applyCh

		for i, log := range rf.log[rf.lastApplied+1-rf.snapshotLastIndex : rf.commitIndex+1-rf.snapshotLastIndex] {
			// if log.Command != nil {
			rf.dprint("Apply index %d", log.Index)
			messages = append(messages, ApplyMsg{
				CommandValid: true,
				Command:      log.Command,
				CommandIndex: rf.lastApplied + 1 + i,
			})
			// } else {
			// 	rf.dprint("Ignore no-op entry at index %d", log.Index)
			// }
		}
		rf.lastApplied = rf.commitIndex

		go func() {

			// apply
			for _, msg := range messages {
				applyCh <- msg
			}
		}()
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

	// Your initialization code here (2A, 2B, 2C).
	rf.role = FOLLOWER
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.log = []Log{{Term: 0, Index: 0}}
	rf.leader = -1
	rf.applyCh = applyCh

	rf.resetLastHeard()

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// election
	go rf.electionLoop()
	// leader
	go rf.appendEntriesLoop()

	return rf
}

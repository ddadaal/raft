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

type Role int

const (
	LEADER    = 1
	CANDIDATE = 2
	FOLLOWER  = 3
)

const HEARTBEAT_INTERVAL = 100

// smaller timeout doesn't pass tests
// wtf
const ELECTION_TIMEOUT_MIN = 300
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
}

func (rf *Raft) resetLastHeard() {
	rf.lastHeardTime = time.Now()
	rf.electionTimeoutTime = rf.lastHeardTime.Add(getElectionTimeout())
}

func (rf *Raft) lastLog() *Log {
	return &rf.log[len(rf.log)-1]
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
}

func intMin(a int, b int) int {
	if a > b {
		return b
	} else {
		return a
	}
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {

	rf.mu.Lock()
	defer rf.mu.Unlock()

	rf.dprint("Handle AppendEntries from leader %d", args.LeaderId)

	rf.resetLastHeard()
	reply.Term = rf.currentTerm

	if args.Term < rf.currentTerm {
		reply.Success = false
		rf.dprint("Incoming AppendEntries has lower term %d < %d. Rejected.", args.Term, rf.currentTerm)
		return
	}

	// the incoming args have higher term
	// change term, and become follower
	if args.Term > rf.currentTerm {
		rf.toFollower(args.Term)
	}

	rf.leader = args.LeaderId

	// if it's heartbeat, return
	if len(args.Entries) == 0 {
		reply.Success = true
		return
	}

	// if log doesn't contain an entry at prevLogIndex, or
	// if log contais the entry, but term doesn't matches prevLogTerm
	// return false
	// first index is 1
	if len(rf.log) < args.PrevLogIndex || rf.log[args.PrevLogIndex-1].Term != args.PrevLogTerm {
		reply.Success = false
		return
	}

	for i, log := range args.Entries {
		// if rf.log doesn't contain log.Index, append the rest
		if len(rf.log) < log.Index {
			rf.log = append(rf.log, args.Entries[i:]...)
			break
		} else {
			// rf.log has the same index
			existing := rf.log[log.Index-1]

			if existing.Term == log.Term {
				// ignore the entry, continue loop
			} else {
				// delete the entry and all following it
				rf.log = rf.log[:log.Index]
				// append the rest
				rf.log = append(rf.log, args.Entries[i:]...)
				break
			}
		}
	}

	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = intMin(args.LeaderCommit, rf.lastLog().Index)
	}

	reply.Success = true

}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
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

	if args.Term > rf.currentTerm {
		rf.dprint("Received higher term %d > %d. Become follower", args.Term, rf.currentTerm)
		rf.toFollower(args.Term)
	}

	lastLog := rf.lastLog()

	uptodate := (args.LastLogTerm > lastLog.Term) || (args.LastLogTerm == lastLog.Term && args.LastLogIndex >= lastLog.Index)

	if !uptodate {
		rf.dprint("Reject RequestVote from %d due to not update", args.CandidateId)
	} else if rf.votedFor == -1 || rf.votedFor == args.CandidateId {
		rf.dprint("Granted RequestVote from %d", args.CandidateId)

		reply.VoteGranted = true
		rf.votedFor = args.CandidateId
		rf.role = FOLLOWER
		rf.resetLastHeard()

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

	// Create the log entry
	log := Log{
		Index:   len(rf.log) + 1,
		Term:    rf.currentTerm,
		Command: command,
	}

	rf.log = append(rf.log, log)

	return log.Index, log.Term, true
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

	// reset lastBroadcast to broadcast immediately
	rf.lastBroadcastTime = time.Unix(0, 0)

	// reset volatile states
	rf.nextIndex = make([]int, len(rf.peers))

	lastLogIndex := rf.lastLog().Index + 1
	for i := 0; i < len(rf.peers); i++ {
		rf.nextIndex[i] = lastLogIndex
	}

	rf.matchIndex = make([]int, len(rf.peers))
}

func (rf *Raft) toFollower(term int) {
	rf.role = FOLLOWER
	rf.currentTerm = term
	rf.votedFor = -1
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

func (rf *Raft) electionLoop() {
	for !rf.killed() {

		time.Sleep(1 * time.Millisecond)

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

		rf.role = CANDIDATE
		rf.currentTerm++
		rf.votedFor = rf.me
		rf.resetLastHeard()

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
		case <-time.After(getElectionTimeout()):
			rf.ldprint("Election timeout. Restart.")
		}

		if result != nil {
			rf.mu.Lock()

			if rf.role != CANDIDATE {
				rf.mu.Unlock()
				return
			}

			// received higher term, this vote should be ignored
			if result.maxTerm > rf.currentTerm {
				rf.toFollower(result.maxTerm)
			}

			if result.voteCount > len(rf.peers)/2 {
				rf.dprint("Majority reached. Become leader.")
				rf.toLeader()
			} else {
				rf.dprint("No majority reached. Restart election")
			}
			rf.mu.Unlock()
		}
	}

}

func (rf *Raft) appendEntriesLoop() {
	for !rf.killed() {
		rf.mu.Lock()

		if rf.role != LEADER {
			rf.mu.Unlock()
			continue
		}

		if time.Since(rf.lastBroadcastTime) < HEARTBEAT_INTERVAL*time.Millisecond {
			rf.mu.Unlock()
			continue
		}

		rf.lastBroadcastTime = time.Now()

		lastLog := rf.lastLog()

		for i := range rf.peers {
			if i == rf.me {
				continue
			}

			args := AppendEntriesArgs{
				Term:         rf.currentTerm,
				LeaderId:     rf.me,
				PrevLogIndex: lastLog.Index,
				PrevLogTerm:  lastLog.Term,
				Entries:      make([]Log, 0),
				LeaderCommit: rf.commitIndex,
			}

			if rf.lastLog().Index >= rf.nextIndex[i] {
				args.Entries = rf.log[rf.nextIndex[i]:]
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
					if len(args.Entries) == 0 {
						rf.mu.Unlock()
						return
					}

					if reply.Success {
						rf.nextIndex[i] = args.Entries[len(args.Entries)-1].Index + 1
						rf.matchIndex[i] = rf.nextIndex[i] - 1
					} else {
						if rf.nextIndex[i] > 1 {
							rf.nextIndex[i]--
						}
					}

					N := rf.commitIndex + 1

					for ; N < len(rf.log); N++ {
						// check if log[N].term == currentTerm
						if rf.log[N].Term != rf.currentTerm {
							continue
						}

						// check if a majority of matchIndex[i] > N
						count := 0
						for i := range rf.matchIndex {
							if rf.matchIndex[i] >= N {
								count++
							}
						}
						if count > len(rf.matchIndex)/2 {
							break
						}
					}

					if N < len(rf.log) {
						rf.commitIndex = N
					}

					rf.mu.Unlock()
				}
			}(i)

		}

		rf.mu.Unlock()

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

	rf.resetLastHeard()

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// election
	go rf.electionLoop()
	// leader
	go rf.appendEntriesLoop()

	return rf
}

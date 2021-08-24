use std::cmp::min;
use std::sync::mpsc::{sync_channel, Receiver};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use futures::channel::mpsc::{unbounded, UnboundedSender};
use futures::channel::oneshot::{self, channel};
use futures::executor::block_on;
use futures::future::{join_all, Either};
use futures::stream::FuturesUnordered;
use futures::{join, select, stream, FutureExt, SinkExt, StreamExt, TryFutureExt};
use futures_timer::Delay;
use labcodec::Message;
use rand::Rng;

#[cfg(test)]
pub mod config;
pub mod errors;
pub mod persister;
#[cfg(test)]
mod tests;

use self::errors::*;
use self::persister::*;
use crate::proto::raftpb::*;

const HEARTBEAT_INTERVAL: u32 = 100;

const ELECTION_TIMEOUT_MIN: u32 = 250;
const ELECTION_TIMEOUT_MAX: u32 = 400;

fn get_random_election_timeout() -> u32 {
    let mut rng = rand::thread_rng();

    rng.gen_range(ELECTION_TIMEOUT_MIN, ELECTION_TIMEOUT_MAX)
}

pub struct ApplyMsg {
    pub command_valid: bool,
    pub command: Vec<u8>,
    pub command_index: u64,
}

/// State of a raft peer.
#[derive(Default, Clone, Debug)]
pub struct State {
    pub term: u64,
    pub is_leader: bool,
}

impl State {
    /// The current term of this peer.
    pub fn term(&self) -> u64 {
        self.term
    }
    /// Whether this peer believes it is the leader.
    pub fn is_leader(&self) -> bool {
        self.is_leader
    }
}

#[derive(PartialEq)]
enum RaftRole {
    Leader,
    Candidate,
    Follower,
}

type Term = u64;

// A single Raft peer.
pub struct Raft {
    // RPC end points of all peers
    peers: Vec<RaftClient>,
    // Object to hold this peer's persisted state
    persister: Box<dyn Persister>,
    // this peer's index into peers[]
    me: usize,
    // Your data here (2A, 2B, 2C).
    // Look at the paper's Figure 2 for a description of what
    // state a Raft server must maintain.
    role: RaftRole,
    current_term: u64,
    voted_for: Option<usize>,
    log: Vec<Log>,

    commit_index: usize,
    last_applied: usize,

    next_index: Vec<usize>,
    match_index: Vec<usize>,

    leader: Option<u64>,
    last_heard_time: SystemTime,
    election_timeout_time: SystemTime,

    last_broadcast_time: SystemTime,

    snapshot_last_term: Term,
    snapshot_last_index: usize,

    apply_ch: UnboundedSender<ApplyMsg>,
}

impl Raft {
    /// the service or tester wants to create a Raft server. the ports
    /// of all the Raft servers (including this one) are in peers. this
    /// server's port is peers[me]. all the servers' peers arrays
    /// have the same order. persister is a place for this server to
    /// save its persistent state, and also initially holds the most
    /// recent saved state, if any. apply_ch is a channel on which the
    /// tester or service expects Raft to send ApplyMsg messages.
    /// This method must return quickly.
    pub fn new(
        peers: Vec<RaftClient>,
        me: usize,
        persister: Box<dyn Persister>,
        apply_ch: UnboundedSender<ApplyMsg>,
    ) -> Raft {
        let raft_state = persister.raft_state();

        let peers_len = peers.len();

        // Your initialization code here (2A, 2B, 2C).
        let mut rf = Raft {
            peers,
            persister,
            me,
            apply_ch,
            role: RaftRole::Follower,
            current_term: 0,
            voted_for: None,
            leader: None,
            commit_index: 0,
            match_index: Vec::with_capacity(peers_len),
            next_index: Vec::with_capacity(peers_len),
            log: vec![Log {
                term: 0,
                index: 0,
                command: Vec::new(),
            }],
            last_applied: 0,
            election_timeout_time: UNIX_EPOCH,
            last_broadcast_time: UNIX_EPOCH,
            last_heard_time: UNIX_EPOCH,
            snapshot_last_index: 0,
            snapshot_last_term: 0,
        };

        // initialize from state persisted before a crash
        rf.restore(&raft_state);

        rf
    }

    /// save Raft's persistent state to stable storage,
    /// where it can later be retrieved after a crash and restart.
    /// see paper's Figure 2 for a description of what should be persistent.
    fn persist(&mut self) {
        // Your code here (2C).
        // Example:
        // labcodec::encode(&self.xxx, &mut data).unwrap();
        // labcodec::encode(&self.yyy, &mut data).unwrap();
        // self.persister.save_raft_state(data);
    }

    /// restore previously persisted state.
    fn restore(&mut self, data: &[u8]) {
        if data.is_empty() {
            // bootstrap without any state?
            return;
        }
        // Your code here (2C).
        // Example:
        // match labcodec::decode(data) {
        //     Ok(o) => {
        //         self.xxx = o.xxx;
        //         self.yyy = o.yyy;
        //     }
        //     Err(e) => {
        //         panic!("{:?}", e);
        //     }
        // }
    }

    /// example code to send a RequestVote RPC to a server.
    /// server is the index of the target server in peers.
    /// expects RPC arguments in args.
    ///
    /// The labrpc package simulates a lossy network, in which servers
    /// may be unreachable, and in which requests and replies may be lost.
    /// This method sends a request and waits for a reply. If a reply arrives
    /// within a timeout interval, This method returns Ok(_); otherwise
    /// this method returns Err(_). Thus this method may not return for a while.
    /// An Err(_) return can be caused by a dead server, a live server that
    /// can't be reached, a lost request, or a lost reply.
    ///
    /// This method is guaranteed to return (perhaps after a delay) *except* if
    /// the handler function on the server side does not return.  Thus there
    /// is no need to implement your own timeouts around this method.
    ///
    /// look at the comments in ../labrpc/src/lib.rs for more details.
    async fn send_request_vote(
        &self,
        server: usize,
        args: RequestVoteArgs,
    ) -> Result<RequestVoteReply> {
        // Your code here if you want the rpc becomes async.
        // Example:
        // ```
        // let peer = &self.peers[server];
        // let peer_clone = peer.clone();
        // let (tx, rx) = channel();
        // peer.spawn(async move {
        //     let res = peer_clone.request_vote(&args).await.map_err(Error::Rpc);
        //     tx.send(res).unwrap();
        // });

        // rx
        // ```
        // let (tx, rx) = sync_channel::<Result<RequestVoteReply>>(1);
        // crate::your_code_here((server, args, tx, rx))
        self.peers[server]
            .request_vote(&args)
            .await
            .map_err(Error::Rpc)
    }

    fn start<M>(&self, command: &M) -> Result<(u64, u64)>
    where
        M: labcodec::Message,
    {
        let index = 0;
        let term = 0;
        let is_leader = true;
        let mut buf = vec![];
        labcodec::encode(command, &mut buf).map_err(Error::Encode)?;
        // Your code here (2B).

        if is_leader {
            Ok((index, term))
        } else {
            Err(Error::NotLeader)
        }
    }

    fn log(&self, info: &str) {
        println!("{} [{}] {}", self.me, self.current_term, info);
    }

    fn to_follower(&mut self, term: Term) {
        self.role = RaftRole::Follower;
        self.current_term = term;
        self.voted_for = None;
        self.persist();
        self.leader = None;
    }

    async fn to_leader(&mut self) {
        self.role = RaftRole::Leader;

        let last_log_index = self.last_log().index + 1;
        for i in 0..self.peers.len() {
            self.next_index[i] = last_log_index;
            self.match_index[i] = 0;
        }

        self.broadcast().await;
    }

    fn last_log(&self) -> LastLogInfo {
        let last_log = self.log.last().unwrap();
        LastLogInfo {
            index: last_log.index as usize,
            term: last_log.term,
        }
    }

    fn is_up_to_date(&self, last_log_index: usize, last_log_term: Term) -> bool {
        let last_log = self.last_log();
        last_log_term > last_log.term
            || (last_log_term == last_log.term && last_log_index >= last_log.index)
    }

    fn reset_last_heard(&mut self) {
        self.last_heard_time = SystemTime::now();
        self.election_timeout_time =
            self.last_heard_time + Duration::from_millis(get_random_election_timeout() as u64);
    }

    fn commit_message(&mut self, to: usize) {
        self.commit_index = to;
        self.log(&format!("commit_index to {}", to));

        if self.commit_index > self.last_applied {
            let mut messages = Vec::with_capacity(self.commit_index - self.last_applied);

            for (i, log) in self.log[self.last_applied + 1 - self.snapshot_last_index
                ..self.commit_index + 1 - self.snapshot_last_index]
                .iter()
                .enumerate()
            {
                messages.push(ApplyMsg {
                    command: log.command.clone(),
                    command_valid: true,
                    command_index: (self.last_applied + 1 + i) as u64,
                });
            }

            self.last_applied = self.commit_index;

            let mut apply_ch = self.apply_ch.clone();

            // start a thread to send messages
            thread::spawn(move || {
                block_on(async {
                    apply_ch
                        .send_all(&mut stream::iter(messages).map(|x| Ok(x)))
                        .await
                        .unwrap();
                });
            });
        }
    }

    async fn broadcast(&mut self) {
        self.last_broadcast_time = SystemTime::now();

        for (i, client) in self.peers.iter().enumerate() {
            if i == self.me {
                continue;
            }

            if self.next_index[i] - 1 < self.snapshot_last_index {
                self.log(&format!(
                    "{} is too laggy {} < {}. InstallSnapshot",
                    i,
                    self.next_index[i] - 1,
                    self.snapshot_last_index
                ));

                let last_included_index = self.snapshot_last_index as u64;
                let last_included_term = self.snapshot_last_term;

                let args = InstallSnapshotArgs {
                    term: self.current_term as u64,
                    leader_id: self.me as u64,
                    last_included_index: last_included_index,
                    last_included_term: last_included_term,
                    data: self.persister.snapshot(),
                };

                client.install_snapshot(&args);
            }
        }
    }
}
struct LastLogInfo {
    index: usize,
    term: Term,
}

impl Raft {
    /// Only for suppressing deadcode warnings.
    #[doc(hidden)]
    pub fn __suppress_deadcode(&mut self) {
        let _ = self.start(&0);
        let _ = self.send_request_vote(0, Default::default());
        self.persist();
        let _ = &self.me;
        let _ = &self.persister;
        let _ = &self.peers;
    }
}

// Choose concurrency paradigm.
//
// You can either drive the raft state machine by the rpc framework,
//
// ```rust
// struct Node { raft: Arc<Mutex<Raft>> }
// ```
//
// or spawn a new thread runs the raft state machine and communicate via
// a channel.
//
// ```rust
// struct Node { sender: Sender<Msg> }
// ```
#[derive(Clone)]
pub struct Node {
    raft: Arc<Mutex<Raft>>,
}

impl Node {
    /// Create a new raft service.
    pub fn new(raft: Raft) -> Node {
        // Your code here.
        let node = Node {
            raft: Arc::new(Mutex::new(raft)),
        };

        let node_clone = node.clone();

        // start election loop
        thread::spawn(move || {
            block_on(async {
                node_clone.election_loop().await;
            });
        });

        node
    }

    /// the service using Raft (e.g. a k/v server) wants to start
    /// agreement on the next command to be appended to Raft's log. if this
    /// server isn't the leader, returns [`Error::NotLeader`]. otherwise start
    /// the agreement and return immediately. there is no guarantee that this
    /// command will ever be committed to the Raft log, since the leader
    /// may fail or lose an election. even if the Raft instance has been killed,
    /// this function should return gracefully.
    ///
    /// the first value of the tuple is the index that the command will appear
    /// at if it's ever committed. the second is the current term.
    ///
    /// This method must return without blocking on the raft.
    pub fn start<M>(&self, command: &M) -> Result<(u64, u64)>
    where
        M: labcodec::Message,
    {
        // Your code here.
        // Example:
        // self.raft.start(command)
        crate::your_code_here(command)
    }

    /// The current term of this peer.
    pub fn term(&self) -> u64 {
        // Your code here.
        // Example:
        // self.raft.term
        self.raft.lock().unwrap().current_term
    }

    /// Whether this peer believes it is the leader.
    pub fn is_leader(&self) -> bool {
        // Your code here.
        // Example:
        // self.raft.leader_id == self.id
        self.raft.lock().unwrap().role == RaftRole::Leader
    }

    /// The current state of this peer.
    pub fn get_state(&self) -> State {
        let locked = self.raft.lock().unwrap();

        State {
            term: locked.current_term,
            is_leader: locked.role == RaftRole::Leader,
        }
    }

    /// the tester calls kill() when a Raft instance won't be
    /// needed again. you are not required to do anything in
    /// kill(), but it might be convenient to (for example)
    /// turn off debug output from this instance.
    /// In Raft paper, a server crash is a PHYSICAL crash,
    /// A.K.A all resources are reset. But we are simulating
    /// a VIRTUAL crash in tester, so take care of background
    /// threads you generated with this Raft Node.
    pub fn kill(&self) {
        // Your code here, if desired.
    }

    async fn election_loop(&self) {
        loop {
            Delay::new(Duration::from_millis(10)).await;

            let mut rf = self.raft.lock().unwrap();

            if rf.role == RaftRole::Leader {
                continue;
            }

            if SystemTime::now().lt(&rf.election_timeout_time) {
                continue;
            } else {
                rf.log(&format!("Election started"));
            }

            rf.role = RaftRole::Candidate;
            rf.current_term += 1;
            rf.voted_for = Some(rf.me);
            rf.persist();

            rf.reset_last_heard();

            rf.log(&format!(
                "Starting requesting votes for term {}",
                rf.current_term
            ));

            let last_log = rf.last_log();

            let args = RequestVoteArgs {
                term: rf.current_term,
                candidate_id: rf.me as u64,
                last_log_index: last_log.index as u64,
                last_log_term: last_log.term,
            };

            let mut tasks = FuturesUnordered::new();
            for (i, _) in rf.peers.iter().enumerate() {
                if i != rf.me {
                    tasks.push(rf.send_request_vote(i, args.clone()));
                }
            }

            let mut vote_handler = Box::pin(
                async move {
                    let mut finish_count = 1;
                    let mut vote_count = 1;
                    let mut max_term = 0;

                    let peers_len = tasks.len() + 1;

                    while let Some(value) = tasks.next().await {
                        finish_count += 1;
                        if let Ok(reply) = value {
                            if reply.vote_granted {
                                vote_count += 1;
                            }
                            if reply.term > max_term {
                                max_term = reply.term;
                            }
                        }
                        if finish_count == peers_len || vote_count > peers_len / 2 {
                            break;
                        }
                    }

                    (finish_count, vote_count, max_term)
                }
                .fuse(),
            );

            let timeout = rf.election_timeout_time;

            let original_term = rf.current_term;

            let mut timeout_delay =
                Delay::new(SystemTime::now().duration_since(timeout).unwrap()).fuse();

            select! {
                (_finish_count, vote_count, max_term) = vote_handler => {
                    let mut rf = self.raft.lock().unwrap();
                    rf.log("Election completed.");

                    if rf.role != RaftRole::Candidate || rf.current_term != original_term {
                        continue;
                    }
                    if max_term > rf.current_term {
                        rf.to_follower(max_term);
                    } else if vote_count > rf.peers.len() / 2 {
                        rf.log("Majority reached. Become leader");
                        rf.to_leader().await;
                    } else {
                        rf.log("No majority reached. Back to follower and restart election.");
                    }
                },
                () = timeout_delay => {
                    rf.log("Election timeout. Back to follower and restart.");
                },
            }
        }
    }
}

#[async_trait::async_trait]
impl RaftService for Node {
    // example RequestVote RPC handler.
    //
    // CAVEATS: Please avoid locking or sleeping here, it may jam the network.
    async fn request_vote(&self, args: RequestVoteArgs) -> labrpc::Result<RequestVoteReply> {
        // Your code here (2A, 2B).
        let mut rf = self.raft.lock().unwrap();

        rf.log(&format!("Received request vote from {}", args.candidate_id));

        let mut reply = RequestVoteReply {
            term: rf.current_term,
            vote_granted: false,
        };

        if args.term < rf.current_term {
            rf.log(&format!(
                "Rejected RequestVote from {} due to term",
                args.candidate_id
            ));
            return Ok(reply);
        }

        if args.term > rf.current_term {
            rf.log(&format!(
                "Received higher term {} > {}. Become follower",
                args.term, rf.current_term,
            ));
            rf.to_follower(args.term);
        }

        if None == rf.voted_for || rf.voted_for == Some(args.candidate_id as usize) {
            if rf.is_up_to_date(args.last_log_index as usize, args.last_log_term) {
                rf.log(&format!("Granted RequestVote from {}", args.candidate_id));

                reply.vote_granted = true;

                rf.voted_for = Some(args.candidate_id as usize);

                rf.persist();

                rf.role = RaftRole::Follower;
            } else {
                rf.log(&format!(
                    "Rejected RequestVote from {} due to not update",
                    args.candidate_id
                ));
            }
        }

        Ok(reply)
    }

    async fn append_entries(&self, args: AppendEntriesArgs) -> labrpc::Result<AppendEntriesReply> {
        let mut rf = self.raft.lock().unwrap();

        rf.log(&format!(
            "Handling AppendEntries from leader {}. Log size {}",
            args.leader_id, 0
        ));

        let mut reply = AppendEntriesReply {
            term: rf.current_term,
            conflicting_entry_term: -1,
            first_index_for_term: -1,
            success: false,
        };

        if args.term < rf.current_term {
            rf.log(&format!(
                "Incoming AppendEntries has lower term {} < {}. Rejected",
                args.term, rf.current_term
            ));
            return Ok(reply);
        }

        rf.reset_last_heard();

        if args.term > rf.current_term {
            rf.to_follower(args.term);
        }

        rf.leader = Some(args.leader_id);

        if ((rf.log.len() + rf.snapshot_last_index) as u64) < args.prev_log_index {
            rf.log("Doesn't have PrevLogIndex. Rejects");
            reply.conflicting_entry_term = -1;
            reply.first_index_for_term = (rf.last_log().index + 1) as i64;
            return Ok(reply);
        }

        let last_log_index = (args.prev_log_index as usize) - rf.snapshot_last_index;

        let last_log_term = if last_log_index == 0 {
            rf.snapshot_last_term
        } else {
            rf.log[last_log_index].term
        };

        if last_log_term != args.prev_log_term {
            rf.log("Log inconsistency. Rejected");

            reply.conflicting_entry_term = last_log_term as i64;

            let mut first_index_for_term = args.prev_log_index as usize;

            while first_index_for_term >= rf.snapshot_last_index
                && rf.log[first_index_for_term - rf.snapshot_last_index - 1].term == last_log_term
            {
                first_index_for_term -= 1;
            }

            reply.first_index_for_term = first_index_for_term as i64;

            return Ok(reply);
        }

        for (i, log) in args.entries.iter().enumerate() {
            if rf.log.len() + rf.snapshot_last_index <= log.index as usize {
                rf.log.extend_from_slice(&args.entries[i..]);
                rf.persist();
                break;
            } else {
                let actual_index = (log.index as usize) - rf.snapshot_last_index;

                let existing = &rf.log[actual_index];

                if existing.term == log.term {
                    // ignored
                } else {
                    rf.log.drain(..actual_index);
                    rf.log.extend_from_slice(&args.entries[i..]);
                    rf.persist();
                    break;
                }
            }
        }

        rf.log(&format!(
            "LeaderCommit {}, commitIndex {}",
            args.leader_commit, rf.commit_index
        ));

        if args.leader_commit > (rf.commit_index as u64) {
            let n = min(args.leader_commit as usize, rf.last_log().index);
            rf.commit_message(n);
        }

        Ok(reply)
    }

    async fn install_snapshot(
        &self,
        req: InstallSnapshotArgs,
    ) -> labrpc::Result<InstallSnapshotReply> {
        todo!()
    }
}

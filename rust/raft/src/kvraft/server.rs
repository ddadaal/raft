use std::borrow::Borrow;
use std::collections::{HashMap, HashSet};
use std::hash::Hash;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Duration;

use futures::channel::mpsc::{unbounded, UnboundedReceiver, UnboundedSender};
use futures::channel::oneshot;
use futures::executor::block_on;
use futures::future::join_all;
use futures::lock::MutexGuard;
use futures::{join, select, Future, FutureExt, SinkExt, StreamExt};
use futures_timer::Delay;

use crate::proto::kvraftpb::*;
use crate::raft::errors::Error;
use crate::raft::{self, ApplyMsg};

#[derive(Debug)]
pub enum ListenResult {
    Completed(String),
    LeaderChange,
}

pub struct KvServer {
    pub rf: raft::Node,
    me: usize,
    // snapshot if log grows this big
    maxraftstate: Option<usize>,
    // Your definitions here.
    hashmap: HashMap<String, String>,
    apply_ch: Option<UnboundedReceiver<ApplyMsg>>,

    // every highest committed id for each client
    highest_committed_id: HashMap<String, u64>,

    // key is request id, value is sender
    results: HashMap<u64, oneshot::Sender<ListenResult>>,
}

impl KvServer {
    pub fn new(
        servers: Vec<crate::proto::raftpb::RaftClient>,
        me: usize,
        persister: Box<dyn raft::persister::Persister>,
        maxraftstate: Option<usize>,
    ) -> KvServer {
        // You may need initialization code here.

        let (tx, apply_ch) = unbounded();
        let rf = raft::Raft::new(servers, me, persister, tx);

        let rf_node = raft::Node::new(rf);

        let server = KvServer {
            rf: rf_node,
            me,
            maxraftstate,
            hashmap: HashMap::new(),
            apply_ch: Some(apply_ch),
            highest_committed_id: HashMap::new(),
            results: HashMap::new(),
        };

        server
    }

    fn log(&self, log: &str) {
        println!("{} {}", self.me, log);
    }

    pub fn register(&mut self, request_id: u64) -> oneshot::Receiver<ListenResult> {
        let (sender, receiver) = oneshot::channel();

        self.results.insert(request_id, sender);

        receiver
    }

    fn apply_request(&mut self, arg: Request) -> String {
        match arg.op() {
            OpType::Get => {
                // call the listeners and remove the items
                let value = self
                    .hashmap
                    .get(&arg.key)
                    .map_or_else(|| String::new(), |x| x.to_string());

                value
            }
            OpType::Put => {
                self.hashmap.insert(arg.key, arg.value.to_string());
                arg.value
            }
            OpType::Append => {
                let value = self.hashmap.get(&arg.key);
                match value {
                    Some(v) => {
                        let new_value = format!("{}{}", v, arg.value);
                        self.hashmap.insert(arg.key, new_value.to_string());
                        new_value
                    }
                    None => {
                        self.hashmap.insert(arg.key, arg.value.to_string());
                        arg.value
                    }
                }
            }
        }
    }
}

impl KvServer {
    /// Only for suppressing deadcode warnings.
    #[doc(hidden)]
    pub fn __suppress_deadcode(&mut self) {
        let _ = &self.me;
        let _ = &self.maxraftstate;
    }
}

// Choose concurrency paradigm.
//
// You can either drive the kv server by the rpc framework,
//
// ```rust
// struct Node { server: Arc<Mutex<KvServer>> }
// ```
//
// or spawn a new thread runs the kv server and communicate via
// a channel.
//
// ```rust
// struct Node { sender: Sender<Msg> }
// ```
#[derive(Clone)]
pub struct Node {
    // Your definitions here.
    server: Arc<Mutex<KvServer>>,
}

impl Node {
    pub fn new(mut kv: KvServer) -> Node {
        // Your code here.
        // crate::your_code_here(kv);

        let mut apply_ch = kv.apply_ch.take().unwrap();
        let max_state_size = kv.maxraftstate.take();

        let server = Arc::new(Mutex::new(kv));

        let server_clone = server.clone();

        thread::spawn(move || {
            block_on(async {
                while let Some(message) = apply_ch.next().await {
                    let mut server = server_clone.lock().unwrap();

                    server.log(&format!("Received commit index {}", message.command_index));

                    if message.command_valid {
                        let arg = labcodec::decode::<Request>(&message.command).unwrap();

                        let arg_id = arg.id;

                        // duplicate entry, sending the result directly
                        // https://zhuanlan.zhihu.com/p/258338915
                        if let Some(highest_id) = server.highest_committed_id.get(&arg.client_name)
                        {
                            if arg.id <= *highest_id {
                                if let Some(sender) = server.results.remove(&arg_id) {
                                    if let Some(value) = server.hashmap.get(&arg.key) {
                                        let _ =
                                            sender.send(ListenResult::Completed(value.to_string()));
                                    }
                                }
                                continue;
                            }
                        }

                        server
                            .highest_committed_id
                            .insert(arg.client_name.to_string(), arg.id);

                        let value = server.apply_request(arg);

                        if let Some(sender) = server.results.remove(&arg_id) {
                            let _ = sender.send(ListenResult::Completed(value.to_string()));
                        }

                        if let Some(max_state_size) = max_state_size {
                            if server.rf.should_snapshot(max_state_size) {
                                server.log(&format!(
                                    "Making snapshot to index {}",
                                    message.command_index
                                ));

                                // snapshot current hashmap
                                let mut buf = Vec::new();
                                labcodec::encode(
                                    &Snapshot {
                                        kv: server.hashmap.clone(),
                                        highest_committed_id: server.highest_committed_id.clone(),
                                    },
                                    &mut buf,
                                )
                                .unwrap();

                                // save snapshot
                                server.rf.snapshot(message.command_index, buf);
                            }
                        }
                    } else {
                        // is snapshot. update snapshot
                        let snapshot = labcodec::decode::<Snapshot>(&message.command).unwrap();

                        // update the hashmap
                        server.hashmap.clear();
                        server.hashmap.extend(snapshot.kv.into_iter());

                        // update highest_committed_id
                        server.highest_committed_id.clear();
                        server
                            .highest_committed_id
                            .extend(snapshot.highest_committed_id.into_iter());
                    }
                }
            });
        });

        Node { server }
    }

    /// the tester calls kill() when a KVServer instance won't
    /// be needed again. you are not required to do anything
    /// in kill(), but it might be convenient to (for example)
    /// turn off debug output from this instance.
    pub fn kill(&self) {
        // If you want to free some resources by `raft::Node::kill` method,
        // you should call `raft::Node::kill` here also to prevent resource leaking.
        // Since the test framework will call kvraft::Node::kill only.
        self.server.lock().unwrap().rf.kill();

        // Your code here, if desired.
    }

    /// The current term of this peer.
    pub fn term(&self) -> u64 {
        self.get_state().term()
    }

    /// Whether this peer believes it is the leader.
    pub fn is_leader(&self) -> bool {
        self.get_state().is_leader()
    }

    pub fn get_state(&self) -> raft::State {
        // Your code here.
        self.server.lock().unwrap().rf.get_state()
    }
}

#[async_trait::async_trait]
impl KvService for Node {
    // CAVEATS: Please avoid locking or sleeping here, it may jam the network.
    async fn request_op(&self, req: Request) -> labrpc::Result<Reply> {
        // Your code here.
        let mut rx = {
            let mut server = self.server.lock().unwrap();

            let id = req.id;

            let r = server.rf.start(&req);

            if let Err(e) = r {
                return match e {
                    Error::NotLeader => Ok(Reply {
                        wrong_leader: true,
                        value: "".into(),
                    }),
                    _ => Err(labrpc::Error::Other("".into())),
                };
            }

            let (index, term) = r.unwrap();
            server.log(&format!(
                "Command id {} is inserted into raft. index {}, term {}",
                id, index, term
            ));

            server.register(id)
        };

        // let result = rx.await.unwrap();
        let result = select! {
            value = rx => value.unwrap(),
            () = Delay::new(Duration::from_millis(500)).fuse() => ListenResult::LeaderChange,
        };

        Ok(match result {
            ListenResult::Completed(value) => Reply {
                wrong_leader: false,
                value,
            },
            ListenResult::LeaderChange => Reply {
                wrong_leader: true,
                value: "".into(),
            },
        })
    }
}

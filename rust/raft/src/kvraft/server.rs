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
use futures::{join, select, FutureExt, SinkExt, StreamExt};
use futures_timer::Delay;

use crate::proto::kvraftpb::*;
use crate::raft::errors::Error;
use crate::raft::{self, ApplyMsg};

pub enum ListenResult<T> {
    Completed(T),
    LeaderChange,
}

pub struct KvServer {
    pub rf: raft::Node,
    me: usize,
    // snapshot if log grows this big
    maxraftstate: Option<usize>,
    // Your definitions here.
    hashmap: HashMap<String, String>,
    latest_index: u64,
    apply_ch: Option<UnboundedReceiver<ApplyMsg>>,

    // key is id, value is result
    executed_but_not_responded_ids: HashMap<u64, String>,

    // key is index, value is (id, sender)
    listeners: HashMap<u64, (u64, oneshot::Sender<ListenResult<String>>)>,
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
            latest_index: 0,
            apply_ch: Some(apply_ch),
            executed_but_not_responded_ids: HashMap::new(),
            listeners: HashMap::new(),
        };

        server
    }

    fn log(&self, log: &str) {
        println!("{} {}", self.me, log);
    }

    pub fn register(
        &mut self,
        request_id: u64,
        index: u64,
    ) -> oneshot::Receiver<ListenResult<String>> {
        let (sender, receiver) = oneshot::channel();
        // check if the index has already been executed
        let result = self.executed_but_not_responded_ids.remove(&request_id);

        if let Some(result) = result {
            sender.send(ListenResult::Completed(result));
            return receiver;
        }

        self.listeners.insert(index, (request_id, sender));
        receiver
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
    fn execute_request(server: &mut std::sync::MutexGuard<KvServer>, arg: RaftRequest) -> String {
        match arg.req.unwrap() {
            raft_request::Req::Get(get) => {
                // call the listeners and remove the items
                let value = server
                    .hashmap
                    .get(&get.key)
                    .map_or_else(|| String::new(), |x| x.to_string());
                value
            }
            raft_request::Req::Put(arg) => {
                if arg.op == Op::Put as i32 {
                    server.hashmap.insert(arg.key, arg.value.to_string());
                    arg.value
                } else {
                    let value = server.hashmap.get(&arg.key);
                    match value {
                        Some(v) => {
                            let new_value = format!("{}{}", v, arg.value);
                            server.hashmap.insert(arg.key, new_value.to_string());
                            new_value
                        }
                        None => {
                            server.hashmap.insert(arg.key, arg.value.to_string());
                            arg.value
                        }
                    }
                }
            }
        }
    }

    pub fn new(mut kv: KvServer) -> Node {
        // Your code here.
        // crate::your_code_here(kv);

        let mut apply_ch = kv.apply_ch.take().unwrap();

        let server = Arc::new(Mutex::new(kv));

        let server_clone = server.clone();

        thread::spawn(move || {
            block_on(async {
                while let Some(message) = apply_ch.next().await {
                    let mut server = server_clone.lock().unwrap();

                    server.log(&format!("Received commit index {}", message.command_index));

                    server.latest_index = message.command_index;

                    let arg = labcodec::decode::<RaftRequest>(&message.command).unwrap();

                    if let Some((id, sender)) = server.listeners.remove(&message.command_index) {
                        if id != arg.id {
                            sender.send(ListenResult::LeaderChange);
                            continue;
                        }

                        let value = Node::execute_request(&mut server, arg);
                        let _ = sender.send(ListenResult::Completed(value));
                    } else {
                        let arg_id = arg.id;
                        let value = Node::execute_request(&mut server, arg);
                        server.executed_but_not_responded_ids.insert(arg_id, value);
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
    async fn get(&self, arg: GetRequest) -> labrpc::Result<GetReply> {
        // Your code here.
        let mut rx = {
            let mut server = self.server.lock().unwrap();

            let id = arg.id;

            let r = server.rf.start(&RaftRequest {
                req: Some(raft_request::Req::Get(arg)),
                id,
            });

            if let Err(e) = r {
                return match e {
                    Error::NotLeader => Ok(GetReply {
                        wrong_leader: true,
                        value: "".into(),
                    }),
                    _ => Err(labrpc::Error::Other("".into())),
                };
            }

            let (index, term) = r.unwrap();

            server.register(id, index)
        };

        let result = rx.await.unwrap();

        Ok(match result {
            ListenResult::Completed(value) => GetReply {
                wrong_leader: false,
                value,
            },
            ListenResult::LeaderChange => GetReply {
                wrong_leader: true,
                value: "".into(),
            },
        })
    }

    // CAVEATS: Please avoid locking or sleeping here, it may jam the network.
    async fn put_append(&self, arg: PutAppendRequest) -> labrpc::Result<PutAppendReply> {
        // Your code here.
        let mut rx = {
            let mut server = self.server.lock().unwrap();

            let id = arg.id;
            // write the log
            let r = server.rf.start(&RaftRequest {
                req: Some(raft_request::Req::Put(arg)),
                id,
            });

            if let Err(e) = r {
                return match e {
                    Error::NotLeader => Ok(PutAppendReply { wrong_leader: true }),
                    _ => Err(labrpc::Error::Other("".into())),
                };
            }

            let (index, term) = r.unwrap();
            server.register(id, index)
        };

        let result = rx.await.unwrap();

        Ok(match result {
            ListenResult::Completed(value) => PutAppendReply {
                wrong_leader: false,
            },
            ListenResult::LeaderChange => PutAppendReply { wrong_leader: true },
        })
    }
}

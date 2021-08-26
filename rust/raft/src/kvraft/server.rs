use std::borrow::Borrow;
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Duration;

use futures::channel::mpsc::{unbounded, UnboundedReceiver, UnboundedSender};
use futures::channel::oneshot;
use futures::executor::block_on;
use futures::future::join_all;
use futures::{join, SinkExt, StreamExt};
use futures_timer::Delay;

use crate::proto::kvraftpb::*;
use crate::raft::errors::Error;
use crate::raft::{self, ApplyMsg};

pub struct KvServer {
    pub rf: raft::Node,
    me: usize,
    // snapshot if log grows this big
    maxraftstate: Option<usize>,
    // Your definitions here.
    hashmap: HashMap<String, String>,
    latest_index: u64,
    apply_ch: Option<UnboundedReceiver<ApplyMsg>>,

    put_listeners: HashMap<u64, Vec<futures::channel::mpsc::UnboundedSender<()>>>,
    get_listeners: HashMap<u64, Vec<futures::channel::mpsc::UnboundedSender<String>>>,
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
            put_listeners: HashMap::new(),
            get_listeners: HashMap::new(),
        };

        server
    }

    fn log(&self, log: &str) {
        println!("{} {}", self.me, log);
    }

    pub fn register_put(&mut self, index: u64, sender: UnboundedSender<()>) {
        let registered = self.put_listeners.get_mut(&index);
        if let Some(v) = registered {
            v.push(sender);
        } else {
            self.put_listeners.insert(index, vec![sender]);
        }
    }

    pub fn register_get(&mut self, index: u64, sender: UnboundedSender<String>) {
        let registered = self.get_listeners.get_mut(&index);
        if let Some(v) = registered {
            v.push(sender);
        } else {
            self.get_listeners.insert(index, vec![sender]);
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

        let server = Arc::new(Mutex::new(kv));

        let server_clone = server.clone();

        thread::spawn(move || {
            block_on(async {
                while let Some(message) = apply_ch.next().await {
                    let mut server = server_clone.lock().unwrap();

                    server.log(&format!("Received commit index {}", message.command_index));

                    server.latest_index = message.command_index;

                    let arg = labcodec::decode::<RaftRequest>(&message.command).unwrap();

                    match arg.req.unwrap() {
                        raft_request::Req::Get(get) => {
                            // call the listeners and remove the items
                            let value = server
                                .hashmap
                                .get(&get.key)
                                .map_or_else(|| String::new(), |x| x.to_string());

                            let listeners = server.get_listeners.get_mut(&message.command_index);
                            if let Some(v) = listeners {
                                join_all(v.iter_mut().map(|x| x.send(value.to_string()))).await;
                            }
                            server.put_listeners.remove(&message.command_index);
                        }
                        raft_request::Req::Put(arg) => {
                            if arg.op == Op::Put as i32 {
                                server.hashmap.insert(arg.key, arg.value);
                            } else {
                                let value = server.hashmap.get(&arg.key);
                                match value {
                                    Some(v) => {
                                        let value = v.to_string();
                                        server
                                            .hashmap
                                            .insert(arg.key, format!("{}{}", value, arg.value));
                                    }
                                    None => {
                                        server.hashmap.insert(arg.key, arg.value);
                                    }
                                }
                            }

                            // call the listeners and remove the items
                            let listeners = server.put_listeners.get_mut(&message.command_index);
                            if let Some(v) = listeners {
                                join_all(v.iter_mut().map(|x| x.send(()))).await;
                            }
                            server.put_listeners.remove(&message.command_index);
                        }
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

            let r = server.rf.start(&RaftRequest {
                req: Some(raft_request::Req::Get(arg)),
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
            let (tx, rx) = unbounded();

            server.register_get(index, tx);

            rx
        };

        let value = rx.next().await.unwrap();

        Ok(GetReply {
            wrong_leader: false,
            value,
        })
    }

    // CAVEATS: Please avoid locking or sleeping here, it may jam the network.
    async fn put_append(&self, arg: PutAppendRequest) -> labrpc::Result<PutAppendReply> {
        // Your code here.
        let mut rx = {
            let mut server = self.server.lock().unwrap();

            // write the log
            let r = server.rf.start(&RaftRequest {
                req: Some(raft_request::Req::Put(arg)),
            });

            if let Err(e) = r {
                return match e {
                    Error::NotLeader => Ok(PutAppendReply {
                        wrong_leader: true,
                        err: "".into(),
                    }),
                    _ => Err(labrpc::Error::Other("".into())),
                };
            }

            let (index, term) = r.unwrap();
            let (tx, rx) = unbounded();

            server.register_put(index, tx);

            rx
        };

        // wait for the latest_index to reach the index
        rx.next().await;

        Ok(PutAppendReply {
            err: "".into(),
            wrong_leader: false,
        })
    }
}

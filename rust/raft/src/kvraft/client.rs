use std::{
    borrow::Borrow,
    cell::{Cell, RefCell},
    fmt,
    sync::atomic::{AtomicU64, Ordering},
    thread,
    time::Duration,
};

use futures::{executor::block_on, select, Future, FutureExt};
use futures_timer::Delay;
use labrpc::RpcFuture;

use crate::proto::kvraftpb::*;

static ID: AtomicU64 = AtomicU64::new(0);

fn get_and_increment_id() -> u64 {
    ID.fetch_add(1, Ordering::SeqCst)
}

enum Op {
    Put(String, String),
    Append(String, String),
}

pub struct Clerk {
    pub name: String,
    pub servers: Vec<KvClient>,
    // You will have to modify this struct.
    leader_index: Cell<Option<usize>>,
}

impl fmt::Debug for Clerk {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Clerk").field("name", &self.name).finish()
    }
}

impl Clerk {
    pub fn new(name: String, servers: Vec<KvClient>) -> Clerk {
        // You'll have to add code here.
        Clerk {
            name,
            servers,
            leader_index: Cell::new(None),
        }
        // crate::your_code_here((name, servers))
    }

    fn log(&self, log: &str) {
        println!("Clerk {}: {}", self.name, log);
    }

    fn execute<FExecute, FRetry, T>(&self, f: FExecute, retry: FRetry) -> T
    where
        FExecute: Fn(usize) -> RpcFuture<labrpc::Result<T>>,
        FRetry: Fn(&T) -> bool,
    {
        let leader_index = self.leader_index.get().unwrap_or(0);

        for i in (0..self.servers.len()).cycle().skip(leader_index) {
            self.log(&format!("Sending to server {}", i));
            // let value = block_on(async move {
            //     select! {
            //         value = f(i).fuse() => Some(value),
            //         () = Delay::new(Duration::from_millis(200)).fuse() => None,
            //     }
            // });
            // let value =
            if let Ok(reply) = block_on(f(i)) {
                if retry(&reply) {
                    self.log(&format!("Server {} is not leader. Change", i));
                    continue;
                } else {
                    self.log(&format!("Server {} is leader. Executed", i));
                    self.leader_index.set(Some(i));
                    return reply;
                }
            }
        }

        unreachable!()
    }

    /// fetch the current value for a key.
    /// returns "" if the key does not exist.
    /// keeps trying forever in the face of all other errors.
    //
    // you can send an RPC with code like this:
    // if let Some(reply) = self.servers[i].get(args).wait() { /* do something */ }
    pub fn get(&self, key: String) -> String {
        // You will have to modify this function.
        // crate::your_code_here(key)

        let args = GetRequest {
            key,
            id: get_and_increment_id(),
        };

        self.log(&format!("Request: {:?}", args));

        let reply = self.execute(|i| self.servers[i].get(&args), |x| x.wrong_leader);

        reply.value
    }

    /// shared by Put and Append.
    //
    // you can send an RPC with code like this:
    // let reply = self.servers[i].put_append(args).unwrap();
    fn put_append(&self, op: Op) {
        // You will have to modify this function.
        let args = match op {
            Op::Append(key, value) => PutAppendRequest {
                key,
                value,
                op: crate::proto::kvraftpb::Op::Append as i32,
                id: get_and_increment_id(),
            },
            Op::Put(key, value) => PutAppendRequest {
                key,
                value,
                op: crate::proto::kvraftpb::Op::Put as i32,
                id: get_and_increment_id(),
            },
        };

        self.log(&format!("Request: {:?}", args));

        self.execute(|i| self.servers[i].put_append(&args), |x| x.wrong_leader);
    }

    pub fn put(&self, key: String, value: String) {
        self.put_append(Op::Put(key, value))
    }

    pub fn append(&self, key: String, value: String) {
        self.put_append(Op::Append(key, value))
    }
}

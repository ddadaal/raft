# Raft Lab

MIT 6.824 lab 2.

# [Go Implementation](go/)

完成了2021 [MIT 6.824 Lab2 Raft](https://pdos.csail.mit.edu/6.824/labs/lab-raft.html)的所有4个Part：

- [x] 2a leader election
- [x] 2b log
- [x] 2c persistence
- [x] 2d log compaction

还完成了以下几个优化工作：

- [x] No-op Entry ([Raft Extended Paper](https://raft.github.io/raft.pdf) Section 8)
- [x] Pre Vote ([Four modifications for the Raft consensus algorithm](https://www.openlife.cc/sites/default/files/4-modifications-for-Raft-consensus.pdf))
- [x] Leader Stickiness (来源同上)

# [Rust Implementation](rust/)

[PingCAP的Talent Plan课程](https://github.com/pingcap/talent-plan/tree/master/courses/dss)的Rust实现的Raft部分

- [x] 2a leader election
- [x] 2b log
- [x] 2c persistence
- [x] 3a kv
- [ ] 3b kv with snapshot

# Raft

PKUEECS 研究生课程 分布式系统概念与设计 大作业：Raft算法实现。

完成了2021 [MIT 6.824 Lab2 Raft](https://pdos.csail.mit.edu/6.824/labs/lab-raft.html)的所有4个Part：

- leader election
- log
- persistence
- log compaction

还完成了以下几个优化工作：

- No-op Entry ([Raft Extended Paper](https://raft.github.io/raft.pdf) Section 8)
- Pre Vote ([Four modifications for the Raft consensus algorithm](https://www.openlife.cc/sites/default/files/4-modifications-for-Raft-consensus.pdf))
- Leader Stickiness (来源同上)


# 优化

默认只开启了Prevote，需要修改的话可以进入`raft/raft.go:41`修改feature flag。

注意：开启NOOP_LOG后，由于No-op log占用了index位置，所以B测试无法通过，实际上功能正常。

```go
// raft/raft.go:41
const (
	PREVOTE          = true
	LEADER_STICKNESS = false
	NOOP_LOG         = false
)
```

# DEBUG

默认开启了DEBUG选项，会打出很多debug信息，若不需要可以去`raft/util.go:9`关闭`DEBUG`选项

```go
// raft/util.go:41
const Debug = true
```
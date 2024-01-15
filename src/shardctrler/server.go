package shardctrler

import (
	"log"
	"sort"
	"sync"
	"time"

	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
)

const Debug = true
const DebugDeadLock = true

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

const (
	RPCTimeOut = 150
)

type ShardCtrler struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	// Your data here.
	notifyChs  map[int]chan CmdIdentify
	lastCmdIds map[int64]uint64

	configs []Config // indexed by config num
}

type CmdIdentify struct {
	ClientId int64
	CmdId    uint64
}

func (sc *ShardCtrler) isRepeateCmd(ClientId int64, CmdId uint64) bool {
	lastCmdId, ok := sc.lastCmdIds[ClientId]
	if !ok || CmdId > lastCmdId {
		return false
	} else {
		return true
	}
}

type Op struct {
	// Your data here.
	CmdId    uint64
	ClientId int64
	Op       string
	Servers  map[int][]string
	GIDs     []int
	Shard    int
	GID      int
	Num      int
}

const (
	op_join  = "Join"
	op_leave = "leave"
	op_move  = "Move"
	op_query = "Query"
)

func (sc *ShardCtrler) Join(args *JoinArgs, reply *JoinReply) {
	// Your code here.
	newOp := Op{Op: op_join, ClientId: args.ClientId, CmdId: args.CmdId, Servers: args.Servers}
	err := sc.startOp(&newOp)
	reply.Err = err
	reply.WrongLeader = (err == ErrWrongLeader)
}

func (sc *ShardCtrler) Leave(args *LeaveArgs, reply *LeaveReply) {
	// Your code here.
	newOp := Op{Op: op_leave, ClientId: args.ClientId, CmdId: args.CmdId, GIDs: args.GIDs}
	err := sc.startOp(&newOp)
	reply.Err = err
	reply.WrongLeader = (err == ErrWrongLeader)
}

func (sc *ShardCtrler) Move(args *MoveArgs, reply *MoveReply) {
	// Your code here.
	newOp := Op{Op: op_move, ClientId: args.ClientId, CmdId: args.CmdId, Shard: args.Shard, GID: args.GID}
	err := sc.startOp(&newOp)
	reply.Err = err
	reply.WrongLeader = (err == ErrWrongLeader)
}

func (sc *ShardCtrler) Query(args *QueryArgs, reply *QueryReply) {
	// Your code here.
	newOp := Op{Op: op_query, ClientId: args.ClientId, CmdId: args.CmdId, Num: args.Num}
	err := sc.startOp(&newOp)
	reply.Err = err
	reply.WrongLeader = (err == ErrWrongLeader)
	if err == OK {
		sc.mu.Lock()
		if args.Num < 0 || args.Num >= len(sc.configs) {
			reply.Config = sc.configs[len(sc.configs)-1]
		} else {
			reply.Config = sc.configs[args.Num]
		}
		sc.mu.Unlock()
	}
}

func (sc *ShardCtrler) startOp(op *Op) Err {
	var err Err
	index, _, isLeader := sc.rf.Start(*op)
	if !isLeader {
		err = ErrWrongLeader
	} else {
		sc.mu.Lock()
		DPrintf("sc server %d start op %v", sc.me, *op)
		if _, ok := sc.notifyChs[index]; !ok {
			sc.notifyChs[index] = make(chan CmdIdentify, 1)
		}
		ch := sc.notifyChs[index]
		sc.mu.Unlock()
		select {
		case cmdIdentify := <-ch:
			if cmdIdentify.ClientId == op.ClientId && cmdIdentify.CmdId == op.CmdId {
				err = OK
			} else {
				err = ErrWrongLeader
			}
		case <-time.After(time.Duration(RPCTimeOut) * time.Millisecond):
			sc.mu.Lock()
			if sc.isRepeateCmd(op.ClientId, op.CmdId) {
				sc.mu.Unlock()
				err = OK
			} else {
				sc.mu.Unlock()
				err = ErrTimeOut
			}
		}
		sc.mu.Lock()
		delete(sc.notifyChs, index)
		sc.mu.Unlock()
	}
	return err
}

func (sc *ShardCtrler) applier() {
	for {
		newApplyMsg := <-sc.applyCh
		if newApplyMsg.CommandValid {
			op := newApplyMsg.Command.(Op)
			sc.mu.Lock()
			DPrintf("sc server %d apply op %v", sc.me, op)
			if sc.isRepeateCmd(op.ClientId, op.CmdId) {
				sc.mu.Unlock()
				continue
			}
			switch op.Op {
			case op_join:
				sc.join(&op)
			case op_leave:
				sc.leave(&op)
			case op_move:
				sc.move(&op)
			case op_query:

			}
			sc.lastCmdIds[op.ClientId] = op.CmdId
			index := newApplyMsg.CommandIndex
			if ch, ok := sc.notifyChs[index]; ok {
				ch <- CmdIdentify{ClientId: op.ClientId, CmdId: op.CmdId}
			}
			sc.mu.Unlock()
		}
	}
}

func reBalance(newConf *Config, oldShards *[NShards]int) {
	newGroups := make([]int, 0)
	groupShardsMap := make(map[int][]int)
	for gid := range newConf.Groups {
		newGroups = append(newGroups, gid)
		groupShardsMap[gid] = make([]int, 0)
	}
	freeShards := make([]int, 0)
	for i, gid := range newConf.Shards {
		if _, ok := groupShardsMap[gid]; ok {
			groupShardsMap[gid] = append(groupShardsMap[gid], i)
		} else {
			freeShards = append(freeShards, i)
		}
	}
	sort.Ints(newGroups)
	sort.Slice(newGroups, func(i, j int) bool { return len(groupShardsMap[i]) > len(groupShardsMap[j]) })

	if len(newGroups) == 0 {
		for i := 0; i < NShards; i++ {
			newConf.Shards[i] = 0
		}
		return
	}

	perGroupShardNum := NShards / len(newGroups)
	bigGroupNum := NShards - perGroupShardNum*len(newGroups)
	for _, gid := range newGroups {
		var shardNum int
		if bigGroupNum > 0 {
			shardNum = perGroupShardNum + 1
			bigGroupNum--
		} else {
			shardNum = perGroupShardNum
		}
		for len(groupShardsMap[gid]) > shardNum {
			freeShards = append(freeShards, groupShardsMap[gid][len(groupShardsMap[gid])-1])
			groupShardsMap[gid] = groupShardsMap[gid][:len(groupShardsMap[gid])-1]
		}
		for len(groupShardsMap[gid]) < shardNum {
			freeShard := freeShards[len(freeShards)-1]
			freeShards = freeShards[:len(freeShards)-1]
			groupShardsMap[gid] = append(groupShardsMap[gid], freeShard)
		}
	}
	for gid, shards := range groupShardsMap {
		for _, shard := range shards {
			newConf.Shards[shard] = gid
		}
	}
}

func (sc *ShardCtrler) join(op *Op) {
	newConf := Config{}
	newConf.Num = len(sc.configs)
	newConf.Groups = make(map[int][]string)

	lastConf := sc.configs[len(sc.configs)-1]

	for gid, servers := range lastConf.Groups {
		newConf.Groups[gid] = make([]string, len(servers))
		copy(newConf.Groups[gid], servers)
	}

	for gid, servers := range op.Servers {
		newConf.Groups[gid] = make([]string, len(servers))
		copy(newConf.Groups[gid], servers)
	}
	reBalance(&newConf, &lastConf.Shards)
	sc.configs = append(sc.configs, newConf)
	if Debug {
		DPrintf("sc server %d join new group and make a new config %v", sc.me, newConf)
	}

}

func (sc *ShardCtrler) leave(op *Op) {
	newConf := Config{}
	newConf.Num = len(sc.configs)
	newConf.Groups = make(map[int][]string)

	lastConf := sc.configs[len(sc.configs)-1]

	for gid, servers := range lastConf.Groups {
		newConf.Groups[gid] = make([]string, len(servers))
		copy(newConf.Groups[gid], servers)
	}

	for _, gid := range op.GIDs {
		delete(newConf.Groups, gid)
	}
	reBalance(&newConf, &lastConf.Shards)
	sc.configs = append(sc.configs, newConf)
	if Debug {
		DPrintf("sc server %d leave group and make a new config %v", sc.me, newConf)
	}
}

func (sc *ShardCtrler) move(op *Op) {
	newConf := Config{}
	newConf.Num = len(sc.configs)
	newConf.Groups = make(map[int][]string)

	lastConf := sc.configs[len(sc.configs)-1]

	for gid, servers := range lastConf.Groups {
		newConf.Groups[gid] = make([]string, len(servers))
		copy(newConf.Groups[gid], servers)
	}
	newConf.Shards = lastConf.Shards
	newConf.Shards[op.Shard] = op.GID
	sc.configs = append(sc.configs, newConf)
}

//
// the tester calls Kill() when a ShardCtrler instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (sc *ShardCtrler) Kill() {
	sc.rf.Kill()
	// Your code here, if desired.
}

// needed by shardkv tester
func (sc *ShardCtrler) Raft() *raft.Raft {
	return sc.rf
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant shardctrler service.
// me is the index of the current server in servers[].
//
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardCtrler {
	sc := new(ShardCtrler)
	sc.me = me

	sc.configs = make([]Config, 1)
	sc.configs[0].Groups = map[int][]string{}
	sc.configs[0].Num = 0
	for i := 0; i < NShards; i++ {
		sc.configs[0].Shards[i] = 0
	}
	labgob.Register(Op{})
	sc.applyCh = make(chan raft.ApplyMsg)
	sc.rf = raft.Make(servers, me, persister, sc.applyCh)

	sc.notifyChs = make(map[int]chan CmdIdentify)
	sc.lastCmdIds = make(map[int64]uint64)

	// Your code here.
	go sc.applier()

	return sc
}

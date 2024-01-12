package shardkv

import (
	"bytes"
	"fmt"
	"log"
	"sync"
	"time"

	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
	"6.824/shardctrler"
)

const Debug = true

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

const (
	RPCTimeOut = 500
)

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	CmdId      uint64
	ClientId   int64
	Key        string
	Value      string
	Op         string
	ConfigNum  int
	Shard      int
	KvMap      map[string]string
	LastCmdIds map[int64]uint64
	ShardKVId  int
	Config     shardctrler.Config
}

const (
	op_put    = "Put"
	op_get    = "Get"
	op_append = "Append"
	op_update = "Update" // update the config
	op_push   = "Push"   // push the old shard
	op_done   = "Done"   // the update of config finish
)

type CmdIdentify struct {
	ClientId  int64
	CmdId     uint64
	ShardKVId int
	ConfigNum int
	Shard     int
	Err       Err
	Result    string
}

type ShardKV struct {
	mu           sync.Mutex
	me           int
	rf           *raft.Raft
	applyCh      chan raft.ApplyMsg
	make_end     func(string) *labrpc.ClientEnd
	gid          int
	ctrlers      []*labrpc.ClientEnd
	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	persister     *raft.Persister
	sm            *shardctrler.Clerk
	config        shardctrler.Config
	kvMap         map[string]string
	notifyChs     map[int]chan CmdIdentify
	lastCmdIds    map[int64]uint64
	newConfig     shardctrler.Config
	updating      bool
	pullShardsMap [shardctrler.NShards]int // shard -> configNum, use to refuse repeate push shards

	pushFinish    int
	commitNewConf int
	commitDone    int
}

func (kv *ShardKV) isRepeateCmd(ClientId int64, CmdId uint64) bool {
	lastCmdId, ok := kv.lastCmdIds[ClientId]
	if !ok || CmdId > lastCmdId {
		return false
	} else {
		return true
	}
}

func (kv *ShardKV) startOp(op *Op) (Err, string) {
	var err Err
	var result string
	index, _, isLeader := kv.rf.Start(*op)
	if !isLeader {
		err = ErrWrongLeader
	} else {
		kv.mu.Lock()
		DPrintf("kv server %d of gid %d success start the op %v", kv.me, kv.gid, *op)
		if _, ok := kv.notifyChs[index]; !ok {
			kv.notifyChs[index] = make(chan CmdIdentify, 1)
		}
		ch := kv.notifyChs[index]
		kv.mu.Unlock()
		select {
		case cmdIdentify := <-ch:
			if cmdIdentify.ClientId == op.ClientId && cmdIdentify.CmdId == op.CmdId {
				err = cmdIdentify.Err
				if op.Op == op_get && err == OK {
					result = cmdIdentify.Result
				}
			} else {
				err = ErrWrongLeader
			}
		case <-time.After(time.Duration(RPCTimeOut) * time.Millisecond):
			kv.mu.Lock()
			if kv.isRepeateCmd(op.ClientId, op.CmdId) && op.Op != op_get {
				kv.mu.Unlock()
				err = OK
			} else {
				kv.mu.Unlock()
				err = ErrTimeOut
			}
		}
		kv.mu.Lock()
		delete(kv.notifyChs, index)
		kv.mu.Unlock()
	}
	return err, result
}

func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	shard := key2shard(args.Key)
	kv.mu.Lock()
	if kv.config.Shards[shard] != kv.gid {
		reply.Err = ErrWrongGroup
		kv.mu.Unlock()
		return
	}
	kv.mu.Unlock()

	newOp := Op{Key: args.Key, Op: op_get, CmdId: args.CmdId, ClientId: args.ClientId}
	err, result := kv.startOp(&newOp)
	if err == OK {
		reply.Value = result
	}
	reply.Err = err

}

func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	shard := key2shard(args.Key)
	kv.mu.Lock()
	if kv.config.Shards[shard] != kv.gid {
		reply.Err = ErrWrongGroup
		kv.mu.Unlock()
		return
	}
	kv.mu.Unlock()

	newOp := Op{Key: args.Key, Value: args.Value, Op: args.Op, CmdId: args.CmdId, ClientId: args.ClientId}
	err, _ := kv.startOp(&newOp)
	reply.Err = err
}

func (kv *ShardKV) applier() {
	for {
		newApplyMsg := <-kv.applyCh
		if newApplyMsg.CommandValid {
			op := newApplyMsg.Command.(Op)
			kv.mu.Lock()
			if kv.isRepeateCmd(op.ClientId, op.CmdId) && op.Op != op_get {
				kv.mu.Unlock()
				continue
			}
			index := newApplyMsg.CommandIndex
			if op.Op == op_push {
				DPrintf("kv server %d of gid %d apply push shard %d at config %d", kv.me, kv.gid, op.Shard, op.ConfigNum)
				if op.ConfigNum == kv.config.Num+1 && kv.updating && op.ConfigNum > kv.pullShardsMap[op.Shard] {
					DPrintf("kv server %d of gid %d begin to merge shard %d at config %d", kv.me, kv.gid, op.Shard, op.ConfigNum)
					for key, value := range op.KvMap {
						kv.kvMap[key] = value
						DPrintf("kv server %d of gid %d state: key %v, value %v because push shards\n", kv.me, kv.gid, key, value)
					}
					for client, lastCmdId := range op.LastCmdIds {
						if cmdId, ok := kv.lastCmdIds[client]; ok {
							if lastCmdId > cmdId {
								kv.lastCmdIds[client] = lastCmdId
							}
						} else {
							kv.lastCmdIds[client] = lastCmdId
						}
					}
					kv.config.Shards[op.Shard] = kv.gid
					kv.pullShardsMap[op.Shard] = op.ConfigNum
					if ch, ok := kv.notifyChs[index]; ok {
						ch <- CmdIdentify{ConfigNum: op.ConfigNum, Shard: op.Shard, ShardKVId: op.ShardKVId, Err: OK}
					}
				} else if op.ConfigNum < kv.config.Num+1 || op.ConfigNum <= kv.pullShardsMap[op.Shard] {
					if ch, ok := kv.notifyChs[index]; ok {
						ch <- CmdIdentify{ConfigNum: op.ConfigNum, Shard: op.Shard, ShardKVId: op.ShardKVId, Err: OK}
					}
				} else if op.ConfigNum > kv.config.Num+1 || !kv.updating {
					if op.ConfigNum > kv.config.Num+1 {
						DPrintf("ErrWrongConfigNum occur, kv server %d, gid %d, op.ConfigNum: %d, kv.configNum: %d", kv.me, kv.gid, op.ConfigNum, kv.config.Num)
					} else {
						DPrintf("ErrWrongConfigNum occur, kv server %d, gid %d, not updating", kv.me, kv.gid)
					}
					if ch, ok := kv.notifyChs[index]; ok {
						ch <- CmdIdentify{ConfigNum: op.ConfigNum, Shard: op.Shard, ShardKVId: op.ShardKVId, Err: ErrWrongConfigNum}
					}
				}
			} else if op.Op == op_update {
				DPrintf("kv server %d of gid %d apply update op with Op %v", kv.me, kv.gid, op)
				if !kv.updating && op.Config.Num == kv.config.Num+1 {
					kv.updateConf(&op.Config)
				} else {
					DPrintf("kv server %d of gid %d apply update op err with Op %v", kv.me, kv.gid, op)
				}
			} else if op.Op == op_done {
				DPrintf("kv server %d of gid %d apply the done op at config %d", kv.me, kv.gid, op.ConfigNum)
				if op.ConfigNum == kv.config.Num+1 {
					kv.config = kv.newConfig
					DPrintf("kv server %d of gid %d set config to %v", kv.me, kv.gid, kv.config)
					kv.updating = false
					DPrintf("kv server %d of gid %d set updating to false", kv.me, kv.gid)
					deleteKeys := make([]string, 0)
					for key := range kv.kvMap {
						shard := key2shard(key)
						if kv.config.Shards[shard] != kv.gid {
							deleteKeys = append(deleteKeys, key)
						}
					}
					for _, key := range deleteKeys {
						delete(kv.kvMap, key)
					}
					DPrintf("kv server %d of gid %d success update with new configNum %d and new shards %v", kv.me, kv.gid, kv.config.Num, kv.config.Shards)
				}
			} else {
				shard := key2shard(op.Key)
				if kv.config.Shards[shard] == kv.gid {
					if op.Op == op_put {
						kv.kvMap[op.Key] = op.Value
						kv.lastCmdIds[op.ClientId] = op.CmdId
						DPrintf("kv server %d of gid %d apply op %v\n", kv.me, kv.gid, op)
						DPrintf("kv server %d of gid %d state: key %v, value %v\n", kv.me, kv.gid, op.Key, kv.kvMap[op.Key])
						if ch, ok := kv.notifyChs[index]; ok {
							ch <- CmdIdentify{ClientId: op.ClientId, CmdId: op.CmdId, Err: OK}
						}
					} else if op.Op == op_append {
						_, ok := kv.kvMap[op.Key]
						if ok {
							kv.kvMap[op.Key] += op.Value
						} else {
							kv.kvMap[op.Key] = op.Value
						}
						kv.lastCmdIds[op.ClientId] = op.CmdId
						DPrintf("kv server %d of gid %d apply op %v\n", kv.me, kv.gid, op)
						DPrintf("kv server %d of gid %d state: key %v, value %v\n", kv.me, kv.gid, op.Key, kv.kvMap[op.Key])
						if ch, ok := kv.notifyChs[index]; ok {
							ch <- CmdIdentify{ClientId: op.ClientId, CmdId: op.CmdId, Err: OK}
						}
					} else if op.Op == op_get {
						kv.lastCmdIds[op.ClientId] = op.CmdId
						DPrintf("kv server %d of gid %d apply get op %v\n", kv.me, kv.gid, op)
						DPrintf("kv server %d of gid %d state: key %v, value %v\n", kv.me, kv.gid, op.Key, kv.kvMap[op.Key])
						if ch, ok := kv.notifyChs[index]; ok {
							ch <- CmdIdentify{ClientId: op.ClientId, CmdId: op.CmdId, Err: OK, Result: kv.kvMap[op.Key]}
						}
					}
				} else {
					if ch, ok := kv.notifyChs[index]; ok {
						ch <- CmdIdentify{ClientId: op.ClientId, CmdId: op.CmdId, Err: ErrWrongGroup}
					}
				}
			}
			kv.makeSnapshot(index)
			kv.mu.Unlock()
		} else if newApplyMsg.SnapshotValid {
			kv.mu.Lock()
			kv.applySnapshot(newApplyMsg.Snapshot, newApplyMsg.SnapshotIndex)
			kv.mu.Unlock()
		}
	}
}

func (kv *ShardKV) updateConf(newConfig *shardctrler.Config) {
	DPrintf("kv server %d of gid %d apply update op success with config %v", kv.me, kv.gid, newConfig)
	kv.newConfig = *newConfig
	kv.updating = true
	DPrintf("kv server %d of gid %d set updating to true", kv.me, kv.gid)
	for i := 0; i < shardctrler.NShards; i++ {
		if newConfig.Shards[i] != kv.gid && kv.config.Shards[i] == kv.gid {
			// the shard is moved to other group
			kv.config.Shards[i] = -1 // means this shard need to push to other group
		} else if kv.config.Shards[i] == 0 && newConfig.Shards[i] == kv.gid {
			kv.config.Shards[i] = kv.gid
		} else if kv.config.Shards[i] != kv.gid && newConfig.Shards[i] != kv.gid {
			kv.config.Shards[i] = newConfig.Shards[i]
		}
	}
}

func (kv *ShardKV) getNextConf() {
	for {
		// DPrintf("kv server %d of gid %d begin getNextConf loop", kv.me, kv.gid)
		_, isLeader := kv.rf.GetState()
		kv.mu.Lock()
		if isLeader {
			if kv.commitNewConf <= kv.config.Num {
				newConfigNum := kv.config.Num + 1
				kv.mu.Unlock()
				DPrintf("kv server %d of gid %d try to query config %d", kv.me, kv.gid, newConfigNum)
				config := kv.sm.Query(newConfigNum)
				DPrintf("kv server %d of gid %d query config %v at time %v", kv.me, kv.gid, config, time.Now())
				kv.mu.Lock()
				if config.Num == kv.config.Num+1 && config.Num > kv.commitNewConf {
					DPrintf("kv server %d of gid %d get a new config %v", kv.me, kv.gid, config)
					newOp := Op{
						Config: config,
						Op:     op_update,
					}
					index, _, isLeader := kv.rf.Start(newOp)
					if isLeader {
						kv.commitNewConf = config.Num
						DPrintf("kv server %d of gid %d success start the update op with %v at index %d, and set commitNewConf to %d", kv.me, kv.gid, newOp, index, kv.newConfig.Num)
					}
				}
			} else {
				DPrintf("kv server %d of gid %d is leader but kv.commitNewConf %d > kv.config.Num %d", kv.me, kv.gid, kv.commitNewConf, kv.config.Num)
			}
		}
		kv.mu.Unlock()
		time.Sleep(100 * time.Millisecond)
	}
}

// the leader check is the group updating config, if updating, push all the shard which is not belong to the group
func (kv *ShardKV) pushAllShards() {
	for {
		_, isLeader := kv.rf.GetState()
		if isLeader {
			var wg sync.WaitGroup
			kv.mu.Lock()
			newConfigNum := kv.newConfig.Num
			if kv.updating && kv.pushFinish < kv.newConfig.Num {
				DPrintf("kv server %d of gid %d begin push all shards at config %d", kv.me, kv.gid, kv.newConfig.Num)
				resultChs := make([]chan bool, 0)
				for i := 0; i < shardctrler.NShards; i++ {
					if kv.config.Shards[i] == -1 && kv.newConfig.Shards[i] != 0 {
						resultChs = append(resultChs, make(chan bool, 1))
						wg.Add(1)
						args := TransferShardArgs{}
						args.ConfigNum = kv.newConfig.Num
						args.Shard = i
						args.ShardKVId = kv.me
						args.LastCmdIds = make(map[int64]uint64)
						for client, lastCmdId := range kv.lastCmdIds {
							args.LastCmdIds[client] = lastCmdId
						}
						args.KvMap = make(map[string]string)
						for key, value := range kv.kvMap {
							shard := key2shard(key)
							if shard == i {
								args.KvMap[key] = value
							}
						}
						go kv.callPushShard(&args, kv.newConfig.Shards[i], &wg, resultChs[len(resultChs)-1])
					}
				}
				kv.mu.Unlock()
				wg.Wait()
				success := true
				for _, ch := range resultChs {
					temp := <-ch
					if !temp {
						success = false
					}
				}
				kv.mu.Lock()
				if success {
					DPrintf("kv server %d of gid %d success push all shards in config %d", kv.me, kv.gid, newConfigNum)
					kv.pushFinish = newConfigNum
					DPrintf("kv server %d of gid %d set pushFinish to %d", kv.me, kv.gid, newConfigNum)
				}
			}
			kv.mu.Unlock()
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (kv *ShardKV) callPushShard(args *TransferShardArgs, gid int, wg *sync.WaitGroup, ch chan bool) {
	defer wg.Done()
	kv.mu.Lock()
	DPrintf("kv server %d of gid %d begin push shard %d to gid %d at config %d", kv.me, kv.gid, args.Shard, gid, args.ConfigNum)
	servers := kv.newConfig.Groups[gid]
	if _, ok := kv.newConfig.Groups[gid]; !ok {
		// the newconfig is too new, so the shard has been send by earlier leader, don't need to send again,
		DPrintf("kv server %d of gid %d push shard %d at config %d is missed in current newConfig", kv.me, kv.gid, args.Shard, args.ConfigNum)
		kv.mu.Unlock()
		ch <- false
		return
	}
	for si := 0; ; si = (si + 1) % len(servers) {
		srv := kv.make_end(servers[si])
		var reply TransferShardReply
		kv.mu.Unlock()
		ok := srv.Call("ShardKV.PushShard", args, &reply)
		kv.mu.Lock()
		_, isLeader := kv.rf.GetState()
		if !isLeader || !kv.updating || kv.pushFinish >= kv.newConfig.Num {
			kv.mu.Unlock()
			ch <- false
			return
		}
		if ok && reply.Err == OK {
			DPrintf("kv server %d of gid %d success push shard %d to gid %d in config %d", kv.me, kv.gid, args.Shard, gid, args.ConfigNum)
			kv.mu.Unlock()
			ch <- true
			return
		}
		if ok && reply.Err == ErrWrongConfigNum {
			// the target group's config is slow than kv.gid group
			DPrintf("kv server %d of gid %d push shard %d to gid %d in config %d ErrWrongConfigNum", kv.me, kv.gid, args.Shard, gid, args.ConfigNum)
			si = (si + len(servers) - 1) % len(servers)
			time.Sleep(10 * time.Millisecond)
			continue
		}
		if !ok || reply.Err == ErrWrongLeader || reply.Err == ErrTimeOut {
			if !ok {
				DPrintf("kv server %d of gid %d push shard %d to gid %d in config %d failed", kv.me, kv.gid, args.Shard, gid, args.ConfigNum)
			} else if reply.Err == ErrWrongLeader {
				DPrintf("kv server %d of gid %d push shard %d to gid %d in config %d ErrWrongLeader", kv.me, kv.gid, args.Shard, gid, args.ConfigNum)
			} else if reply.Err == ErrTimeOut {
				DPrintf("kv server %d of gid %d push shard %d to gid %d in config %d ErrTimeOut", kv.me, kv.gid, args.Shard, gid, args.ConfigNum)
			}
			continue
		}
	}
}

func (kv *ShardKV) PushShard(args *TransferShardArgs, reply *TransferShardReply) {
	// Your code here.
	op := Op{
		ConfigNum:  args.ConfigNum,
		Shard:      args.Shard,
		KvMap:      args.KvMap,
		LastCmdIds: args.LastCmdIds,
		ShardKVId:  args.ShardKVId,
		Op:         op_push,
	}
	var err Err
	kv.mu.Lock()
	index, _, isLeader := kv.rf.Start(op)
	DPrintf("kv server %d of gid %d start the push shard of shard %d at config %d", kv.me, kv.gid, op.Shard, op.ConfigNum)
	if !isLeader {
		kv.mu.Unlock()
		err = ErrWrongLeader
	} else {
		if _, ok := kv.notifyChs[index]; !ok {
			kv.notifyChs[index] = make(chan CmdIdentify, 1)
		}
		ch := kv.notifyChs[index]
		kv.mu.Unlock()
		select {
		case cmdIdentify := <-ch:
			if cmdIdentify.ShardKVId == op.ShardKVId && cmdIdentify.ConfigNum == op.ConfigNum && cmdIdentify.Shard == op.Shard {
				err = cmdIdentify.Err
			} else {
				err = ErrWrongLeader
			}
		case <-time.After(500 * time.Millisecond):
			err = ErrTimeOut
		}
	}
	reply.Err = err
	kv.mu.Lock()
	delete(kv.notifyChs, index)
	kv.mu.Unlock()
}

func (kv *ShardKV) checkUpdateFinish() {
	for {
		_, isLeader := kv.rf.GetState()
		kv.mu.Lock()
		if isLeader && kv.commitDone < kv.newConfig.Num && kv.updating {
			if kv.pushFinish == kv.newConfig.Num {
				updateFinish := true
				for i := 0; i < shardctrler.NShards; i++ {
					if kv.newConfig.Shards[i] == kv.gid && kv.config.Shards[i] != kv.gid {
						updateFinish = false
						break
					}
				}
				if updateFinish {
					op := Op{
						Op:        op_done,
						ConfigNum: kv.newConfig.Num,
					}
					_, _, isLeader := kv.rf.Start(op)
					if isLeader {
						DPrintf("kv server %d of gid %d start the done op at config %d", kv.me, kv.gid, kv.newConfig.Num)
						kv.commitDone = kv.newConfig.Num
					}
				}
			}

		}
		kv.mu.Unlock()
		time.Sleep(100 * time.Millisecond)
	}
}

func (kv *ShardKV) makeSnapshot(index int) {
	if kv.maxraftstate != -1 && kv.maxraftstate < kv.persister.RaftStateSize() {
		w := new(bytes.Buffer)
		e := labgob.NewEncoder(w)
		e.Encode(kv.config)
		e.Encode(kv.kvMap)
		e.Encode(kv.lastCmdIds)
		e.Encode(kv.newConfig)
		e.Encode(kv.updating)
		e.Encode(kv.pullShardsMap)
		snapshot := w.Bytes()
		DPrintf("kv server %d of gid %d ready the snapshot at index %d\n", kv.me, kv.gid, index)
		kv.rf.Snapshot(index, snapshot)
		DPrintf("kv server %d of gid %d finish the snapshot at index %d\n", kv.me, kv.gid, index)
	}
}

func (kv *ShardKV) recorverFromSnapshot() {
	DPrintf("kv server %d of gid %d reboot and recorver from snapshot", kv.me, kv.gid)
	snapshot := kv.persister.ReadSnapshot()
	kv.applySnapshot(snapshot, -1)
}

func (kv *ShardKV) applySnapshot(snapshot []byte, index int) {
	if snapshot == nil || len(snapshot) < 1 {
		return
	}
	r := bytes.NewBuffer(snapshot)
	d := labgob.NewDecoder(r)
	var config shardctrler.Config
	var kvMap map[string]string
	var lastCmdId map[int64]uint64
	var newConfig shardctrler.Config
	var updating bool
	var pullShardsMap [shardctrler.NShards]int
	if d.Decode(&config) != nil ||
		d.Decode(&kvMap) != nil ||
		d.Decode(&lastCmdId) != nil ||
		d.Decode(&newConfig) != nil ||
		d.Decode(&updating) != nil ||
		d.Decode(&pullShardsMap) != nil {
		fmt.Printf("read Snapshot decode error\n")
	} else {
		kv.config = config
		kv.kvMap = kvMap
		kv.lastCmdIds = lastCmdId
		kv.newConfig = newConfig
		kv.updating = updating
		kv.pullShardsMap = pullShardsMap
		DPrintf("kv server %d of gid %d apply the snapshot at index %d recorver kvmap %v\n", kv.me, kv.gid, index, kv.kvMap)
	}
}

//
// the tester calls Kill() when a ShardKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (kv *ShardKV) Kill() {
	kv.rf.Kill()
	// Your code here, if desired.
}

//
// servers[] contains the ports of the servers in this group.
//
// me is the index of the current server in servers[].
//
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
//
// the k/v server should snapshot when Raft's saved state exceeds
// maxraftstate bytes, in order to allow Raft to garbage-collect its
// log. if maxraftstate is -1, you don't need to snapshot.
//
// gid is this group's GID, for interacting with the shardctrler.
//
// pass ctrlers[] to shardctrler.MakeClerk() so you can send
// RPCs to the shardctrler.
//
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs. You'll need this to send RPCs to other groups.
//
// look at client.go for examples of how to use ctrlers[]
// and make_end() to send RPCs to the group owning a specific shard.
//
// StartServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int, gid int, ctrlers []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *ShardKV {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(ShardKV)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.make_end = make_end
	kv.gid = gid
	kv.ctrlers = ctrlers

	// Your initialization code here.

	// Use something like this to talk to the shardctrler:
	// kv.mck = shardctrler.MakeClerk(kv.ctrlers)

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	kv.persister = persister

	kv.kvMap = make(map[string]string)
	kv.notifyChs = make(map[int]chan CmdIdentify)
	kv.lastCmdIds = make(map[int64]uint64)

	kv.sm = shardctrler.MakeClerk(ctrlers)
	kv.updating = false
	kv.pushFinish = 0
	kv.commitDone = 0
	kv.commitNewConf = 0

	kv.recorverFromSnapshot()

	go kv.applier()
	go kv.getNextConf()
	go kv.pushAllShards()
	go kv.checkUpdateFinish()

	return kv
}

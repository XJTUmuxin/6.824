package kvraft

import (
	"bytes"
	"fmt"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
)

const Debug = false
const DebugDeadLock = true

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
	CmdId    uint64
	ClientId int64
	Key      string
	Value    string
	Op       string
}

const (
	op_put    = "Put"
	op_get    = "Get"
	op_append = "Append"
)

type CmdIdentify struct {
	ClientId int64
	CmdId    uint64
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	kvMap      map[string]string
	notifyChs  map[int]chan CmdIdentify
	lastCmdIds map[int64]uint64

	persister *raft.Persister
}

func (kv *KVServer) isRepeateCmd(ClientId int64, CmdId uint64) bool {
	lastCmdId, ok := kv.lastCmdIds[ClientId]
	if !ok || CmdId > lastCmdId {
		return false
	} else {
		return true
	}
}

func (kv *KVServer) startOp(op *Op) Err {
	var err Err
	kv.mu.Lock()
	index, _, isLeader := kv.rf.Start(*op)
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
			if cmdIdentify.ClientId == op.ClientId && cmdIdentify.CmdId == op.CmdId {
				err = OK
			} else {
				err = ErrWrongLeader
			}
		case <-time.After(time.Duration(RPCTimeOut) * time.Millisecond):
			kv.mu.Lock()
			if kv.isRepeateCmd(op.ClientId, op.CmdId) {
				kv.mu.Unlock()
				err = OK
			} else {
				kv.mu.Unlock()
				err = ErrTimeOut
			}
		}
	}
	kv.mu.Lock()
	delete(kv.notifyChs, index)
	kv.mu.Unlock()
	return err
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.

	newOp := Op{Key: args.Key, Op: op_get, CmdId: args.CmdId, ClientId: args.ClientId}
	err := kv.startOp(&newOp)
	if err == OK {
		kv.mu.Lock()
		reply.Value = kv.kvMap[args.Key]
		kv.mu.Unlock()
	}
	reply.Err = err
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	newOp := Op{Key: args.Key, Value: args.Value, Op: args.Op, CmdId: args.CmdId, ClientId: args.ClientId}
	err := kv.startOp(&newOp)
	reply.Err = err
}

func (kv *KVServer) applier() {
	for !kv.killed() {
		newApplyMsg := <-kv.applyCh
		if newApplyMsg.CommandValid {
			op := newApplyMsg.Command.(Op)
			kv.mu.Lock()
			if kv.isRepeateCmd(op.ClientId, op.CmdId) {
				kv.mu.Unlock()
				continue
			}
			if op.Op == op_put {
				kv.kvMap[op.Key] = op.Value
			} else if op.Op == op_append {
				_, ok := kv.kvMap[op.Key]
				if ok {
					kv.kvMap[op.Key] += op.Value
				} else {
					kv.kvMap[op.Key] = op.Value
				}
			}
			kv.lastCmdIds[op.ClientId] = op.CmdId
			DPrintf("kv server %d apply op %v\n", kv.me, op)
			DPrintf("kv server %d state: key %v, value %v\n", kv.me, op.Key, kv.kvMap[op.Key])
			index := newApplyMsg.CommandIndex
			if ch, ok := kv.notifyChs[index]; ok {
				ch <- CmdIdentify{ClientId: op.ClientId, CmdId: op.CmdId}
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

func (kv *KVServer) makeSnapshot(index int) {
	if kv.maxraftstate != -1 && kv.maxraftstate < kv.persister.RaftStateSize() {
		w := new(bytes.Buffer)
		e := labgob.NewEncoder(w)
		e.Encode(kv.kvMap)
		e.Encode(kv.lastCmdIds)
		snapshot := w.Bytes()
		DPrintf("kv server %d ready the snapshot at index %d\n", kv.me, index)
		kv.rf.Snapshot(index, snapshot)
		DPrintf("kv server %d finish the snapshot at index %d\n", kv.me, index)
	}
}

func (kv *KVServer) recorverFromSnapshot() {
	snapshot := kv.persister.ReadSnapshot()
	kv.applySnapshot(snapshot, -1)
}

func (kv *KVServer) applySnapshot(snapshot []byte, index int) {
	if snapshot == nil || len(snapshot) < 1 {
		return
	}
	r := bytes.NewBuffer(snapshot)
	d := labgob.NewDecoder(r)
	var kvMap map[string]string
	var lastCmdId map[int64]uint64
	if d.Decode(&kvMap) != nil || d.Decode(&lastCmdId) != nil {
		fmt.Printf("read Snapshot decode error\n")
	} else {
		kv.kvMap = kvMap
		kv.lastCmdIds = lastCmdId
		DPrintf("kv server %d apply the snapshot at index %d recorver kvmap %v\n", kv.me, index, kv.kvMap)
	}
}

//
// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
//
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	kv.persister = persister

	kv.kvMap = make(map[string]string)
	kv.notifyChs = make(map[int]chan CmdIdentify)
	kv.lastCmdIds = make(map[int64]uint64)

	// recorver from the snapshot
	kv.recorverFromSnapshot()
	// You may need initialization code here.
	go kv.applier()
	go func() {
		for {
			kv.mu.Lock()
			DPrintf("kv server %d still not deadlock\n", kv.me)
			kv.mu.Unlock()
			time.Sleep(50000 * time.Millisecond)
		}
	}()
	return kv
}

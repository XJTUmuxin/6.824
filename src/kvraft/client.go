package kvraft

import (
	"crypto/rand"
	"math/big"

	"6.824/labrpc"
)

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	clientId     int64
	nextCmdId    uint64
	lastLeaderId int
	lastTerm     int
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	// You'll have to add code here.
	ck.nextCmdId = 1
	ck.clientId = nrand()
	ck.lastLeaderId = 0
	ck.lastTerm = 0

	return ck
}

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) Get(key string) string {
	// You will have to modify this function.
	cmdId := ck.nextCmdId
	ck.nextCmdId++
	args := GetArgs{Key: key, CmdId: cmdId, ClientId: ck.clientId}
	for i := ck.lastLeaderId; ; i = (i + 1) % len(ck.servers) {
		// if i == len(ck.servers)-1 {
		// 	time.Sleep(50 * time.Millisecond)
		// }
		reply := GetReply{}
		DPrintf("client %v call get to server %d with args %v\n", ck.clientId, i, args)
		ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
		if !ok {
			DPrintf("client %v call get to server with args %v failed\n", ck.clientId, args)
			continue
		} else {
			if reply.Err == OK {
				ck.lastLeaderId = i
				DPrintf("leader success reply to client %v with args %v with reply %v\n", ck.clientId, args, reply)
				return reply.Value
			} else if reply.Err == ErrNoKey {
				ck.lastLeaderId = i
				break
			} else if reply.Err == ErrWrongLeader || reply.Err == ErrTimeOut {
				if reply.Err == ErrTimeOut {
					DPrintf("client %v call get to server with args %v timeout\n", ck.clientId, args)
				} else if reply.Err == ErrWrongLeader {
					DPrintf("client %v call get to server with args %v wrong leader\n", ck.clientId, args)
				}
				continue
			}
		}
	}
	return ""
}

//
// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.
	cmdId := ck.nextCmdId
	ck.nextCmdId++
	args := PutAppendArgs{Key: key, Value: value, Op: op, CmdId: cmdId, ClientId: ck.clientId}
	for i := ck.lastLeaderId; ; i = (i + 1) % len(ck.servers) {
		// if i == len(ck.servers)-1 {
		// 	time.Sleep(50 * time.Millisecond)
		// }
		reply := PutAppendReply{}
		DPrintf("client %v call put or append to server with args %v\n", ck.clientId, args)
		ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
		if !ok {
			DPrintf("client %v call put or append to server with args %v failed\n", ck.clientId, args)
			continue
		} else {
			if reply.Err == OK || reply.Err == ErrNoKey {
				ck.lastLeaderId = i
				DPrintf("leader success reply to client %v with args %v with reply %v\n", ck.clientId, args, reply)
				break
			} else if reply.Err == ErrWrongLeader || reply.Err == ErrTimeOut {
				if reply.Err == ErrTimeOut {
					DPrintf("client %v call put or append to server with args %v timeout\n", ck.clientId, args)
				} else if reply.Err == ErrWrongLeader {
					DPrintf("client %v call put or append to server with args %v wrong leader\n", ck.clientId, args)
				}
				continue
			}
		}
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}

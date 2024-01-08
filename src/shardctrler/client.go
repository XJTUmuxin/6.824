package shardctrler

//
// Shardctrler clerk.
//

import (
	"crypto/rand"
	"math/big"

	"6.824/labrpc"
)

type Clerk struct {
	servers []*labrpc.ClientEnd
	// Your data here.
	clientId     int64
	nextCmdId    uint64
	lastLeaderId int
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
	// Your code here.
	ck.nextCmdId = 1
	ck.clientId = nrand()
	ck.lastLeaderId = 0
	return ck
}

func (ck *Clerk) Query(num int) Config {
	args := &QueryArgs{}
	// Your code here.
	args.Num = num
	args.ClientId = ck.clientId
	args.CmdId = ck.nextCmdId
	ck.nextCmdId++

	// try each known server.
	for i := ck.lastLeaderId; ; i = (i + 1) % len(ck.servers) {
		var reply QueryReply
		DPrintf("begin query to crtl server %d with args %v", i, args)
		ok := ck.servers[i].Call("ShardCtrler.Query", args, &reply)
		if ok && reply.WrongLeader == false {
			ck.lastLeaderId = i
			DPrintf("query success to crtl server %d with ok: %v args: %v reply: %v", i, ok, args, reply)
			return reply.Config
		} else {
			DPrintf("query err to crtl server %d with ok: %v args: %v reply: %v", i, ok, args, reply)
		}
	}
}

func (ck *Clerk) Join(servers map[int][]string) {
	args := &JoinArgs{}
	// Your code here.
	args.Servers = servers
	args.ClientId = ck.clientId
	args.CmdId = ck.nextCmdId
	ck.nextCmdId++

	// try each known server.
	for i := ck.lastLeaderId; ; i = (i + 1) % len(ck.servers) {
		var reply JoinReply
		ok := ck.servers[i].Call("ShardCtrler.Join", args, &reply)
		if ok && reply.WrongLeader == false {
			ck.lastLeaderId = i
			return
		}
	}
}

func (ck *Clerk) Leave(gids []int) {
	args := &LeaveArgs{}
	// Your code here.
	args.GIDs = gids
	args.ClientId = ck.clientId
	args.CmdId = ck.nextCmdId
	ck.nextCmdId++

	// try each known server.
	for i := ck.lastLeaderId; ; i = (i + 1) % len(ck.servers) {
		var reply LeaveReply
		ok := ck.servers[i].Call("ShardCtrler.Leave", args, &reply)
		if ok && reply.WrongLeader == false {
			ck.lastLeaderId = i
			return
		}
	}
}

func (ck *Clerk) Move(shard int, gid int) {
	args := &MoveArgs{}
	// Your code here.
	args.Shard = shard
	args.GID = gid
	args.ClientId = ck.clientId
	args.CmdId = ck.nextCmdId
	ck.nextCmdId++

	// try each known server.
	for i := ck.lastLeaderId; ; i = (i + 1) % len(ck.servers) {
		var reply MoveReply
		ok := ck.servers[i].Call("ShardCtrler.Move", args, &reply)
		if ok && reply.WrongLeader == false {
			ck.lastLeaderId = i
			return
		}
	}
}

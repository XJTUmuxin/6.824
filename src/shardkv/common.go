package shardkv

//
// Sharded key/value server.
// Lots of replica groups, each running Raft.
// Shardctrler decides which group serves each shard.
// Shardctrler may change shard assignment from time to time.
//
// You will have to modify these definitions.
//

const (
	OK                = "OK"
	ErrNoKey          = "ErrNoKey"
	ErrWrongGroup     = "ErrWrongGroup"
	ErrWrongLeader    = "ErrWrongLeader"
	ErrTimeOut        = "ErrTimeOut"
	ErrWrongConfigNum = "ErrWrongConfigNum"
)

type Err string

// Put or Append
type PutAppendArgs struct {
	// You'll have to add definitions here.
	CmdId    uint64
	ClientId int64
	Key      string
	Value    string
	Op       string // "Put" or "Append"
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
}

type PutAppendReply struct {
	Err Err
}

type GetArgs struct {
	CmdId    uint64
	ClientId int64
	Key      string
	// You'll have to add definitions here.
}

type GetReply struct {
	Err   Err
	Value string
}

type TransferShardArgs struct {
	ConfigNum  int
	Shard      int
	KvMap      map[string]string
	LastCmdIds map[int64]uint64
	ShardKVId  int
}
type TransferShardReply struct {
	Err Err
}

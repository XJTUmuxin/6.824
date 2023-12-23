package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
	"strconv"
)

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

type RequestTaskArgs struct {
}

// request task state
const (
	success = 0
	wait    = 1
	finish  = 2
)

type RequestTaskReply struct {
	State     int
	TaskType  string
	InputFile string
	Id        int
	MapperNum int
}

type InotifyFinishArgs struct {
	TaskType string
	Id       int
}
type InotifyFinishReply struct {
	Success bool
}

// Add your RPC definitions here.

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}

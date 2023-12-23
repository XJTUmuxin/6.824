package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

const (
	toDo  = 0
	doing = 1
	done  = 2
)

type MapTask struct {
	state     int
	startTime time.Time
}
type ReduceTask struct {
	state     int
	startTime time.Time
}

type Coordinator struct {
	// Your definitions here.
	nReduce            int          // the reduce task num
	inputFiles         []string     // the input files name
	mapTasks           []MapTask    // the map tasks
	mapFinish          bool         // is all the map tasks finished
	mapFinishedTask    int          // finished map tasks num
	reduceTasks        []ReduceTask // the reduce tasks
	reduceFinish       bool         //is all the reduce tasks finished
	reduceFinishedTask int          // finished reduce tasks num
	lock               sync.Mutex   // the mutex lock
}

// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

func (c *Coordinator) RequestTask(args *RequestTaskArgs, reply *RequestTaskReply) error {
	c.lock.Lock()
	defer c.lock.Unlock()
	if !c.mapFinish {
		findTask := false
		for i, task := range c.mapTasks {
			if task.state == toDo {
				reply.InputFile = c.inputFiles[i]
				reply.Id = i
				c.mapTasks[i].state = doing
				c.mapTasks[i].startTime = time.Now()
				findTask = true
				break
			} else if task.state == doing {
				now := time.Now()
				diff := now.Sub(task.startTime)
				if diff > 10*time.Second {
					// crash
					reply.InputFile = c.inputFiles[i]
					reply.Id = i
					c.mapTasks[i].startTime = now
					findTask = true
					break
				}
			}
		}
		if !findTask {
			reply.State = wait
		} else {
			reply.State = success
			reply.TaskType = "Map"
		}
	} else if !c.reduceFinish {
		findTask := false
		for i, task := range c.reduceTasks {
			if task.state == toDo {
				reply.Id = i
				c.reduceTasks[i].state = doing
				c.reduceTasks[i].startTime = time.Now()
				findTask = true
				break
			} else if task.state == doing {
				now := time.Now()
				diff := now.Sub(task.startTime)
				if diff > 10*time.Second {
					// crash
					reply.Id = i
					c.reduceTasks[i].startTime = now
					findTask = true
					break
				}
			}
		}
		if !findTask {
			reply.State = wait
		} else {
			reply.State = success
			reply.TaskType = "Reduce"
			reply.MapperNum = len(c.inputFiles)
		}
	} else if c.reduceFinish {
		reply.State = finish
	}
	return nil
}

func (c *Coordinator) InotifyFinish(args *InotifyFinishArgs, reply *InotifyFinishReply) error {
	c.lock.Lock()
	defer c.lock.Unlock()
	if args.TaskType == "Map" {
		if c.mapTasks[args.Id].state == doing {
			c.mapTasks[args.Id].state = done
			c.mapFinishedTask++
			if c.mapFinishedTask == len(c.inputFiles) {
				c.mapFinish = true
			}
			reply.Success = true
		} else {
			reply.Success = false
		}
	} else if args.TaskType == "Reduce" {
		if c.reduceTasks[args.Id].state == doing {
			c.reduceTasks[args.Id].state = done
			c.reduceFinishedTask++
			if c.reduceFinishedTask == c.nReduce {
				c.reduceFinish = true
			}
			reply.Success = true
		} else {
			reply.Success = false
		}
	}
	return nil
}

//
// start a thread that listens for RPCs from worker.go
//
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {
	ret := false
	c.lock.Lock()
	defer c.lock.Unlock()
	// Your code here.
	if c.reduceFinish {
		ret = true
	}

	return ret
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}
	c.inputFiles = files
	c.mapTasks = make([]MapTask, len(c.inputFiles))
	c.nReduce = nReduce
	c.reduceTasks = make([]ReduceTask, nReduce)
	// Your code here.

	c.server()
	return &c
}

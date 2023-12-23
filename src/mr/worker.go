package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"time"
)

//
// Map functions return a slice of KeyValue.
//

type KeyValue struct {
	Key   string
	Value string
}

const nReduce = 10

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.
	for {
		reply := CallRequestTask()
		if reply.State == wait {
			time.Sleep(time.Second)
			continue
		} else if reply.State == success {
			if reply.TaskType == "Map" {
				MapWorker(reply.InputFile, reply.Id, mapf)
			} else if reply.TaskType == "Reduce" {
				ReduceWorker(reply.MapperNum, reply.Id, reducef)
			}
		} else if reply.State == finish {
			break
		}
	}
}

func MapWorker(inputfile string, workerId int, mapf func(string, string) []KeyValue) {
	file, err := os.Open(inputfile)
	if err != nil {
		log.Fatalf("cannot open %v", inputfile)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", inputfile)
	}
	file.Close()
	intermediate := mapf(inputfile, string(content))

	ofiles := make([]*os.File, nReduce)
	for i := 0; i < nReduce; i++ {
		ofiles[i], _ = ioutil.TempFile("", "mr")
		defer ofiles[i].Close()
	}
	encs := make([]*json.Encoder, nReduce)
	for i := 0; i < nReduce; i++ {
		encs[i] = json.NewEncoder(ofiles[i])
	}

	for _, kv := range intermediate {
		i := ihash(kv.Key) % nReduce
		encs[i].Encode(&kv)
	}
	// rename temp file
	currentDir, _ := os.Getwd()
	for i := 0; i < nReduce; i++ {
		targetFileName := "mr-" + strconv.Itoa(workerId) + "-" + strconv.Itoa(i)
		targetFilePath := filepath.Join(currentDir, targetFileName)
		os.Rename(ofiles[i].Name(), targetFilePath)
	}
	CallInotifyFinish("Map", workerId)
	// reply := CallInotifyFinish("Map", workerId)
	// fmt.Printf("map worker %d inotify finish %v\n", workerId, reply.Success)
}

func ReduceWorker(mapperNum int, workerId int, reducef func(string, []string) string) {
	intermediate := []KeyValue{}
	for i := 0; i < mapperNum; i++ {
		fileName := "mr-" + strconv.Itoa(i) + "-" + strconv.Itoa(workerId)
		file, _ := os.Open(fileName)
		defer file.Close()
		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			intermediate = append(intermediate, kv)
		}
	}
	sort.Sort(ByKey(intermediate))
	oname := "mr-out-" + strconv.Itoa(workerId)
	ofile, _ := os.Create(oname)
	i := 0
	for i < len(intermediate) {
		j := i + 1
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value)
		}
		output := reducef(intermediate[i].Key, values)

		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)

		i = j
	}
	ofile.Close()
	CallInotifyFinish("Reduce", workerId)
	// reply := CallInotifyFinish("Reduce", workerId)
	// fmt.Printf("reduce worker %d inotify finish %v\n", workerId, reply.Success)
}

// the worker request tast

func CallRequestTask() RequestTaskReply {
	args := RequestTaskArgs{}

	reply := RequestTaskReply{}

	ok := call("Coordinator.RequestTask", &args, &reply)
	if !ok {
		fmt.Printf("call RequestTask failed!\n")
	}
	return reply
}

func CallInotifyFinish(taskType string, Id int) InotifyFinishReply {
	args := InotifyFinishArgs{}

	args.TaskType = taskType
	args.Id = Id

	reply := InotifyFinishReply{}

	ok := call("Coordinator.InotifyFinish", &args, &reply)
	if !ok {
		fmt.Printf("call InotifyFinish failed!\n")
	}
	return reply
}

//
// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
//
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.Example", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("reply.Y %v\n", reply.Y)
	} else {
		fmt.Printf("call failed!\n")
	}
}

//
// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}

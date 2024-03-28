package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"log"
	"net/rpc"
	"os"
	"sort"
	"time"
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

var MyWorkerId int

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	args := RequestArgs{}
	reply := RequestReply{}
	call("Coordinator.Register", &args, &reply)
	MyWorkerId = reply.AssignId
	debugLogger.Println("MyWorkerId: ", MyWorkerId)
	// Your worker implementation here.
	var retry int = 0
	for {
		// request task from coordinator
		args := RequestArgs{WorkerId: MyWorkerId}
		reply := RequestReply{}
		ok := call("Coordinator.RequestTask", &args, &reply)
		debugLogger.Printf("%d ask tasks %d\n", MyWorkerId, reply.Task.TaskType)
		if !ok {
			retry++
			if retry > MaximumRetry {
				reply.Task.TaskType = Exit
				fmt.Printf("Unable to contact coordinator\n")
			}
		} else {
			retry = 0
		}
		switch reply.Task.TaskType {
		case Exit:
			debugLogger.Println("exit the process, ", reply.Task)
			os.Exit(0)
		case Wait:
			time.Sleep(SleepTime)
		case Map:
			doMapTask(&reply, mapf)
		case Reduce:
			doReduceTask(&reply, reducef)
		}
	}
}

func doMapTask(requestReply *RequestReply, mapf func(string, string) []KeyValue) {
	//debugLogger.Println("do map tasks\n")
	// get file name, file content
	data, err := os.ReadFile(requestReply.Task.InputFiles[0])
	if err != nil {
		log.Fatalf("cannot open %v", requestReply.Task.InputFiles[0])
	}
	key := requestReply.Task.InputFiles[0]
	value := string(data)

	// execute map function
	kva := mapf(key, value)
	intermediateFiles := make([][]KeyValue, requestReply.NReduce)

	// distribute files to different jobs
	for _, kv := range kva {
		reduceId := ihash(kv.Key) % requestReply.NReduce
		intermediateFiles[reduceId] = append(intermediateFiles[reduceId], kv)
	}
	// rename file≠
	for reduceId, kva := range intermediateFiles {
		fileName := fmt.Sprintf("mr-%d-%d", requestReply.Task.TaskID, reduceId)
		writeFile(fileName, &kva, true)
	}

	reply := RequestReply{}
	completeNotification := CompleteNotification{Task: requestReply.Task, WorkerId: MyWorkerId}

	// update task information
	call("Coordinator.NotifyFinsished", &completeNotification, &reply)

}

func writeFile(FileName string, kva *[]KeyValue, useJson bool) {
	// create temporary file
	tmpFile, err := os.CreateTemp("", "example")
	if err != nil {
		fmt.Println("Error creating temp file:", err)
		return
	}
	// fmt.Println("Temp file created:", tmpFile.Name())
	defer os.Remove(tmpFile.Name()) // Clean up

	//----------------------------------------------------------------
	if useJson {
		enc := json.NewEncoder(tmpFile)
		for _, kv := range *kva {
			enc.Encode(kv)
		}
	} else {
		for _, kv := range *kva {
			fmt.Fprintf(tmpFile, "%v %v\n", kv.Key, kv.Value)
		}
	}
	//----------------------------------------------------------------

	// Close and rename tmp file
	if err := tmpFile.Close(); err != nil {
		fmt.Println("Error closing temp file:", err)
	}
	// } else {
	// 	fmt.Println("files:", FileName)
	// }
	os.Rename(tmpFile.Name(), FileName)

}

func doReduceTask(requestReply *RequestReply, reducef func(string, []string) string) {
	//debugLogger.Println("do reduce tasks\n")
	// 1. combine input files
	intermediateFiles := []KeyValue{}
	for _, fileName := range requestReply.Task.InputFiles {
		file, err := os.Open(fileName)
		if err != nil {
			log.Fatalf("cannot open %v", fileName)
		}
		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			intermediateFiles = append(intermediateFiles, kv)
		}
	}
	// 2. sort keys
	sort.Slice(intermediateFiles, func(i int, j int) bool {
		return intermediateFiles[i].Key < intermediateFiles[j].Key
	})
	// 3. create output file and apply reduce function
	res := []KeyValue{}
	i := 0
	for i < len(intermediateFiles) {
		k := intermediateFiles[i].Key
		v := []string{}
		j := i
		for ; j < len(intermediateFiles) && intermediateFiles[j].Key == k; j++ {
			v = append(v, intermediateFiles[j].Value)
		}
		i = j
		kv := KeyValue{
			Key:   k,
			Value: reducef(k, v),
		}
		res = append(res, kv)
	}
	fileName := fmt.Sprintf("mr-out-%d", requestReply.Task.TaskID)
	writeFile(fileName, &res, false)
	debugLogger.Println("reduce output:", fileName)
	// 4.notify complete
	reply := RequestReply{}
	completeNotification := CompleteNotification{Task: requestReply.Task, WorkerId: MyWorkerId}

	// update task information
	call("Coordinator.NotifyFinsished", &completeNotification, &reply)
}

// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
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

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
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

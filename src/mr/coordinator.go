package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

type Coordinator struct {

	// Your definitions here.
	MapSchedule Schedule

	ReduceSchedule Schedule

	Mutex sync.Mutex

	Input []string

	RId int
}

type Schedule struct {
	// task list (task id->task)
	Map map[int]*Task // be careful when init
	// total task number
	Number int
	// finished task number
	CompletedTaskNumber int
	// assigned task number
	AssignedTaskNumber int
}

// Your code here -- RPC handlers for the worker to call.

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

func (c *Coordinator) Register(args *CompleteNotification, reply *RequestReply) error {
	c.Mutex.Lock()
	defer c.Mutex.Unlock()
	// set the task status, change the number
	reply.AssignId = c.RId
	c.RId++
	return nil
}

func (c *Coordinator) NotifyFinsished(args *CompleteNotification, reply *RequestReply) error {
	c.Mutex.Lock()
	defer c.Mutex.Unlock()
	// set the task status, change the number
	if args.Task.TaskType == Map && c.MapSchedule.Map[args.Task.TaskID].WorkerId == args.WorkerId {
		c.MapSchedule.Map[args.Task.TaskID].Status = Finsished
		c.MapSchedule.CompletedTaskNumber++
	} else if c.ReduceSchedule.Map[args.Task.TaskID].WorkerId == args.WorkerId {
		c.ReduceSchedule.Map[args.Task.TaskID].Status = Finsished
		c.ReduceSchedule.CompletedTaskNumber++
	}
	return nil
}

func (c *Coordinator) RequestTask(args *RequestArgs, reply *RequestReply) error {

	c.Mutex.Lock()
	defer c.Mutex.Unlock()
	delete()
	// 1. assign map task
	if c.MapSchedule.AssignedTaskNumber < c.MapSchedule.Number {
		for i := 0; i < c.MapSchedule.Number; i++ {
			index := (i + c.MapSchedule.AssignedTaskNumber) % c.MapSchedule.Number
			if c.MapSchedule.Map[index].Status == Unassigned {
				// update schedule status
				c.MapSchedule.Map[index].Status = Assigned
				c.MapSchedule.Map[index].StartTime = time.Now()
				c.MapSchedule.AssignedTaskNumber++
				c.MapSchedule.Map[index].WorkerId = args.WorkerId
				c.MapSchedule.Map[index].InputFiles = append(c.MapSchedule.Map[index].InputFiles, c.Input[index])

				// assign task to worker
				reply.Task = *c.MapSchedule.Map[index]
				reply.NReduce = c.ReduceSchedule.Number
				debugLogger.Println("reply.Task.TaskType:", reply.Task.TaskType)
				return nil
			}
		}
	}
	// 2. wait map task to complete
	if c.MapSchedule.CompletedTaskNumber < c.MapSchedule.Number {
		// relaunch if possible
		for i := 0; i < c.MapSchedule.Number; i++ {
			if c.MapSchedule.Map[i].Status == Finsished {
				continue
			}

			if time.Since(c.MapSchedule.Map[i].StartTime) > MaximumTaskTime {
				debugLogger.Println("time lasts:", c.MapSchedule.Map[i].StartTime, " ", time.Now(), " ", time.Since(c.MapSchedule.Map[i].StartTime))
				// update schedule status
				c.MapSchedule.Map[i].Status = Assigned
				c.MapSchedule.Map[i].StartTime = time.Now()
				debugLogger.Println("reassign map:", c.MapSchedule.Map[i].WorkerId, " to:", args.WorkerId)
				c.MapSchedule.Map[i].WorkerId = args.WorkerId

				//c.MapSchedule.Map[i].InputFiles = append(c.MapSchedule.Map[i].InputFiles, c.Input[index])

				// assign task to worker
				reply.Task = *c.MapSchedule.Map[i]
				reply.NReduce = c.ReduceSchedule.Number
				return nil
			}

		}
		// all normal, let it wait
		// assign waiting task
		reply.Task.TaskType = Wait
		return nil
	}

	// 3. assign reduce task
	if c.ReduceSchedule.AssignedTaskNumber < c.ReduceSchedule.Number {
		for i := 0; i < c.ReduceSchedule.Number; i++ {
			index := (i + c.ReduceSchedule.AssignedTaskNumber) % c.ReduceSchedule.Number
			if c.ReduceSchedule.Map[index].Status == Unassigned {
				// update schedule status
				c.ReduceSchedule.Map[index].Status = Assigned
				c.ReduceSchedule.Map[index].StartTime = time.Now()
				c.ReduceSchedule.AssignedTaskNumber++
				c.ReduceSchedule.Map[index].WorkerId = args.WorkerId

				// assign task to worker
				reply.Task = *c.ReduceSchedule.Map[index]
				return nil
			}
		}
	}

	// 4. wait reduce task to complete
	if c.ReduceSchedule.CompletedTaskNumber < c.ReduceSchedule.Number {
		// relaunch if possible
		for i := 0; i < c.ReduceSchedule.Number; i++ {
			if c.ReduceSchedule.Map[i].Status == Finsished {
				continue
			}
			if time.Since(c.ReduceSchedule.Map[i].StartTime) > MaximumTaskTime {
				debugLogger.Println("time lasts:", c.ReduceSchedule.Map[i].StartTime, " ", time.Now(), " ", time.Since(c.ReduceSchedule.Map[i].StartTime))
				// update schedule status
				c.ReduceSchedule.Map[i].Status = Assigned
				c.ReduceSchedule.Map[i].StartTime = time.Now()
				debugLogger.Println("reassign reduce:", c.ReduceSchedule.Map[i].WorkerId, " to:", args.WorkerId)
				c.ReduceSchedule.Map[i].WorkerId = args.WorkerId

				//c.MapSchedule.Map[i].InputFiles = append(c.MapSchedule.Map[i].InputFiles, c.Input[index])

				// assign task to worker
				reply.Task = *c.ReduceSchedule.Map[i]
				reply.NReduce = c.ReduceSchedule.Number
				return nil
			}

		}
		// all normal, let it wait
		// assign waiting task
		reply.Task.TaskType = Wait
		return nil
	}
	debugLogger.Printf("AssignedTaskNumber%d Number%d \n", c.ReduceSchedule.AssignedTaskNumber, c.ReduceSchedule.Number)
	// // 5. assign exit
	reply.Task.TaskType = Exit
	return nil
}

// start a thread that listens for RPCs from worker.go
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

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	ret := false

	// Your code here.
	c.Mutex.Lock()
	defer c.Mutex.Unlock()
	ret = c.MapSchedule.CompletedTaskNumber == c.MapSchedule.Number && c.ReduceSchedule.CompletedTaskNumber == c.ReduceSchedule.Number
	return ret
}

func (c *Coordinator) Init(files []string, nReduce int) {
	nMap := len(files)

	c.MapSchedule.Number = nMap
	for i := 0; i < nMap; i++ {
		c.MapSchedule.Map[i] = &Task{
			TaskType: TaskType(Map),
			TaskID:   i,
			Status:   Unassigned,
		}
	}

	c.ReduceSchedule.Number = nReduce
	for i := 0; i < nReduce; i++ {
		c.ReduceSchedule.Map[i] = &Task{
			TaskType: TaskType(Reduce),
			TaskID:   i,
			Status:   Unassigned,
		}
		for j := 0; j < nMap; j++ {
			fileName := fmt.Sprintf("mr-%d-%d", j, i)
			c.ReduceSchedule.Map[i].InputFiles = append(c.ReduceSchedule.Map[i].InputFiles, fileName)
		}
	}

}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		MapSchedule: Schedule{
			Map: make(map[int]*Task),
		},
		ReduceSchedule: Schedule{
			Map: make(map[int]*Task),
		},

		Mutex: sync.Mutex{},
		Input: files,
	}

	// Your code here.
	c.Init(files, nReduce)
	c.server()
	return &c
}

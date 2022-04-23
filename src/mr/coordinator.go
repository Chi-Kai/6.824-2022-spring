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

type Task struct {
	TaskType string
	TaskId   int
	WorkerId int
	FileName string
	// check if the worker is timeout
	Time time.Time
	// if task is ok
	Status string
}

type Coordinator struct {
	// Your definitions here.
	// mu to lock the map
	mu sync.Mutex
	// stage is map or reduce
	Stage string
	// read the map files
	mapFiles []Task
	// read the reduce files
	reduceFiles []Task
	// the number of map files = len(mapFiles)
	nMapFiles int
	// the number of reduce files
	nReduceFiles int
	// taskpool is the task pool
	TaskPool chan Task
}

// Your code here -- RPC handlers for the worker to call.
func (c *Coordinator) GetTask(args *TaskArgs, reply *TaskReply) error {
	// 得到args
	//workerId := args.WorkerId
	//lasttask := args.LastTask
	// 开始对coordinator的锁
	c.mu.Lock()
	defer c.mu.Unlock()
	// 如果taskpool 是空的,表示该阶段已经完成
	if len(c.TaskPool) == 0 {
		// 当前stage 是map,但是没有任务了,则进入reduce stage
		if c.Stage == "map" {
			c.Stage = "reduce"
			// 初始化reduceFiles
			for i := 0; i < c.nReduceFiles; i++ {
				task := Task{TaskType: "reduce", TaskId: i, Time: time.Now(), Status: "waiting"}
				c.reduceFiles[i] = task
				c.TaskPool <- task
			}
		}
		// 当前stage 是reduce,但是没有任务了,则stage 是done
		if c.Stage == "reduce" {
			c.Stage = "done"
		}

	}
	// 选择不同的stage 运行
	switch c.Stage {
	case "map":
		c.MapDone(args, reply)
	case "reduce":
		//ReduceDone(args, reply)
	case "done":

	}

	return nil
}

// handler for map
func (c *Coordinator) MapDone(args *TaskArgs, reply *TaskReply) error {
	workerId := args.WorkerId
	lasttask := args.LastTask
	// 如果有要提交的任务,则提交
	if lasttask != -1 {
		c.mapFiles[lasttask].Status = "finish"
		c.mapFiles[lasttask].Time = time.Now()
	}
	// 如果taskpool 不是空的,返回一个任务
	task := <-c.TaskPool
	// 更新task
	task.WorkerId = workerId
	task.Time = time.Now()
	task.Status = "running"
	c.mapFiles[task.TaskId] = task
	// 返回任务
	reply = &TaskReply{
		Tasktype: task.TaskType,
		Taskid:   task.TaskId,
		Filename: task.FileName,
		Nmap:     c.nMapFiles,
		Nreduce:  c.nReduceFiles,
	}
	return nil
}

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
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

	// Your code here.

	return ret
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	// 创建一个Coordinator
	c := Coordinator{
		mu:           sync.Mutex{},
		Stage:        "map",
		nMapFiles:    len(files),
		nReduceFiles: nReduce,
		mapFiles:     make([]Task, len(files)),
		reduceFiles:  make([]Task, nReduce),
		TaskPool:     make(chan Task, len(files)),
	}
	//map stage
	log.Fatal("map stage start")
	//创建一个task pool 使用缓冲为len(files) 的channel
	c.TaskPool = make(chan Task, len(files))
	for i, file := range files {
		// 放入mapFiles 和 TaskPool
		task := Task{TaskType: "map", TaskId: i, FileName: file, Time: time.Now(), Status: "waiting"}
		c.mapFiles[i] = task
		c.TaskPool <- task
	}
	log.Fatal("tasks load finish")
	//一个goroutine 执行检查是否超时
	go func() {
	}()
	c.server()
	return &c
}

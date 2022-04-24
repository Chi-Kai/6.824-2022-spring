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
	// the number of unfinished task
	nTask int
	// all tasks are done
	done bool
}

// Your code here -- RPC handlers for the worker to call.
func (c *Coordinator) GetTask(args *TaskArgs, reply *TaskReply) error {
	// 得到args
	//workerId := args.WorkerId
	//lasttask := args.LastTask
	// 开始对coordinator的锁
	c.mu.Lock()
	defer c.mu.Unlock()
	// nTask为0,表示该阶段已经完成
	if c.nTask == 0 {
		// map阶段已经完成
		if c.Stage == "map" {
			c.Stage = "reduce"
			// 初始化reduceFiles
			for i := 0; i < c.nReduceFiles; i++ {
				task := Task{TaskType: "reduce", TaskId: i, Time: time.Now(), Status: "waiting"}
				c.reduceFiles[i] = task
				c.TaskPool <- task
				c.nTask++
			}
			// 启动goroutine 检查超时
			go func() {
				for {
					for _, task := range c.reduceFiles {
						if task.Status == "waiting" {
							continue
						}
						if time.Now().Sub(task.Time) > time.Second*10 {
							log.Fatalf("reduce task %d in worker %d timeout", task.TaskId, task.WorkerId)
							// 超时
							task.Status = "timeout"
							// 将超时的任务放入taskpool
							c.TaskPool <- task
							c.nTask++
						}
					}
				}
			}()
			// 为了防止map阶段完成后,lasttask为map的最后一个任务,而reduce阶段没有任务,在切换stage后重置lasttask
			log.Fatalf("map stage is finished, reduce stage is starting")
			//返回一个wait 任务
			reply.Taskid = -1
			reply.Tasktype = "wait"
			return nil
		}
		// 当前stage 是reduce,但是没有任务了,则stage 是done
		if c.Stage == "reduce" {
			c.Stage = "done"
			c.done = true
			// 返回一个finish 任务
			reply.Tasktype = "finish"
			return nil
		}

	}
	// 如果pool为空，但nTask不为0,则表示有任务没有被处理,返回一个wait task
	if c.nTask > 0 && len(c.TaskPool) == 0 {
		reply.Tasktype = "wait"
		reply.Taskid = -1
		return nil

	}
	// 选择不同的stage 运行
	switch c.Stage {
	case "map":
		c.MapDone(args, reply)
	case "reduce":
		c.ReduceDone(args, reply)
	case "done":
		// 运行不到
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
		// 确认提交
		// 由这个worker 完成的一系列文件
		for i := 0; i < c.nReduceFiles; i++ {
			createMapResFile(lasttask, workerId, i)
		}
		// 任务数减一
		c.nTask--
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

// handler for reduce
func (c *Coordinator) ReduceDone(args *TaskArgs, reply *TaskReply) error {
	workerId := args.WorkerId
	lasttask := args.LastTask
	// 如果有要提交的任务,则提交
	if lasttask != -1 {
		c.reduceFiles[lasttask].Status = "finish"
		c.reduceFiles[lasttask].Time = time.Now()
		// 确认提交
		createReduceResFile(lasttask, workerId)
		// 任务数减一
		c.nTask--
	}
	// 如果taskpool 不是空的,返回一个任务
	task := <-c.TaskPool
	// 更新task
	task.WorkerId = workerId
	task.Time = time.Now()
	task.Status = "running"
	c.reduceFiles[task.TaskId] = task
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
	return c.done
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
		nTask:        0,
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
		c.nTask++
	}
	log.Fatal("tasks load finish")
	//一个goroutine 执行检查是否超时
	go func() {
		for {
			for _, task := range c.mapFiles {
				if task.Status == "waiting" {
					continue
				}
				// 如果超时,则设置为waiting
				if time.Now().Sub(task.Time) > time.Second*10 {
					log.Fatalf("task %d in work %d timeout", task.TaskId, task.WorkerId)
					task.Status = "waiting"
					task.Time = time.Now()
					c.TaskPool <- task
					c.nTask++
				}
			}
		}
	}()
	c.server()
	return &c
}

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
	// waiting running finish
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
	// 加锁
	c.mu.Lock()
	defer c.mu.Unlock()

	// 选择不同的stage 运行
	switch c.Stage {
	case "map":
		c.MapDone(args, reply)
	case "reduce":
		c.ReduceDone(args, reply)
	}
	// 进行stage 检测
	// nTask为0,表示该阶段已经完成
	if c.nTask == 0 {
		// 当前stage 是reduce,但是没有任务了,则stage 是done
		if c.Stage == "done" {
			reply.Tasktype = "finish"
			return nil
		}
		if c.Stage == "reduce" {
			c.Stage = "done"
			c.done = true
			// 返回一个finish 任务
			reply.Tasktype = "finish"
			log.Println("-----Finish-----")
			close(c.TaskPool)
			// worker 发送请求的时候，master 已经退出了
			return nil
		}
		// map阶段已经完成
		if c.Stage == "map" {
			log.Printf("reduce stage start")
			c.Stage = "reduce"
			// 关闭taskpool
			close(c.TaskPool)
			// 初始化reduce taskpool,任务数为nReduceFiles * nMapFiles
			c.TaskPool = make(chan Task, c.nReduceFiles*c.nMapFiles)
			// 初始化reduceFiles,读入mr-X-Y 格式的文件
			for i := 0; i < c.nReduceFiles; i++ {
				task := Task{TaskType: "reduce", TaskId: i, WorkerId: -1, FileName: "", Time: time.Now(), Status: "wait"}
				c.reduceFiles[i] = task
				c.TaskPool <- task
				c.nTask++
			}
			log.Printf("files load is ok")
			// 启动goroutine 检查超时
			go func() {
				for {
					time.Sleep(time.Millisecond * 300)
					c.mu.Lock()
					for i, task := range c.reduceFiles {
						if task.Status == "waiting" || task.Status == "finish" {
							continue

						}
						if time.Now().Sub(task.Time) > time.Second*10 {
							log.Printf("reduce task %d in worker %d timeout\n", task.TaskId, task.WorkerId)
							// 加锁
							// 如果超时,则重新提交
							task.Status = "waiting"
							task.Time = time.Now()
							c.reduceFiles[i] = task
							c.TaskPool <- task
						}
					}
					c.mu.Unlock()
				}
			}()
			// 为了防止map阶段完成后,lasttask为map的最后一个任务,而reduce阶段没有任务,在切换stage后重置lasttask
			log.Printf("map stage is finished, reduce stage is starting")
			//返回一个wait 任务
			reply.Taskid = -1
			reply.Tasktype = "wait"
			return nil
		}

	}
	//log.Printf("send task %d pool %d nTask %d", reply.Taskid, len(c.TaskPool), c.nTask)
	return nil
}

// handler for map
func (c *Coordinator) MapDone(args *TaskArgs, reply *TaskReply) error {
	workerId := args.WorkerId
	lasttask := args.LastTask
	// 如果有要提交的任务,则提交
	if lasttask != -1 {
		// 由这个worker 完成的一系列文件
		i := 0
		for ; i < c.nReduceFiles; i++ {
			err := createMapResFile(lasttask, workerId, i)
			if err != nil {
				log.Printf("create map res file error %v", err)
				break
			}
		}
		// 如果所有提交， 任务数减一
		if i == c.nReduceFiles {
			// 确认提交
			c.mapFiles[lasttask].Status = "finish"
			c.mapFiles[lasttask].Time = time.Now()
			c.nTask--
		}
	}
	// 如果pool为空，但nTask不为0,则表示有任务没有被处理,返回一个wait task
	if c.nTask > 0 && len(c.TaskPool) == 0 {
		reply.Tasktype = "wait"
		reply.Taskid = -1
		return nil
	}
	// 如果map完成，则返回
	if c.nTask == 0 {
		return nil
	}

	// 如果taskpool 不是空的,返回一个任务
	task := <-c.TaskPool
	// 更新task
	task.WorkerId = workerId
	task.Time = time.Now()
	task.Status = "running"
	c.mapFiles[task.TaskId] = task
	// 返回任务
	// 不能使用 reply = &task更新
	reply.Tasktype = task.TaskType
	reply.Taskid = task.TaskId
	reply.Filename = task.FileName
	reply.Nmap = c.nMapFiles
	reply.Nreduce = c.nReduceFiles
	return nil
}

// handler for reduce
func (c *Coordinator) ReduceDone(args *TaskArgs, reply *TaskReply) error {

	workerId := args.WorkerId
	lasttask := args.LastTask
	// 如果有要提交的任务,则提交
	if lasttask != -1 {
		err := createReduceResFile(lasttask, workerId)
		if err != nil {
			log.Printf("create reduce res file error %v", err)
		} else {
			// 确认提交
			c.reduceFiles[lasttask].Status = "finish"
			c.reduceFiles[lasttask].Time = time.Now()

			// 任务数减一
			c.nTask--
		}
	}

	if c.nTask > 0 && len(c.TaskPool) == 0 {
		reply.Tasktype = "wait"
		reply.Taskid = -1
		return nil
	}
	// 如果reduceTask完成，则返回一个wait 任务
	if c.nTask == 0 {
		return nil
	}

	// 如果taskpool 不是空的,返回一个任务
	task := <-c.TaskPool
	// 更新task
	task.WorkerId = workerId
	task.Time = time.Now()
	task.Status = "running"
	c.reduceFiles[task.TaskId] = task

	reply.Tasktype = task.TaskType
	reply.Taskid = task.TaskId
	reply.Filename = task.FileName
	reply.Nmap = c.nMapFiles
	reply.Nreduce = c.nReduceFiles
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
		log.Println("listen error: s", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
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
		done:         false,
	}
	//map stage
	log.Printf("map stage start")
	//创建一个task pool 使用缓冲为len(files) 的channel
	for i, file := range files {
		// 放入mapFiles 和 TaskPool
		task := Task{TaskType: "map", TaskId: i, FileName: file, Time: time.Now(), Status: "waiting"}
		c.mapFiles[i] = task
		c.TaskPool <- task
		c.nTask++
	}
	log.Printf("tasks load finish: %d", c.nTask)
	//一个goroutine 执行检查是否超时
	go func() {
		for {
			c.mu.Lock()
			// 如果任务完成，则退出
			if c.Stage == "reduce" {
				c.mu.Unlock()
				break
			}
			// 缓缓 防止影响处理进程
			time.Sleep(time.Millisecond * 300)
			// 返回的是
			for i, task := range c.mapFiles {
				if task.Status == "waiting" || task.Status == "finish" {
					continue
				}
				// 如果超时,则设置为waiting
				if time.Now().Sub(task.Time) > time.Second*10 {
					log.Printf("map task %d in work %d timeout nTask %d\n", task.TaskId, task.WorkerId, c.nTask)
					// 更改mapFiles
					task.Status = "waiting"
					task.Time = time.Now()
					c.mapFiles[i] = task
					c.TaskPool <- task
					// 存在一个任务也没有完成，而且超时。说明没有提交
					//c.nTask++
				}

			}
			c.mu.Unlock()
		}
	}()
	log.Printf("checktimeout running")
	c.server()
	return &c
}

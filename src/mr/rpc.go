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

// task args from worker to master
type TaskArgs struct {
	WorkerId int
	// 返回上一次的任务的id 说明已经完成了，master 更新时间戳，如果返回 -1 说明是之前没有完成的任务
	LastTask int
}

// task reply from master to worker
type TaskReply struct {
	// task type is map or reduce
	Tasktype string
	// task id is the index of the task
	Taskid int
	// the number of map tasks
	Nmap int
	// the number of reduce tasks
	Nreduce int
	// the file name to process
	Filename string
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}

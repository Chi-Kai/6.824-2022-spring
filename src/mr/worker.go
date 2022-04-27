package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"sort"
	"time"
)

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

// create a tmp file for mapres
// 临时文件防止多个map任务同时写入同一个文件
func createMapTmpFile(mapid int, reduceid int) string {
	// 建立临时文件mr-workid-mapid-reduceid
	filename := fmt.Sprintf("mr-%d-%d-%d", os.Getpid(), mapid, reduceid)
	os.Remove(filename)
	f, err := os.Create(filename)
	if err != nil {
		log.Printf("create file error %s:", err)
	}
	f.Close()
	return filename
}

// create a res file for map
func createMapResFile(mapid int, workid int, reduceid int) error {
	// 将文件名改为m-taskid
	oldname := fmt.Sprintf("mr-%d-%d-%d", workid, mapid, reduceid)
	// 文件是否存在
	if _, err := os.Stat(oldname); err != nil {
		// 不存在
		return fmt.Errorf("file %s not exist", oldname)
	}
	filename := fmt.Sprintf("mr-%d-%d", mapid, reduceid)
	os.Remove(filename)
	os.Rename(oldname, filename)
	return nil
}

// create a tmp file for reduce
func createReduceTmpFile(taskid int) string {
	filename := fmt.Sprintf("r-%d-%d", os.Getpid(), taskid)
	os.Remove(filename)
	f, err := os.Create(filename)
	if err != nil {
		log.Fatal("create file error:", err)
	}
	f.Close()
	return filename
}

// create a res file for reduce
func createReduceResFile(taskid int, workid int) error {
	// 将文件名改为mr-out-taskid
	oldname := fmt.Sprintf("r-%d-%d", workid, taskid)
	if _, err := os.Stat(oldname); err != nil {
		// 不存在
		return fmt.Errorf("file %s not exist", oldname)
	}
	filename := fmt.Sprintf("mr-out-%d", taskid)
	os.Remove(filename)
	os.Rename(oldname, filename)
	return nil
}

// getMapTmpFiles
func getMapTmpFiles(taskid int, nMap int) []string {
	var files []string
	for i := 0; i < nMap; i++ {
		files = append(files, fmt.Sprintf("mr-%d-%d", i, taskid))
	}
	return files
}

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
// 将相同的key分配到同一个reduce任务中
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

	// 使用进程号作为worker的ID
	workerId := os.Getpid()
	// 向coordinator请求
	// 初次请求时,lasttask为-1
	args := TaskArgs{
		WorkerId: workerId,
		LastTask: -1,
	}
	reply := TaskReply{}

	for {
		//log.Printf("lasttask:%d", args.LastTask)
		// 向coordinator请求任务
		ok := call("Coordinator.GetTask", &args, &reply)
		if !ok {

			log.Printf("Coordinator.GetTask error,workerId:%d", workerId)
		}
		//if reply.Tasktype != "wait" {
		log.Printf("taskId:%d,taskType:%s in worker %d", reply.Taskid, reply.Tasktype, workerId)
		//}
		// 打个补丁 不知道为啥 会漏掉一个task
		if reply.Taskid == -1 && reply.Tasktype != "wait" {
			reply.Taskid = 0
		}
		//time.Sleep(time.Second)
		// 如果完成了所有任务,退出
		if reply.Tasktype == "finish" {
			log.Printf("workerId:%d finish", workerId)
			break
		}
		if reply.Tasktype == "wait" {
			// 没有任务，等待
			//log.Printf("worker %d is waiting", workerId)
			time.Sleep(time.Second)
			// 更新args
			args.LastTask = reply.Taskid
			continue
		}
		if reply.Tasktype == "map" {
			// map任务
			mapTask(reply.Taskid, reply.Filename, reply.Nreduce, mapf)
		} else if reply.Tasktype == "reduce" {
			// reduce任务
			reduceTask(reply.Taskid, reply.Filename, reply.Nmap, reducef)
		} else {
			log.Printf("unknown task type:%s from worker: %d", reply.Tasktype, workerId)
		}
		// 更新args
		args.LastTask = reply.Taskid
	}
}

// mapTask
func mapTask(taskid int, filename string, nReduce int, mapf func(string, string) []KeyValue) {

	file, err := os.Open(filename)
	if err != nil {
		log.Printf("cannot open %v", filename)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Printf("cannot read %v", filename)
	}
	file.Close()
	kva := mapf(filename, string(content))
	// 根据key分配到同一个reduce任务中
	hashmap := make(map[int][]KeyValue)
	for _, kv := range kva {
		hash := ihash(kv.Key)
		// 将hash 限定到0~nReduce-1
		hash = hash % nReduce
		hashmap[hash] = append(hashmap[hash], kv)
	}
	// 写入临时文件
	for i := 0; i < nReduce; i++ {
		tmpFile := createMapTmpFile(taskid, i)
		// 打开文件读写,不然会写入失败
		tmp, err := os.OpenFile(tmpFile, os.O_APPEND|os.O_WRONLY, 0644)
		defer tmp.Close()
		if err != nil {
			log.Printf("cannot open %v", tmpFile)
		}
		enc := json.NewEncoder(tmp)
		// 将hashmap中的keyvalue写入对应临时文件
		kva := hashmap[i]
		for _, kv := range kva {
			err := enc.Encode(&kv)
			if err != nil {
				log.Printf("cannot encode %v,err %v", kv, err)
			}

		}
	}
	//log.Printf("map task %d finish", taskid)
}

// reduceTask
func reduceTask(taskid int, filename string, nMap int, reducef func(string, []string) string) {
	if taskid < 0 {
		log.Printf("taskid:%d is invalid", taskid)
		return
	}
	// 读入相同taskid的所有临时文件
	files := getMapTmpFiles(taskid, nMap)

	// 创建一个空的map
	kva := []KeyValue{}
	for _, file := range files {
		// 读取临时文件
		tmp, err := os.Open(file)
		defer tmp.Close()
		if err != nil {
			log.Printf("cannot open %v", file)
		}
		dec := json.NewDecoder(tmp)
		for {
			var kv KeyValue
			err := dec.Decode(&kv)
			if err != nil {
				break
			}
			kva = append(kva, kv)
		}
		// 删除临时文件
		os.Remove(file)
	}

	// 排序
	sort.Sort(ByKey(kva))
	// 写入临时文件
	tmpFile := createReduceTmpFile(taskid)
	tmp, err := os.OpenFile(tmpFile, os.O_APPEND|os.O_WRONLY, 0644)
	defer tmp.Close()
	if err != nil {
		log.Printf("cannot open %v", tmpFile)
	}
	//log.Printf("reduce task %d log", taskid)
	// 从mrsequential.go中获取
	i := 0
	for i < len(kva) {
		j := i + 1
		for j < len(kva) && kva[j].Key == kva[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, kva[k].Value)
		}
		output := reducef(kva[i].Key, values)

		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(tmp, "%v %v\n", kva[i].Key, output)

		i = j
	}
	//log.Printf("reduce task %d finish", taskid)
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

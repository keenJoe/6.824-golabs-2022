package mr

import (
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"sort"
	"strings"
	"time"
)

// KeyValue 是用于存储map输出和reduce输入的键值对
type KeyValue struct {
	Key   string
	Value string
}

// 用于排序KeyValue数组
type ByKey []KeyValue

func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

// Worker执行MapReduce任务
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	workerId := os.Getpid()
	retryCount := 0
	maxRetries := 3

	for {
		task, err := getTask(workerId)
		if err != nil {
			log.Printf("Error getting task: %v", err)
			retryCount++
			if retryCount > maxRetries {
				log.Printf("Maximum retries reached, waiting longer...")
				time.Sleep(3 * time.Second)
				retryCount = 0
			} else {
				time.Sleep(time.Second)
			}
			continue
		}

		// 成功获取任务后重置重试计数
		retryCount = 0

		switch task.TaskType {
		case MapTask:
			err := doMap(task, mapf, workerId)
			if err != nil {
				log.Printf("Map task %d failed: %v", task.TaskId, err)
				time.Sleep(time.Second)
			}
		case ReduceTask:
			err := doReduce(task, reducef, workerId)
			if err != nil {
				log.Printf("Reduce task %d failed: %v", task.TaskId, err)
				time.Sleep(time.Second)
			}
		case WaitTask:
			time.Sleep(time.Second)
		case ExitTask:
			log.Printf("Worker %d: All tasks completed, exiting", workerId)
			return
		}
	}
}

// 执行Map任务
func doMap(task *AssignTaskReply, mapf func(string, string) []KeyValue,
	workerId int) error {

	if len(task.InputFiles) == 0 {
		return fmt.Errorf("no input files for map task")
	}

	// 读取输入文件
	content, err := ioutil.ReadFile(task.InputFiles[0])
	if err != nil {
		return fmt.Errorf("cannot read %v: %v", task.InputFiles[0], err)
	}

	// 执行map函数
	kva := mapf(task.InputFiles[0], string(content))

	// 创建临时文件，将结果划分到不同的reduce任务
	tempFiles := make(map[int]*os.File)
	defer func() {
		// 关闭所有打开的文件
		for _, f := range tempFiles {
			f.Close()
		}
	}()

	// 将kv对写入临时文件
	for _, kv := range kva {
		// 计算key的hash值，确定它属于哪个reduce任务
		reduceTaskNum := ihash(kv.Key) % task.NReduce

		// 如果还没有为这个reduce任务创建文件，创建一个
		if _, exists := tempFiles[reduceTaskNum]; !exists {
			tmpFile, err := ioutil.TempFile("", "map-")
			if err != nil {
				return fmt.Errorf("cannot create temp file: %v", err)
			}
			tempFiles[reduceTaskNum] = tmpFile
		}

		// 写入kv对到对应的文件
		fmt.Fprintf(tempFiles[reduceTaskNum], "%v %v\n", kv.Key, kv.Value)
	}

	// 安全地重命名临时文件为最终的中间文件
	outputFiles := make([]string, 0)
	for r, tmpFile := range tempFiles {
		tmpFile.Close()
		finalName := fmt.Sprintf("mr-%d-%d", task.TaskId, r)
		os.Rename(tmpFile.Name(), finalName)
		outputFiles = append(outputFiles, finalName)
	}

	// 通知Coordinator任务完成
	return updateTask(task.TaskId, workerId, MapTask, outputFiles)
}

// 执行Reduce任务
func doReduce(task *AssignTaskReply, reducef func(string, []string) string,
	workerId int) error {

	// 创建临时输出文件
	tempName := fmt.Sprintf("mr-tmp-%d-%d", task.ReduceId, workerId)
	finalName := fmt.Sprintf("mr-out-%d", task.ReduceId)

	// 确保清理临时文件
	defer func() {
		if _, err := os.Stat(tempName); err == nil {
			os.Remove(tempName)
		}
	}()

	// 检查输入文件列表是否为空
	if len(task.InputFiles) == 0 {
		// 创建空的输出文件
		emptyFile, err := os.Create(finalName)
		if err != nil {
			return fmt.Errorf("cannot create empty output file: %v", err)
		}
		emptyFile.Close()

		// 通知Coordinator任务完成
		return updateTask(task.TaskId, workerId, ReduceTask, []string{finalName})
	}

	// 读取所有中间文件
	var kva []KeyValue
	for _, filename := range task.InputFiles {
		file, err := os.Open(filename)
		if err != nil {
			log.Printf("Warning: cannot open %v: %v", filename, err)
			continue
		}

		content, err := ioutil.ReadAll(file)
		file.Close()
		if err != nil {
			log.Printf("Warning: cannot read %v: %v", filename, err)
			continue
		}

		lines := strings.Split(string(content), "\n")
		for _, line := range lines {
			if line == "" {
				continue
			}
			parts := strings.SplitN(line, " ", 2)
			if len(parts) == 2 {
				kva = append(kva, KeyValue{Key: parts[0], Value: parts[1]})
			}
		}
	}

	// 按key排序
	sort.Sort(ByKey(kva))

	// 创建临时输出文件
	tempFile, err := os.Create(tempName)
	if err != nil {
		return fmt.Errorf("cannot create temp file: %v", err)
	}

	// 进行reduce操作
	i := 0
	for i < len(kva) {
		// 找到所有具有相同Key的值
		j := i + 1
		for j < len(kva) && kva[j].Key == kva[i].Key {
			j++
		}

		// 收集该key的所有值
		values := make([]string, 0, j-i)
		for k := i; k < j; k++ {
			values = append(values, kva[k].Value)
		}

		// 对这些值应用reduce函数
		output := reducef(kva[i].Key, values)

		// 写入结果
		fmt.Fprintf(tempFile, "%v %v\n", kva[i].Key, output)

		// 移动到下一组key
		i = j
	}

	tempFile.Close()

	// 原子重命名临时文件为最终文件
	err = os.Rename(tempName, finalName)
	if err != nil {
		return fmt.Errorf("cannot rename %v to %v: %v", tempName, finalName, err)
	}

	// 通知Coordinator任务完成
	return updateTask(task.TaskId, workerId, ReduceTask, []string{finalName})
}

// 计算哈希值
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// 从Coordinator获取任务
func getTask(workerId int) (*AssignTaskReply, error) {
	args := AssignTaskArgs{
		WorkerId: workerId,
	}
	reply := AssignTaskReply{}

	if ok := call("Coordinator.AssignTask", &args, &reply); !ok {
		return nil, fmt.Errorf("RPC call to get task failed")
	}
	return &reply, nil
}

// 向Coordinator更新任务状态
func updateTask(taskId int, workerId int, taskType TaskType,
	files []string) error {
	args := UpdateTaskArgs{
		TaskId:      taskId,
		WorkerId:    workerId,
		TaskType:    taskType,
		OutputFiles: files,
	}
	reply := UpdateTaskReply{}

	if ok := call("Coordinator.UpdateTask", &args, &reply); !ok {
		return fmt.Errorf("failed to update task status")
	}
	return nil
}

// RPC调用辅助函数
func call(rpcname string, args interface{}, reply interface{}) bool {
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		return false
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	return err == nil
}

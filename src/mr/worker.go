package mr

import (
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"math/rand"
	"net/rpc"
	"os"
	"sort"
	"strings"
	"time"
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	log.Println("mr worker is working")

	// Your worker implementation here.

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()
	// GetTask()
	mainProcess(mapf, reducef)
}

func mainProcess(mapf func(string, string) []KeyValue, reducef func(string, []string) string) {
	flag := true
	workerId := generateWorkerId()

	for flag {
		reply, err := getTask(workerId)
		if err != nil {
			log.Printf("Error getting task: %v", err)
			time.Sleep(time.Second)
			break
		}

		if reply.TaskType == MapTaskType {
			// log.Printf("do map task is working")
			// log.Printf("map task reply: %v", reply)
			doMapTask(reply, mapf, workerId)
		} else if reply.TaskType == ReduceTaskType {
			// log.Printf("do reduce task is working")
			// log.Printf("reduce task reply: %v", reply)
			doReduceTask(reply, reducef, workerId)
		} else if reply.TaskType == NoTaskType {
			log.Printf("No task available, waiting...")
			time.Sleep(time.Second)
		}
		// time.Sleep(1 * time.Second)
	}
}

func doMapTask(reply AssignTaskReply, mapf func(string, string) []KeyValue, workerId int) {
	intermediate := []KeyValue{}

	filename := reply.InputFile
	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("cannot open %v", filename)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", filename)
	}
	file.Close()

	kva := mapf(filename, string(content))
	intermediate = append(intermediate, kva...)

	// 使用 map 替代固定大小的数组
	intermediateFiles := make(map[int]string)

	for _, kv := range intermediate {
		index := ihash(kv.Key) % reply.NReduce
		ofileName := fmt.Sprintf("mr-%d-%d", reply.TaskId, index)

		if _, exists := intermediateFiles[index]; !exists {
			ofile, err := os.Create(ofileName)
			if err != nil {
				log.Fatalf("cannot create %v", ofileName)
			}
			intermediateFiles[index] = ofileName
			ofile.Close()
		}

		// 追加写入文件
		ofile, err := os.OpenFile(ofileName, os.O_WRONLY|os.O_APPEND, 0666)
		if err != nil {
			log.Fatalf("cannot open %v", ofileName)
		}
		fmt.Fprintf(ofile, "%v %v\n", kv.Key, kv.Value)
		ofile.Close()
	}

	// 将 map 转换为切片
	outputFiles := make([]string, 0, len(intermediateFiles))
	for _, fname := range intermediateFiles {
		outputFiles = append(outputFiles, fname)
	}

	args := UpdateTaskArgs{
		TaskId:      reply.TaskId,
		WorkerId:    workerId,
		TaskType:    MapTaskType,
		Done:        true,
		OutputFiles: outputFiles,
	}
	updateTaskReply := UpdateTaskReply{}
	call("Coordinator.UpdateTask", &args, &updateTaskReply)
	// log.Printf("update task reply: %v", updateTaskReply)

	if updateTaskReply.Received {
		log.Printf("map task done")
	} else {
		log.Printf("map task failed")
		// 需要删除临时文件
		for _, fileName := range outputFiles {
			os.Remove(fileName)
		}
	}
}

func doReduceTask(reply AssignTaskReply, reducef func(string, []string) string, workerId int) {
	reduceFiles := reply.ReduceFiles

	// 检查文件列表是否为空
	if len(reduceFiles) == 0 {
		// 创建空输出文件
		outFileName := fmt.Sprintf("mr-out-%d", reply.TaskId)
		os.Create(outFileName) // 创建空文件

		// 通知 Coordinator 任务完成
		args := UpdateTaskArgs{
			TaskId:   reply.TaskId,
			WorkerId: workerId,
			TaskType: ReduceTaskType,
			Done:     true,
		}
		updateTaskReply := UpdateTaskReply{}
		call("Coordinator.UpdateTask", &args, &updateTaskReply)

		if updateTaskReply.Received {
			log.Printf("reduce task done")
		} else {
			log.Printf("reduce task failed")
		}
		return
	}

	// 原有代码处理非空文件列表
	allContent := []KeyValue{}
	for _, file := range reduceFiles {
		content, err := os.ReadFile(file)
		if err != nil {
			log.Fatalf("cannot read %v", file)
		}
		lines := strings.Split(string(content), "\n")
		for _, line := range lines {
			if line == "" {
				continue
			}
			kv := strings.Split(line, " ")
			allContent = append(allContent, KeyValue{Key: kv[0], Value: kv[1]})
		}
	}

	sort.Sort(ByKey(allContent))
	// 3、开始遍历，然后进行统计
	suffixNumber := reduceFiles[0][strings.LastIndex(reduceFiles[0], "-")+1:]
	// log.Printf("suffixNumber: %v", suffixNumber)
	ofileName := fmt.Sprintf("mr-out-%s", suffixNumber)
	// log.Printf("ofileName: %v", ofileName)
	ofile, err := os.Create(ofileName)
	if err != nil {
		log.Fatalf("cannot create %v", ofileName)
	}

	i := 0
	for i < len(allContent) {
		j := i + 1
		for j < len(allContent) && allContent[j].Key == allContent[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, allContent[k].Value)
		}
		output := reducef(allContent[i].Key, values)
		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(ofile, "%v %v\n", allContent[i].Key, output)
		i = j
	}

	ofile.Close()

	// 通知 coordinator 任务完成，并传递临时文件信息
	args := UpdateTaskArgs{
		TaskId:   reply.TaskId,
		WorkerId: workerId,
		TaskType: ReduceTaskType,
		Done:     true,
	}
	updateTaskReply := UpdateTaskReply{}
	call("Coordinator.UpdateTask", &args, &updateTaskReply)
	// log.Printf("update task reply: %v", updateTaskReply)

	if updateTaskReply.Received {
		log.Printf("reduce task done")
	} else {
		log.Printf("reduce task failed")
		// 需要删除临时文件
		os.Remove(ofileName)
	}

	// log.Printf("reduce task done")
}

// 获取任务
func getTask(workerId int) (AssignTaskReply, error) {
	// log.Printf("workerId: %v", workerId)
	args := AssignTaskArgs{
		WorkerID: workerId,
	}
	// declare a reply structure.
	reply := AssignTaskReply{}
	ok := call("Coordinator.AssignTask", &args, &reply)
	if !ok {
		return AssignTaskReply{}, fmt.Errorf("RPC call failed")
	}
	return reply, nil
}

// 生成唯一的worker id
func generateWorkerId() int {
	// 使用当前时间戳和随机数生成唯一id
	rand.Seed(time.Now().UnixNano())
	// 生成一个1到100000之间的随机数
	return rand.Intn(100000) + 1
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
		log.Printf("dialing error: %v", err)
		return false
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}

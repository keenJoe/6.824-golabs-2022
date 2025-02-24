package mr

import (
	"fmt"
	"hash/fnv"
	"io"
	"log"
	"math/rand"
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
	for flag {
		reply, workerId := GetTask()
		if reply.TaskType == NoTaskType {
			break
		}

		// 如果没有可以分配的map，但是map任务还未执行完，此时需要等待
		if reply.TaskType == MapTaskType && reply.TaskId == -1 {
			time.Sleep(10 * time.Second)
			continue
		}

		if reply.TaskType == MapTaskType {
			DoMapTask(reply, mapf, workerId)
			flag = false
			log.Printf("map task done")
		} else if reply.TaskType == ReduceTaskType {
			DoReduceTask(reply)
		}
		// time.Sleep(10 * time.Second)
	}
}

func DoMapTask(reply AssignTaskReply, mapf func(string, string) []KeyValue, workerId int) {
	intermediate := []KeyValue{}

	filename := reply.InputFile
	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("cannot open %v", filename)
	}
	content, err := io.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", filename)
	}
	file.Close()

	kva := mapf(filename, string(content))
	intermediate = append(intermediate, kva...)
	// log.Printf("intermediate: %v", intermediate)

	sort.Sort(ByKey(intermediate))
	// 将中间结果写入中间文件
	intermediateFileName := make([]string, 10)
	for _, kv := range intermediate {
		// 判断数据应该写入哪个reduce任务的文件
		index := ihash(kv.Key) % reply.NReduce
		ofileName := fmt.Sprintf("mr-%d-%d-%d", reply.TaskId, index, workerId)

		//判断文件是否存在
		if _, err := os.Stat(ofileName); os.IsNotExist(err) {
			log.Printf("ofileName: %v", ofileName)
			ofile, err := os.Create(ofileName)
			if err != nil {
				log.Fatalf("cannot create %v", ofileName)
			}
			intermediateFileName[index] = ofileName
			fmt.Fprintf(ofile, "%v %v\n", kv.Key, kv.Value)
			ofile.Close()
		} else {
			ofile, err := os.OpenFile(ofileName, os.O_WRONLY|os.O_APPEND, 0666)
			if err != nil {
				log.Fatalf("cannot open %v", ofileName)
			}
			fmt.Fprintf(ofile, "%v %v\n", kv.Key, kv.Value)
			ofile.Close()
		}
	}

	// time.Sleep(10 * time.Second)

	// 通知 coordinator 任务完成，并传递临时文件信息
	args := UpdateTaskArgs{
		TaskId:      reply.TaskId,
		WorkerId:    workerId,
		TaskType:    MapTaskType,
		Done:        true,
		OutputFiles: intermediateFileName,
	}
	updateTaskReply := UpdateTaskReply{}
	call("Coordinator.UpdateTask", &args, &updateTaskReply)
	log.Printf("update task reply: %v", updateTaskReply)

	if updateTaskReply.Received {
		log.Printf("map task done")
	} else {
		log.Printf("map task failed")
		// 需要删除临时文件
		for _, fileName := range intermediateFileName {
			os.Remove(fileName)
		}
	}
}

func DoReduceTask(reply AssignTaskReply) {

}

// 获取任务
func GetTask() (AssignTaskReply, int) {
	workerId := generateWorkerId()
	log.Printf("workerId: %v", workerId)
	args := AssignTaskArgs{
		WorkerID: workerId,
	}
	// declare a reply structure.
	reply := AssignTaskReply{}
	ok := call("Coordinator.AssignTask", &args, &reply)
	if ok {
		// reply.Y should be 100.
		log.Printf("reply.Y %v\n", reply)
	} else {
		fmt.Printf("call failed!\n")
	}

	return reply, workerId
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

package mr

import "log"
import "net"
import "os"
import "net/rpc"
import "net/http"

// task status
const (
	TODO    = 0
	RUUNING = 1
	DONE    = 2
)

const (
	MAP_TASK    = "MAP"
	REDUCE_TASK = "REDUCE"
)

type Coordinator struct {
	// Your definitions here.
	files          []string
	nReduce        int
	mapTaskList    []MapTask
	reduceTaskList []ReduceTask
	allCommited    bool
}

// map task definition
type MapTask struct {
	FileName   string
	TaskStatus int
	TaskId     int
	WorkerId   string
}

// reduce task definition
type ReduceTask struct {
	fileName   string
	taskStatus int
	taskId     int
	workerId   string
}

// Your code here -- RPC handlers for the worker to call.
// worker apply for task
/**
1. worker is not sure to apply for task what type is, map or reduce
2. firstly, judging the map task
3. secondly, juding the reduce task
*/
func (c *Coordinator) ApplyForTask(args *WorkerArgs, reply *WorkerReply) error {
	println("worker is applying for task")
	//processingMap := true

	for _, task := range c.mapTaskList {
		println(task.FileName)
		reply.FileName = task.FileName
	}

	//for index, task := range c.mapTaskList {
	//	println(index)
	//	println(task.FileName)
	//	reply.TaskId = task.TaskId
	//	reply.TaskType = MAP_TASK
	//	reply.FileName = "TEST"
	//	return nil
	//
	//	//if task.TaskStatus == 0 {
	//	//
	//	//} else {
	//	//	println("can not find todo task")
	//	//}
	//}

	//if !processingMap {
	//	for _, task := range c.reduceTaskList {
	//
	//	}
	//}

	return nil
}

// when all task's status is Done, modify the c.allCommited as true
func (c *Coordinator) CommitTask(args *CommitArgs, reply *CommitReply) {

}

// an example RPC handler.
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
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
	//ret := false
	//ret := false

	// Your code here.
	// 如果所有的reduce任务均已完成，则将 ret 值修改为true
	//for _, task := range c.reduceTaskList {
	//	if task.taskStatus != 2 {
	//		ret = false
	//	}
	//}

	//return ret
	return c.allCommited
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	println("start running")
	c := Coordinator{
		files:          files,
		nReduce:        nReduce,
		mapTaskList:    createMapTasks(files),
		reduceTaskList: createReduceTasks(nReduce),
	}

	// Your code here.

	c.server()
	return &c
}

func createReduceTasks(reduce int) []ReduceTask {
	return make([]ReduceTask, reduce)
}

func createMapTasks(files []string) []MapTask {
	tasks := make([]MapTask, len(files))

	for index, fileName := range files {
		task := MapTask{}
		task.TaskStatus = TODO
		task.FileName = fileName
		task.TaskId = index
		tasks = append(tasks, task)
	}

	return tasks
}

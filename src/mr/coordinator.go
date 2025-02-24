package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"strings"
	"sync"
	"time"
)

const (
	TaskTimeout     = 10 * time.Second // worker处理任务的超时时间
	MonitorInterval = 2 * time.Second  // 监控检查的间隔时间
)

type Coordinator struct {
	// Your definitions here.
	mu          sync.Mutex
	mapTasks    []MapTask
	reduceTasks []ReduceTask
	nReduce     int   // reduce任务数量
	nMap        int   // map任务数量
	phase       Phase // 当前阶段（MAP/REDUCE）
	// done              bool       // 是否所有任务完成
	intermediateFiles [][]string // map任务产生的中间文件
}

// Your code here -- RPC handlers for the worker to call.

// 分配map任务
func (c *Coordinator) AssignTask(args *AssignTaskArgs, reply *AssignTaskReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	//如果当前任务状态是map，那么随机返回一个未开始的map任务
	//如果当前任务状态是reduce，那么随机返回一个未开始的reduce任务
	workerId := args.WorkerID
	if c.phase == MapPhase {
		for i, task := range c.mapTasks {
			// 如果任务未开始，则分配给worker
			if task.Status == Idle {
				// task.Status = InProgress
				// task.WorkerId = workerId
				// task.StartTime = time.Now()
				c.mapTasks[i].Status = InProgress
				c.mapTasks[i].WorkerId = workerId
				c.mapTasks[i].StartTime = time.Now()
				//c.mapTasks[i] = task
				reply.TaskType = MapTaskType
				reply.TaskId = task.TaskId
				reply.NReduce = c.nReduce
				reply.InputFile = task.FileName
				return nil
			}
		}

		reply.TaskType = NoTaskType
		reply.TaskId = -1
		return nil
	} else if c.phase == ReducePhase {
		// 如果当前任务状态是reduce，那么随机返回一个未开始的reduce任务
	} else if c.phase == CompletePhase {
		reply.TaskType = NoTaskType
		return nil
	}

	return nil
}

// 更新任务
func (c *Coordinator) UpdateTask(args *UpdateTaskArgs, reply *UpdateTaskReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if args.TaskType == MapTaskType &&
		c.mapTasks[args.TaskId].Status == InProgress &&
		args.WorkerId == c.mapTasks[args.TaskId].WorkerId &&
		time.Since(c.mapTasks[args.TaskId].StartTime) <= 10*time.Second {
		if args.Done {
			c.mapTasks[args.TaskId].Status = Completed
			// 将中间文件写入到reduce任务的文件中
			if args.OutputFiles != nil {
				for _, oldName := range args.OutputFiles {
					log.Printf("oldName: %v", oldName)
					newName := oldName[:strings.LastIndex(oldName, "-")]
					oldPath := "../main/" + oldName
					newPath := "../main/" + newName
					os.Rename(oldPath, newPath)
				}
			}
		}
		reply.Received = true
	} else {
		// 任务已经超时或被重新分配
		reply.Received = false
	}
	return nil
}

// 监控任务
func (c *Coordinator) MonitorTask() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.phase == MapPhase {
		// 使用索引遍历，这样可以直接修改原始数据
		for i := range c.mapTasks {
			if c.mapTasks[i].Status == InProgress {
				if time.Since(c.mapTasks[i].StartTime) > TaskTimeout {
					c.mapTasks[i].Status = Idle
					c.mapTasks[i].WorkerId = -1
					c.mapTasks[i].StartTime = time.Time{}
					log.Printf("被取消的任务：%v", c.mapTasks[i])
				}
			}
		}
	} else if c.phase == ReducePhase {
		// 这里也需要实现reduce任务的监控逻辑
	}
	return nil
}

func (c *Coordinator) DoneForWorker() bool {
	// log.Println("mr coordinator done")
	ret := false

	// Your code here.
	if c.phase == CompletePhase {
		ret = true
	}

	return ret
}

// an example RPC handler.
//
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
	// log.Println("mr coordinator done")
	ret := false

	// Your code here.
	if c.phase == CompletePhase {
		ret = true
	}

	return ret
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	log.Println("mr coordinator is making")
	c := Coordinator{}

	// Your code here.
	c.init(files, nReduce)

	// 启动后台监控任务
	go func() {
		log.Printf("监控任务启动")
		for {
			c.MonitorTask()
			time.Sleep(MonitorInterval)
		}
	}()

	c.server()
	return &c
}

// init Coordinator
func (c *Coordinator) init(files []string, nReduce int) {
	c.nReduce = nReduce
	c.nMap = len(files)
	c.phase = MapPhase
	c.mapTasks = make([]MapTask, c.nMap)
	for i, file := range files {
		c.mapTasks[i] = MapTask{
			FileName: file,
			Status:   Idle,
			TaskId:   i,
		}
	}

	c.reduceTasks = make([]ReduceTask, c.nReduce)
	for i := range c.reduceTasks {
		c.reduceTasks[i] = ReduceTask{
			Status:     Idle,
			TaskNumber: i,
		}
	}

	c.intermediateFiles = make([][]string, c.nMap)
	for i := range c.intermediateFiles {
		c.intermediateFiles[i] = make([]string, c.nReduce)
	}

	log.Println("init map tasks: ", c)
	log.Printf("mr coordinator init done")
}

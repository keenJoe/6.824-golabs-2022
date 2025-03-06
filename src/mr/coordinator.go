package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
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
	// 1. 参数验证
	if err := c.validateWorker(args.WorkerID); err != nil {
		return err
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	// 2. 根据当前阶段分配任务
	switch c.phase {
	case MapPhase:
		return c.assignMapTask(args.WorkerID, reply)
	case ReducePhase:
		return c.assignReduceTask(args.WorkerID, reply)
	case CompletePhase:
		reply.TaskType = NoTaskType
		reply.TaskId = -1
		return nil
	default:
		return fmt.Errorf("unknown phase: %v", c.phase)
	}
}

// Map 任务分配
func (c *Coordinator) assignMapTask(workerId int, reply *AssignTaskReply) error {
	// 查找空闲的 Map 任务
	for i := range c.mapTasks {
		task := &c.mapTasks[i]
		if task.Status == Idle {
			// 更新任务状态
			c.assignTaskToWorker(task, workerId)

			// 填充响应
			reply.TaskType = MapTaskType
			reply.TaskId = task.TaskId
			reply.NReduce = c.nReduce
			reply.InputFile = task.FileName

			log.Printf("Assigned map task %d to worker %d", task.TaskId, workerId)
			return nil
		}
	}

	// 检查是否所有 Map 任务都已完成
	if c.allMapTasksCompleted() {
		c.phase = ReducePhase
		log.Printf("All map tasks completed, switching to reduce phase")
	}

	reply.TaskType = NoTaskType
	reply.TaskId = -1
	return nil
}

// Reduce 任务分配
func (c *Coordinator) assignReduceTask(workerId int, reply *AssignTaskReply) error {
	// 查找空闲的 Reduce 任务
	for i := range c.reduceTasks {
		task := &c.reduceTasks[i]
		if task.Status == Idle {
			// 更新任务状态
			c.assignTaskToWorker(task, workerId)

			// 填充响应
			reply.TaskType = ReduceTaskType
			reply.TaskId = task.TaskNumber
			reply.NReduce = c.nReduce
			reply.ReduceFiles = task.InputFiles

			log.Printf("Assigned reduce task %d to worker %d", task.TaskNumber, workerId)
			return nil
		}
	}

	// 检查是否所有 Reduce 任务都已完成
	if c.allReduceTasksCompleted() {
		c.phase = CompletePhase
		log.Printf("All reduce tasks completed, switching to complete phase")
	}

	reply.TaskType = NoTaskType
	reply.TaskId = -1
	return nil
}

// 更新任务状态
func (c *Coordinator) assignTaskToWorker(task interface{}, workerId int) {
	switch t := task.(type) {
	case *MapTask:
		t.Status = InProgress
		t.WorkerId = workerId
		t.StartTime = time.Now()
	case *ReduceTask:
		t.Status = InProgress
		t.WorkerId = workerId
		t.StartTime = time.Now()
	}
}

// 检查所有 Map 任务是否完成
func (c *Coordinator) allMapTasksCompleted() bool {
	for _, task := range c.mapTasks {
		if task.Status != Completed {
			return false
		}
	}

	// 将map任务产生的中间文件收集起来，用于reduce任务的输入
	for _, task := range c.mapTasks {
		for _, filename := range task.OutputFiles {
			// 当前的文件格式是： mr-0-0
			// 需要提取出reduce编号
			var mapId, reduceId int
			fmt.Sscanf(filename, "mr-%d-%d", &mapId, &reduceId)
			c.intermediateFiles[reduceId] = append(c.intermediateFiles[reduceId], filename)
		}
	}

	c.reduceTasks = make([]ReduceTask, c.nReduce)
	for i := 0; i < c.nReduce; i++ {
		c.reduceTasks[i] = ReduceTask{
			TaskNumber: i,
			Status:     Idle,
			InputFiles: c.intermediateFiles[i],
		}
	}
	return true
}

// 检查所有 Reduce 任务是否完成
func (c *Coordinator) allReduceTasksCompleted() bool {
	for _, task := range c.reduceTasks {
		if task.Status != Completed {
			return false
		}
	}
	return true
}

// 验证 Worker ID
func (c *Coordinator) validateWorker(workerId int) error {
	if workerId < 0 {
		return fmt.Errorf("invalid worker ID: %d", workerId)
	}
	return nil
}

// 监控任务超时
func (c *Coordinator) MonitorTasks() {
	for {
		c.mu.Lock()
		now := time.Now()

		// 检查 Map 任务超时
		if c.phase == MapPhase {
			for i := range c.mapTasks {
				task := &c.mapTasks[i]
				if task.Status == InProgress && now.Sub(task.StartTime) > TaskTimeout {
					log.Printf("Map task %d timeout, resetting", task.TaskId)
					task.Status = Idle
					task.WorkerId = -1
				}
			}
		}

		// 检查 Reduce 任务超时
		if c.phase == ReducePhase {
			for i := range c.reduceTasks {
				task := &c.reduceTasks[i]
				if task.Status == InProgress && now.Sub(task.StartTime) > TaskTimeout {
					log.Printf("Reduce task %d timeout, resetting", task.TaskNumber)
					task.Status = Idle
					task.WorkerId = -1
				}
			}
		}

		c.mu.Unlock()
		time.Sleep(time.Second)
	}
}

// 更新任务完成状态
func (c *Coordinator) UpdateTask(args *UpdateTaskArgs, reply *UpdateTaskReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	switch args.TaskType {
	case MapTaskType:
		if task := c.getMapTask(args.TaskId); task != nil {
			if task.Status == InProgress && task.WorkerId == args.WorkerId {
				task.Status = Completed
				reply.Received = true
				log.Printf("Map task %d completed by worker %d", args.TaskId, args.WorkerId)

				// todo 处理输出文件，源头要加上绝对路径
				for _, tempFile := range args.OutputFiles {
					// 从临时文件名(如 "mr-0-0-1110")提取信息
					var mapId, reduceId int
					fmt.Sscanf(tempFile, "mr-%d-%d-", &mapId, &reduceId)
					// 构造最终文件名(如 "mr-0-0")
					finalName := fmt.Sprintf("mr-%d-%d", mapId, reduceId)
					// 重命名文件
					err := os.Rename(tempFile, finalName)
					if err != nil {
						log.Printf("Failed to rename file from %s to %s: %v", tempFile, finalName, err)
						continue
					}

					// 记录最终文件名
					task.OutputFiles = append(task.OutputFiles, finalName)
				}

				return nil
			}
		}
	case ReduceTaskType:
		if task := c.getReduceTask(args.TaskId); task != nil {
			if task.Status == InProgress && task.WorkerId == args.WorkerId {
				task.Status = Completed
				reply.Received = true
				log.Printf("Reduce task %d completed by worker %d", args.TaskId, args.WorkerId)
				return nil
			}
		}
	}

	reply.Received = false
	return nil
}

// 获取 Map 任务
func (c *Coordinator) getMapTask(taskId int) *MapTask {
	if taskId >= 0 && taskId < len(c.mapTasks) {
		return &c.mapTasks[taskId]
	}
	return nil
}

// 获取 Reduce 任务
func (c *Coordinator) getReduceTask(taskId int) *ReduceTask {
	if taskId >= 0 && taskId < len(c.reduceTasks) {
		return &c.reduceTasks[taskId]
	}
	return nil
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
	c.mu.Lock()
	defer c.mu.Unlock()

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
			c.MonitorTasks()
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

	// 初始化map任务
	c.mapTasks = make([]MapTask, c.nMap)
	for i, file := range files {
		c.mapTasks[i] = MapTask{
			FileName: file,
			Status:   Idle,
			TaskId:   i,
			WorkerId: -1,
		}
	}

	c.reduceTasks = make([]ReduceTask, c.nReduce)
	for i := 0; i < c.nReduce; i++ {
		c.reduceTasks[i] = ReduceTask{
			TaskNumber: i,
			Status:     Idle,
			InputFiles: make([]string, 0),
			WorkerId:   -1,
		}

		// // 预处理，收集所有map任务产生的，以i为reduce编号的中间文件
		// for mapIndex := 0; mapIndex < c.nMap; mapIndex++ {
		// 	filename := fmt.Sprintf("mr-%d-%d", mapIndex, i)
		// 	c.reduceTasks[i].InputFiles = append(c.reduceTasks[i].InputFiles, filename)
		// }
	}

	// 初始化中间文件
	// c.intermediateFiles = make([][]string, c.nMap)
	// for i := range c.intermediateFiles {
	// 	c.intermediateFiles[i] = make([]string, c.nReduce)
	// }

	// log.Println("init map tasks: ", c)
	log.Printf("mr coordinator init done")
}

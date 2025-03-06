package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

const (
	TaskTimeout     = 10 * time.Second // worker处理任务的超时时间
	MonitorInterval = 2 * time.Second  // 监控检查的间隔时间
)

type Coordinator struct {
	mu                sync.Mutex
	mapTasks          []MapTask
	reduceTasks       []ReduceTask
	nReduce           int        // reduce任务数量
	nMap              int        // map任务数量
	phase             Phase      // 当前阶段（MAP/REDUCE/COMPLETE）
	intermediateFiles [][]string // map任务产生的中间文件
}

// 分配任务
func (c *Coordinator) AssignTask(args *AssignTaskArgs, reply *AssignTaskReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	// 根据当前阶段分配任务
	switch c.phase {
	case MapPhase:
		return c.assignMapTask(args.WorkerID, reply)
	case ReducePhase:
		return c.assignReduceTask(args.WorkerID, reply)
	case CompletePhase:
		return fmt.Errorf("unknown phase: %v", c.phase)
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
			task.Status = InProgress
			task.WorkerId = workerId
			task.StartTime = time.Now()

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
		// 准备 Reduce 任务
		c.prepareReduceTasks()
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
			task.Status = InProgress
			task.WorkerId = workerId
			task.StartTime = time.Now()

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
		log.Printf("All reduce tasks completed, job is done")
		return fmt.Errorf("unknown phase: %v", c.phase)
	}

	reply.TaskType = NoTaskType
	reply.TaskId = -1
	return nil
}

// 检查所有 Map 任务是否完成
func (c *Coordinator) allMapTasksCompleted() bool {
	for _, task := range c.mapTasks {
		if task.Status != Completed {
			return false
		}
	}
	return true
}

// 准备 Reduce 任务
func (c *Coordinator) prepareReduceTasks() {
	c.intermediateFiles = make([][]string, c.nReduce)
	for i := range c.intermediateFiles {
		c.intermediateFiles[i] = make([]string, 0)
	}

	// 收集所有 Map 任务产生的中间文件
	for _, task := range c.mapTasks {
		for _, filename := range task.OutputFiles {
			// 从文件名提取 reduce 编号
			lastIndex := strings.LastIndex(filename, "-")
			reduceId, err := strconv.Atoi(filename[lastIndex+1:])
			if err != nil {
				log.Printf("Warning: Could not parse filename %s", filename)
			}
			c.intermediateFiles[reduceId] = append(c.intermediateFiles[reduceId], filename)
		}
	}

	// 创建 Reduce 任务
	c.reduceTasks = make([]ReduceTask, c.nReduce)
	for i := 0; i < c.nReduce; i++ {
		c.reduceTasks[i] = ReduceTask{
			TaskNumber: i,
			Status:     Idle,
			InputFiles: c.intermediateFiles[i],
			WorkerId:   -1,
		}
	}

	log.Printf("prepareReduceTasks: %v", c.reduceTasks)
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

// 更新任务完成状态
func (c *Coordinator) UpdateTask(args *UpdateTaskArgs, reply *UpdateTaskReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	switch args.TaskType {
	case MapTaskType:
		if args.TaskId >= 0 && args.TaskId < len(c.mapTasks) {
			task := &c.mapTasks[args.TaskId]
			if task.Status == InProgress && task.WorkerId == args.WorkerId {
				task.Status = Completed
				task.OutputFiles = args.OutputFiles

				// 将map任务临时生成的文件改成永久文件
				for i, file := range task.OutputFiles {
					lastIndex := strings.LastIndex(file, "-")
					fileName := file[:lastIndex]
					os.Rename(file, fileName)
					task.OutputFiles[i] = fileName
				}

				reply.Received = true
				return nil
			}
		}
	case ReduceTaskType:
		if args.TaskId >= 0 && args.TaskId < len(c.reduceTasks) {
			task := &c.reduceTasks[args.TaskId]
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
		time.Sleep(MonitorInterval)
	}
}

// 检查是否所有任务都已完成
func (c *Coordinator) Done() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.phase == CompletePhase
}

// 启动 RPC 服务器
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

// 创建 Coordinator
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		nReduce: nReduce,
		nMap:    len(files),
		phase:   MapPhase,
	}

	// 初始化 Map 任务
	c.mapTasks = make([]MapTask, c.nMap)
	for i, file := range files {
		c.mapTasks[i] = MapTask{
			TaskId:      i,
			FileName:    file,
			Status:      Idle,
			WorkerId:    -1,
			OutputFiles: make([]string, 0),
		}
	}

	// 启动监控任务
	go c.MonitorTasks()
	// 启动 RPC 服务器
	c.server()
	log.Printf("Coordinator initialized with %d map tasks and %d reduce tasks", c.nMap, c.nReduce)
	return &c
}

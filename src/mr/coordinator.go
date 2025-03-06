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
	for i := range c.mapTasks {
		task := &c.mapTasks[i]
		if task.Status == Idle {
			task.Status = InProgress
			task.WorkerId = workerId
			task.StartTime = time.Now()

			reply.TaskType = MapTaskType
			reply.TaskId = task.TaskId
			reply.NReduce = c.nReduce
			reply.InputFile = task.FileName

			return nil
		}
	}

	if c.allMapTasksCompleted() {
		c.phase = ReducePhase
		c.prepareReduceTasks()
	}

	reply.TaskType = NoTaskType
	reply.TaskId = -1
	return nil
}

// Reduce 任务分配
func (c *Coordinator) assignReduceTask(workerId int, reply *AssignTaskReply) error {
	for i := range c.reduceTasks {
		task := &c.reduceTasks[i]
		if task.Status == Idle {
			task.Status = InProgress
			task.WorkerId = workerId
			task.StartTime = time.Now()

			reply.TaskType = ReduceTaskType
			reply.TaskId = task.TaskNumber
			reply.NReduce = c.nReduce
			reply.ReduceFiles = task.InputFiles

			return nil
		}
	}

	if c.allReduceTasksCompleted() {
		c.phase = CompletePhase
		return nil
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
	// 初始化中间文件数组
	c.intermediateFiles = make([][]string, c.nReduce)
	for i := range c.intermediateFiles {
		c.intermediateFiles[i] = make([]string, 0)
	}

	// 收集所有 Map 任务产生的中间文件
	for _, task := range c.mapTasks {
		if task.Status == Completed {
			for _, filename := range task.OutputFiles {
				// 从文件名提取 reduce 编号 - 假设格式为 mr-M-R
				parts := strings.Split(filename, "-")
				if len(parts) >= 3 {
					reduceNum, err := strconv.Atoi(parts[2])
					if err == nil && reduceNum < c.nReduce {
						c.intermediateFiles[reduceNum] = append(c.intermediateFiles[reduceNum], filename)
					}
				}
			}
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

	// log.Printf("prepareReduceTasks: %v", c.reduceTasks)
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
				// 不需要重命名文件，保持原样
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

		if c.phase == MapPhase {
			for i := range c.mapTasks {
				task := &c.mapTasks[i]
				if task.Status == InProgress && now.Sub(task.StartTime) > TaskTimeout {
					log.Printf("Map task %d timeout, resetting", task.TaskId)
					task.Status = Idle
					task.WorkerId = -1
					// 清理可能存在的中间文件
					for _, file := range task.OutputFiles {
						os.Remove(file)
					}
					task.OutputFiles = nil
				}
			}
		}

		if c.phase == ReducePhase {
			for i := range c.reduceTasks {
				task := &c.reduceTasks[i]
				if task.Status == InProgress && now.Sub(task.StartTime) > TaskTimeout {
					log.Printf("Reduce task %d timeout, resetting", task.TaskNumber)
					task.Status = Idle
					task.WorkerId = -1
					// 尝试删除可能存在的输出文件
					outFile := fmt.Sprintf("mr-out-%d", task.TaskNumber)
					os.Remove(outFile)
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

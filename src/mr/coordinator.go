package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

const (
	TaskTimeout     = 10 * time.Second       // 恢复到更短的超时时间
	MonitorInterval = 1 * time.Second        // 更频繁的监控检查
	QueueRetryDelay = 100 * time.Millisecond // 重试间隔
	MaxQueueRetries = 3                      // 最大重试次数
)

type TaskUpdate struct {
	TaskId      int
	WorkerId    int
	TaskType    TaskType
	OutputFiles []string
	UpdateTime  time.Time
}

type Coordinator struct {
	mu            sync.Mutex
	tasks         map[int]*Task
	nReduce       int
	phase         Phase
	nextTaskId    int
	files         []string
	tasksDone     int
	updateQueue   chan TaskUpdate   // 任务更新队列
	taskUpdateMap map[int]time.Time // 记录每个任务最后一次更新时间
}

// 创建Coordinator
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := &Coordinator{
		tasks:         make(map[int]*Task),
		nReduce:       nReduce,
		phase:         MapPhase,
		nextTaskId:    0,
		files:         files,
		tasksDone:     0,
		updateQueue:   make(chan TaskUpdate, 100), // 缓冲队列
		taskUpdateMap: make(map[int]time.Time),
	}

	// 创建Map任务
	for _, file := range files {
		task := &Task{
			TaskId:     c.nextTaskId,
			TaskType:   MapTask,
			Status:     Idle,
			InputFiles: []string{file},
			WorkerId:   -1,
		}
		c.tasks[c.nextTaskId] = task
		c.nextTaskId++
	}

	// 启动任务更新处理协程
	go c.processTaskUpdates()

	c.server()
	go c.monitor()

	return c
}

// 分配任务
func (c *Coordinator) AssignTask(args *AssignTaskArgs, reply *AssignTaskReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	switch c.phase {
	case MapPhase:
		// 寻找未完成的Map任务
		for _, task := range c.tasks {
			if task.TaskType == MapTask && task.Status == Idle {
				// 设置任务状态
				task.Status = InProgress
				task.WorkerId = args.WorkerId
				task.StartTime = time.Now()

				// 设置响应
				reply.TaskType = MapTask
				reply.TaskId = task.TaskId
				reply.NReduce = c.nReduce
				reply.InputFiles = task.InputFiles

				log.Printf("Assigned Map task %d to worker %d",
					task.TaskId, args.WorkerId)
				return nil
			}
		}

		// 检查是否所有Map任务都完成
		if c.allMapTasksDone() {
			log.Printf("All Map tasks completed, transitioning to Reduce phase")
			c.transitionToReduce()
			return c.tryAssignReduceTask(args, reply)
		}

		reply.TaskType = WaitTask
		return nil

	case ReducePhase:
		return c.tryAssignReduceTask(args, reply)

	case CompletePhase:
		reply.TaskType = ExitTask
		return nil
	}

	return nil
}

// 尝试分配Reduce任务 - 新方法替代goto跳转
func (c *Coordinator) tryAssignReduceTask(args *AssignTaskArgs, reply *AssignTaskReply) error {
	// 寻找未完成的Reduce任务
	for _, task := range c.tasks {
		if task.TaskType == ReduceTask && task.Status == Idle {
			task.Status = InProgress
			task.WorkerId = args.WorkerId
			task.StartTime = time.Now()

			reply.TaskType = ReduceTask
			reply.TaskId = task.TaskId
			reply.ReduceId = task.ReduceId
			reply.InputFiles = task.InputFiles

			// 确保输入文件都存在
			allFilesExist := true
			for _, file := range task.InputFiles {
				if _, err := os.Stat(file); err != nil {
					allFilesExist = false
					break
				}
			}

			if !allFilesExist {
				// 如果有文件丢失，重置任务状态
				task.Status = Idle
				task.WorkerId = -1
				reply.TaskType = WaitTask
				return nil
			}

			return nil
		}
	}

	if c.allReduceTasksDone() {
		c.phase = CompletePhase
		reply.TaskType = ExitTask
		return nil
	}

	reply.TaskType = WaitTask
	return nil
}

// 处理任务更新的协程
func (c *Coordinator) processTaskUpdates() {
	const batchSize = 10
	updates := make([]TaskUpdate, 0, batchSize)

	for update := range c.updateQueue {
		updates = append(updates, update)

		// 尝试读取更多更新，直到达到批处理大小或队列为空
		for len(updates) < batchSize {
			select {
			case u := <-c.updateQueue:
				updates = append(updates, u)
			default:
				goto ProcessBatch
			}
		}

	ProcessBatch:
		c.mu.Lock()

		// 按时间戳排序，确保按正确顺序处理更新
		sort.Slice(updates, func(i, j int) bool {
			return updates[i].UpdateTime.Before(updates[j].UpdateTime)
		})

		// 批量处理更新
		for _, update := range updates {
			task, exists := c.tasks[update.TaskId]
			lastUpdate, hasLastUpdate := c.taskUpdateMap[update.TaskId]

			if exists && task.Status == InProgress &&
				task.WorkerId == update.WorkerId &&
				(!hasLastUpdate || update.UpdateTime.After(lastUpdate)) {

				task.Status = Completed
				task.OutputFiles = update.OutputFiles
				c.tasksDone++
				c.taskUpdateMap[update.TaskId] = update.UpdateTime
			}
		}

		// 检查阶段转换
		if c.phase == MapPhase && c.allMapTasksDone() {
			c.transitionToReduce()
		} else if c.phase == ReducePhase && c.allReduceTasksDone() {
			c.phase = CompletePhase
		}

		c.mu.Unlock()

		// 清空更新列表，准备下一批
		updates = updates[:0]
	}
}

// 更新任务状态 - 添加重试机制
func (c *Coordinator) UpdateTask(args *UpdateTaskArgs, reply *UpdateTaskReply) error {
	update := TaskUpdate{
		TaskId:      args.TaskId,
		WorkerId:    args.WorkerId,
		TaskType:    args.TaskType,
		OutputFiles: args.OutputFiles,
		UpdateTime:  time.Now(),
	}

	// 使用队列更新任务状态
	select {
	case c.updateQueue <- update:
		reply.Success = true
	default:
		// 队列满时直接更新
		c.mu.Lock()
		defer c.mu.Unlock()

		task, exists := c.tasks[args.TaskId]
		if !exists {
			reply.Success = false
			return nil
		}

		if task.Status == InProgress && task.WorkerId == args.WorkerId {
			task.Status = Completed
			task.OutputFiles = args.OutputFiles
			c.tasksDone++

			// 检查阶段转换
			if c.phase == MapPhase && c.allMapTasksDone() {
				c.transitionToReduce()
			} else if c.phase == ReducePhase && c.allReduceTasksDone() {
				c.phase = CompletePhase
			}

			reply.Success = true
		} else {
			reply.Success = false
		}
	}
	return nil
}

// 改进监控函数，考虑任务更新时间
func (c *Coordinator) monitor() {
	for {
		time.Sleep(MonitorInterval)
		c.mu.Lock()

		now := time.Now()
		for _, task := range c.tasks {
			if task.Status == InProgress {
				if now.Sub(task.StartTime) > TaskTimeout {
					log.Printf("Task %d (type %v) timed out, resetting",
						task.TaskId, taskTypeString(task.TaskType))

					// 重置任务状态
					task.Status = Idle
					task.WorkerId = -1

					// 不删除已经生成的中间文件
					if task.TaskType == ReduceTask {
						// 只删除临时的reduce输出文件
						pattern := fmt.Sprintf("mr-tmp-%d-*", task.ReduceId)
						tmpFiles, _ := filepath.Glob(pattern)
						for _, f := range tmpFiles {
							os.Remove(f)
						}
					}
				}
			}
		}

		c.mu.Unlock()
	}
}

// 清理任务相关的临时文件
func (c *Coordinator) cleanupTaskFiles(task *Task) {
	if task.TaskType == MapTask {
		pattern := fmt.Sprintf("map-*")
		tmpFiles, _ := filepath.Glob(pattern)
		for _, f := range tmpFiles {
			os.Remove(f)
		}
	} else if task.TaskType == ReduceTask {
		pattern := fmt.Sprintf("mr-tmp-%d-*", task.ReduceId)
		tmpFiles, _ := filepath.Glob(pattern)
		for _, f := range tmpFiles {
			os.Remove(f)
		}
	}
}

// 辅助函数：将任务类型转换为可读字符串
func taskTypeString(t TaskType) string {
	switch t {
	case MapTask:
		return "Map"
	case ReduceTask:
		return "Reduce"
	case WaitTask:
		return "Wait"
	case ExitTask:
		return "Exit"
	default:
		return "Unknown"
	}
}

// 改进从Map阶段转换到Reduce阶段的函数
func (c *Coordinator) transitionToReduce() {
	log.Printf("All Map tasks completed, transitioning to Reduce phase")

	// 收集中间文件
	intermediateFiles := make([][]string, c.nReduce)
	for i := range intermediateFiles {
		intermediateFiles[i] = make([]string, 0)
	}

	// 从已完成的Map任务收集输出文件
	for _, task := range c.tasks {
		if task.TaskType == MapTask && task.Status == Completed {
			for _, file := range task.OutputFiles {
				// 确保文件存在
				if _, err := os.Stat(file); err == nil {
					parts := strings.Split(file, "-")
					if len(parts) == 3 {
						if r, err := strconv.Atoi(parts[2]); err == nil && r < c.nReduce {
							intermediateFiles[r] = append(intermediateFiles[r], file)
						}
					}
				}
			}
		}
	}

	// 保存Map任务状态
	oldTasks := c.tasks
	c.tasks = make(map[int]*Task)
	for id, task := range oldTasks {
		if task.TaskType == MapTask && task.Status == Completed {
			c.tasks[id] = task
		}
	}

	// 创建Reduce任务
	c.tasksDone = 0
	for r := 0; r < c.nReduce; r++ {
		// 只有当有输入文件时才创建reduce任务
		if len(intermediateFiles[r]) > 0 {
			task := &Task{
				TaskId:     c.nextTaskId,
				TaskType:   ReduceTask,
				Status:     Idle,
				InputFiles: intermediateFiles[r],
				WorkerId:   -1,
				ReduceId:   r,
			}
			c.tasks[c.nextTaskId] = task
			c.nextTaskId++
		}
	}

	c.phase = ReducePhase
}

// 检查所有Map任务是否完成
func (c *Coordinator) allMapTasksDone() bool {
	for _, task := range c.tasks {
		if task.TaskType == MapTask && task.Status != Completed {
			return false
		}
	}
	return true
}

// 检查所有Reduce任务是否完成
func (c *Coordinator) allReduceTasksDone() bool {
	for _, task := range c.tasks {
		if task.TaskType == ReduceTask && task.Status != Completed {
			return false
		}
	}
	return true
}

// 检查整个任务是否已完成
func (c *Coordinator) Done() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.phase == CompletePhase
}

// 启动RPC服务器
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

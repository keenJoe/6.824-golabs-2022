package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"
)

const (
	TaskTimeout = 10 * time.Second
)

type Coordinator struct {
	mu         sync.Mutex
	tasks      map[int]*Task
	nReduce    int
	phase      Phase
	nextTaskId int
	files      []string
	tasksDone  int
}

// 创建Coordinator
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := &Coordinator{
		tasks:      make(map[int]*Task),
		nReduce:    nReduce,
		phase:      MapPhase,
		nextTaskId: 0,
		files:      files,
		tasksDone:  0,
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

	c.server()
	go c.monitor()

	return c
}

// 分配任务
func (c *Coordinator) AssignTask(args *AssignTaskArgs, reply *AssignTaskReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	// 根据当前阶段分配任务
	switch c.phase {
	case MapPhase:
		// 寻找未完成的Map任务
		for _, task := range c.tasks {
			if task.TaskType == MapTask && task.Status == Idle {
				task.Status = InProgress
				task.WorkerId = args.WorkerId
				task.StartTime = time.Now()

				reply.TaskType = MapTask
				reply.TaskId = task.TaskId
				reply.NReduce = c.nReduce
				reply.InputFiles = task.InputFiles
				return nil
			}
		}

		// 检查是否可以进入Reduce阶段
		if c.allMapTasksDone() {
			c.transitionToReduce()
			// 直接尝试分配Reduce任务
			return c.tryAssignReduceTask(args, reply)
		}

		// 没有可用任务，但Map阶段未结束
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
			return nil
		}
	}

	// 检查是否所有Reduce任务已完成
	if c.allReduceTasksDone() {
		c.phase = CompletePhase
		reply.TaskType = ExitTask
		return nil
	}

	// 没有可用任务，但Reduce阶段未结束
	reply.TaskType = WaitTask
	return nil
}

// 更新任务状态
func (c *Coordinator) UpdateTask(args *UpdateTaskArgs, reply *UpdateTaskReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	task, exists := c.tasks[args.TaskId]
	if exists && task.Status == InProgress && task.WorkerId == args.WorkerId {
		task.Status = Completed
		task.OutputFiles = args.OutputFiles
		c.tasksDone++
		reply.Success = true
	} else {
		reply.Success = false
	}

	return nil
}

// 监控任务状态
func (c *Coordinator) monitor() {
	for {
		time.Sleep(500 * time.Millisecond)
		c.mu.Lock()

		now := time.Now()
		anyTaskReset := false

		for _, task := range c.tasks {
			if task.Status == InProgress {
				if now.Sub(task.StartTime) > TaskTimeout {
					// 任务超时处理
					log.Printf("Task %d (type %v) timed out, resetting", task.TaskId, taskTypeString(task.TaskType))
					task.Status = Idle
					task.WorkerId = -1
					anyTaskReset = true

					// 只删除临时文件，保留中间文件供其他worker使用
					if task.TaskType == MapTask {
						// 临时文件清理，但保留已完成的中间文件
						pattern := fmt.Sprintf("map-*")
						tmpFiles, _ := filepath.Glob(pattern)
						for _, f := range tmpFiles {
							os.Remove(f)
						}
					} else if task.TaskType == ReduceTask {
						// 查找和清理临时文件
						pattern := fmt.Sprintf("mr-tmp-%d-*", task.ReduceId)
						tmpFiles, _ := filepath.Glob(pattern)
						for _, f := range tmpFiles {
							os.Remove(f)
						}
					}
				}
			}
		}

		// 仅在状态变化时才检查阶段转换
		if anyTaskReset || c.tasksDone > 0 {
			if c.phase == MapPhase && c.allMapTasksDone() {
				c.transitionToReduce()
			} else if c.phase == ReducePhase && c.allReduceTasksDone() {
				c.phase = CompletePhase
			}
		}

		c.mu.Unlock()
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

	// 收集中间文件并创建Reduce任务
	intermediateFiles := make([][]string, c.nReduce)
	for i := range intermediateFiles {
		intermediateFiles[i] = make([]string, 0)
	}

	// 从已完成的Map任务收集输出文件
	for _, task := range c.tasks {
		if task.TaskType == MapTask && task.Status == Completed {
			for _, file := range task.OutputFiles {
				// 从文件名直接提取reduce编号 - 更可靠的方式
				parts := strings.Split(file, "-")
				if len(parts) == 3 {
					if r, err := strconv.Atoi(parts[2]); err == nil && r < c.nReduce {
						intermediateFiles[r] = append(intermediateFiles[r], file)
					}
				}
			}
		}
	}

	// 创建新的任务映射 - 保留已完成的Map任务以供记录
	oldTasks := c.tasks
	c.tasks = make(map[int]*Task)

	// 保留已完成的Map任务
	for id, task := range oldTasks {
		if task.TaskType == MapTask {
			c.tasks[id] = task
		}
	}

	// 添加Reduce任务
	c.tasksDone = 0 // 重置完成任务计数
	for r := 0; r < c.nReduce; r++ {
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

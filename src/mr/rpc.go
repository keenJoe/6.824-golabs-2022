package mr

import (
	"os"
	"strconv"
	"time"
)

// 任务类型
type TaskType int

const (
	MapTask TaskType = iota
	ReduceTask
	WaitTask
	ExitTask
)

// 任务状态
type TaskStatus int

const (
	Idle TaskStatus = iota
	InProgress
	Completed
)

// 系统阶段
type Phase int

const (
	MapPhase Phase = iota
	ReducePhase
	CompletePhase
)

// 任务定义
type Task struct {
	TaskId      int
	TaskType    TaskType
	Status      TaskStatus
	InputFiles  []string // Map任务用第一个，Reduce任务用全部
	OutputFiles []string
	WorkerId    int
	StartTime   time.Time
	ReduceId    int // 仅Reduce任务使用
}

// RPC请求和响应结构
type AssignTaskArgs struct {
	WorkerId int
}

type AssignTaskReply struct {
	TaskType   TaskType
	TaskId     int
	NReduce    int
	InputFiles []string
	ReduceId   int
}

type UpdateTaskArgs struct {
	TaskId      int
	WorkerId    int
	TaskType    TaskType
	OutputFiles []string
}

type UpdateTaskReply struct {
	Success bool
}

// RPC辅助函数
func coordinatorSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}

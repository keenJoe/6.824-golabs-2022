package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
	"strconv"
	"time"
)

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

// Add your RPC definitions here.

// 任务类型
type TaskType int

const (
	MapTaskType    TaskType = 0
	ReduceTaskType TaskType = 1
	NoTaskType     TaskType = 2
)

// 任务状态
type TaskStatus int

const (
	Idle       TaskStatus = 0
	InProgress TaskStatus = 1
	Completed  TaskStatus = 2
)

// 系统阶段
type Phase int

const (
	MapPhase      Phase = 0
	ReducePhase   Phase = 1
	CompletePhase Phase = 2
)

// Map任务
type MapTask struct {
	TaskId      int        // 任务ID
	FileName    string     // 输入文件名
	Status      TaskStatus // 任务状态
	WorkerId    int        // 分配给的Worker ID
	StartTime   time.Time  // 开始时间
	OutputFiles []string   // 输出文件列表
}

// Reduce任务
type ReduceTask struct {
	TaskNumber int        // Reduce任务编号
	Status     TaskStatus // 任务状态
	InputFiles []string   // 输入文件列表
	WorkerId   int        // 分配给的Worker ID
	StartTime  time.Time  // 开始时间
}

// 请求任务参数
type AssignTaskArgs struct {
	WorkerID int // Worker ID
}

// 请求任务响应
type AssignTaskReply struct {
	TaskType    TaskType // 任务类型
	TaskId      int      // 任务ID
	NReduce     int      // Reduce任务数量
	InputFile   string   // Map任务输入文件
	ReduceFiles []string // Reduce任务输入文件列表
}

// 更新任务状态参数
type UpdateTaskArgs struct {
	TaskId      int      // 任务ID
	WorkerId    int      // Worker ID
	TaskType    TaskType // 任务类型
	Done        bool     // 是否完成
	OutputFiles []string // 输出文件列表
}

// 更新任务状态响应
type UpdateTaskReply struct {
	Received bool // 是否接收成功
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/5840-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}

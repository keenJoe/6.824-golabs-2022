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

type TaskStatus int

const (
	Idle TaskStatus = iota
	InProgress
	Completed
)

type Phase int

const (
	MapPhase Phase = iota
	ReducePhase
	CompletePhase
)

type MapTask struct {
	FileName  string
	Status    TaskStatus
	WorkerId  int
	StartTime time.Time
	TaskId    int // task的编号，用于生成中间文件名
}

type ReduceTask struct {
	Status     TaskStatus
	WorkerId   int
	StartTime  time.Time
	TaskNumber int // task的编号，用于生成中间文件名
}

type TaskType int

const (
	MapTaskType TaskType = iota
	ReduceTaskType
	NoTaskType
)

type AssignTaskArgs struct {
	WorkerID int
}

type AssignTaskReply struct {
	TaskType    TaskType
	TaskId      int
	InputFile   string   // Map任务的输入文件
	ReduceFiles []string // Reduce任务的输入文件
	NReduce     int      // Reduce任务数量
}

// Worker完成任务时发送的参数
type UpdateTaskArgs struct {
	TaskType    TaskType
	TaskId      int
	WorkerId    int
	Done        bool
	OutputFiles []string // 输出文件的位置
}

// Coordinator响应Worker完成报告的结构体
type UpdateTaskReply struct {
	Received bool
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}

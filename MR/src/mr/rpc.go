package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
	"strconv"
)

type NRemainReduceReply struct {
	NReduce int
}

type GetAJobArgs struct {
	WorkerID int
}

type GetAJobReply struct {
	WorkID   int
	WorkType int
	File     string
}

type ReportArgs struct {
	WorkerID int
	TaskType int
	WorkID   int
}

type ReportReply struct {
	Kill bool
}

////
//// example to show how to declare the arguments
//// and reply for an RPC.
////

//type ExampleArgs struct {
//X int
//}

//type ExampleReply struct {
//Y int
//}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}

package mr

import (
	"io"
	"log"
	"os"
	"time"
)

type TaskType int

const (
	Exit = iota
	Wait
	Map
	Reduce
)

type Status int

const (
	Assigned = iota
	Finsished
	Unassigned
)

type Task struct {
	TaskType TaskType // init when lanching
	// task id
	TaskID int    // init
	Status Status // init &update when assigning

	WorkerId  int       //  update when assigning
	StartTime time.Time // update when assigning

	// control information
	InputFiles []string
}

const MaximumTaskTime = 5 * time.Second

const SleepTime = 3 * time.Second

const MaximumRetry = 3

var debugLogger *log.Logger

func LogInit() {
	if true {
		debugLogger = log.New(os.Stdout, "DEBUG: ", log.Ldate|log.Ltime|log.Lshortfile)
	} else {
		// Disable logging by setting output to ioutil.Discard
		debugLogger = log.New(io.Discard, "", 0)
	}
}

package mapreduce

import "time"

type (
	MapTask struct {
		ID            string
		InputFile     string
		Status        TaskStatus
		CreatedAt     time.Time
		InProgressAt  *time.Time
		FailedAt      *time.Time
		DoneAt        *time.Time
		RescheduledAt *time.Time
	}

	MapTaskResult struct {
		TaskID      string
		OutputFiles []string
		Error       error
	}

	ReduceTask struct {
		ID            string
		InputFiles    []string
		Status        TaskStatus
		CreatedAt     time.Time
		InProgressAt  *time.Time
		FailedAt      *time.Time
		DoneAt        *time.Time
		RescheduledAt *time.Time
	}

	ReduceTaskResult struct {
		TaskID     string
		OutputFile string
		Error      error
	}

	TaskStatus int
)

const (
	TaskCreated TaskStatus = iota
	TaskInProgress
	TaskFailed
	TaskDone
	TaskRescheduled
)

func (ts TaskStatus) String() string {
	return [...]string{
		"TaskCreated",
		"TaskInProgress",
		"TaskFailed",
		"TaskDone",
		"TaskRescheduled",
	}[ts]
}

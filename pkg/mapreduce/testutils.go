package mapreduce

import (
	"fmt"
	"time"
)

type (
	MapTaskMatcher struct {
		MapTask
	}

	ReduceTaskMatcher struct {
		ReduceTask
	}

	MapTaskResultMatcher struct {
		MapTaskResult
	}

	ReduceTaskResultMatcher struct {
		ReduceTaskResult
	}
)

func (m MapTaskMatcher) Matches(x interface{}) bool {
	other, ok := x.(MapTask)
	if !ok {
		return false
	}

	if m.ID != other.ID {
		return false
	}
	if m.Status != other.Status {
		return false
	}
	if m.InputFile != other.InputFile {
		return false
	}
	if m.CreatedAt.Sub(other.CreatedAt) > time.Millisecond {
		return false
	}
	if m.InProgressAt == nil && other.InProgressAt != nil {
		return false
	} else if m.InProgressAt != nil && other.InProgressAt == nil {
		return false
	} else if m.InProgressAt != nil && other.InProgressAt != nil {
		if m.InProgressAt.Sub(*other.InProgressAt) > time.Millisecond {
			return false
		}
	}
	if m.RescheduledAt == nil && other.RescheduledAt != nil {
		return false
	} else if m.RescheduledAt != nil && other.RescheduledAt == nil {
		return false
	} else if m.RescheduledAt != nil && other.RescheduledAt != nil {
		if m.RescheduledAt.Sub(*other.RescheduledAt) > time.Millisecond {
			return false
		}
	}
	if m.FailedAt == nil && other.FailedAt != nil {
		return false
	} else if m.FailedAt != nil && other.FailedAt == nil {
		return false
	} else if m.FailedAt != nil && other.FailedAt != nil {
		if m.FailedAt.Sub(*other.FailedAt) > time.Millisecond {
			return false
		}
	}
	if m.DoneAt == nil && other.DoneAt != nil {
		return false
	} else if m.DoneAt != nil && other.DoneAt == nil {
		return false
	} else if m.DoneAt != nil && other.DoneAt != nil {
		if m.DoneAt.Sub(*other.DoneAt) > time.Millisecond {
			return false
		}
	}

	return true
}

func (m MapTaskMatcher) String() string {
	return fmt.Sprintf("MapTask{ID=%s, InputFile=%s, Status=%s}", m.ID, m.InputFile, m.Status)
}

func (m ReduceTaskMatcher) Matches(x interface{}) bool {
	other, ok := x.(ReduceTask)
	if !ok {
		return false
	}

	if m.ID != other.ID {
		return false
	}
	if m.Status != other.Status {
		return false
	}
	if len(m.InputFiles) != len(other.InputFiles) {
		return false
	}
	for i, f := range m.InputFiles {
		if f != other.InputFiles[i] {
			return false
		}
	}
	if m.CreatedAt.Sub(other.CreatedAt) > time.Millisecond {
		return false
	}
	if m.InProgressAt == nil && other.InProgressAt != nil {
		return false
	} else if m.InProgressAt != nil && other.InProgressAt == nil {
		return false
	} else if m.InProgressAt != nil && other.InProgressAt != nil {
		if m.InProgressAt.Sub(*other.InProgressAt) > time.Millisecond {
			return false
		}
	}
	if m.RescheduledAt == nil && other.RescheduledAt != nil {
		return false
	} else if m.RescheduledAt != nil && other.RescheduledAt == nil {
		return false
	} else if m.RescheduledAt != nil && other.RescheduledAt != nil {
		if m.RescheduledAt.Sub(*other.RescheduledAt) > time.Millisecond {
			return false
		}
	}
	if m.FailedAt == nil && other.FailedAt != nil {
		return false
	} else if m.FailedAt != nil && other.FailedAt == nil {
		return false
	} else if m.FailedAt != nil && other.FailedAt != nil {
		if m.FailedAt.Sub(*other.FailedAt) > time.Millisecond {
			return false
		}
	}
	if m.DoneAt == nil && other.DoneAt != nil {
		return false
	} else if m.DoneAt != nil && other.DoneAt == nil {
		return false
	} else if m.DoneAt != nil && other.DoneAt != nil {
		if m.DoneAt.Sub(*other.DoneAt) > time.Millisecond {
			return false
		}
	}

	return true
}

func (m ReduceTaskMatcher) String() string {
	return fmt.Sprintf("ReduceTask{ID=%s, InputFiles=%s, Status=%s}", m.ID, m.InputFiles, m.Status)
}

func (m MapTaskResultMatcher) Matches(x interface{}) bool {
	other, ok := x.(MapTaskResult)
	if !ok {
		return false
	}

	if m.TaskID != other.TaskID {
		return false
	}
	if len(m.OutputFiles) != len(other.OutputFiles) {
		return false
	}
	for i, f := range m.OutputFiles {
		if f != other.OutputFiles[i] {
			return false
		}
	}
	if m.Error == nil && other.Error != nil {
		return false
	} else if m.Error != nil && other.Error == nil {
		return false
	} else if m.Error != nil && other.Error != nil {
		if m.Error.Error() != other.Error.Error() {
			return false
		}
	}

	return true
}

func (m MapTaskResultMatcher) String() string {
	return fmt.Sprintf("MapTaskResult{TaskID=%s, OutputFiles=%s, Error=%s}", m.TaskID, m.OutputFiles, m.Error)
}

func (m ReduceTaskResultMatcher) Matches(x interface{}) bool {
	other, ok := x.(ReduceTaskResult)
	if !ok {
		return false
	}

	if m.TaskID != other.TaskID {
		return false
	}
	if m.OutputFile != other.OutputFile {
		return false
	}
	if m.Error == nil && other.Error != nil {
		return false
	} else if m.Error != nil && other.Error == nil {
		return false
	} else if m.Error != nil && other.Error != nil {
		if m.Error.Error() != other.Error.Error() {
			return false
		}
	}

	return true
}

func (m ReduceTaskResultMatcher) String() string {
	return fmt.Sprintf("ReduceTaskResult{TaskID=%s, OutputFile=%s, Error=%s}", m.TaskID, m.OutputFile, m.Error)
}

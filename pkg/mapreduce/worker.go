package mapreduce

type (
	MapWorker interface {
		ProcessMapTasks() error
		ProcessMapTask(task MapTask) error
	}
	ReduceWorker interface {
		ProcessReduceTasks() error
		ProcessReduceTask(task ReduceTask) error
	}
)

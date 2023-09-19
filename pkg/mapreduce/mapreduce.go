package mapreduce

type (
	KeyValue struct {
		Key   string
		Value any
	}

	Map    func(key string, value any) []KeyValue
	Reduce func(key string, values ...any) []any
)

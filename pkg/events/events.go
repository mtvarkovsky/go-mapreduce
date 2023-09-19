package events

type (
	Event struct {
		Type EventType         `json:"type"`
		Data map[string]string `json:"data"`
	}

	EventType int
)

const (
	MapTaskCreated EventType = iota
	ReduceTaskCreated
)

func (et EventType) String() string {
	return [...]string{
		"MapTaskCreated",
		"ReduceTaskCreated",
	}[et]
}

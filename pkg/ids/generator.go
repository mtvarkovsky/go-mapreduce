package ids

type (
	EntityType int
)

const (
	MapTask EntityType = iota
	ReduceTask
)

func (et EntityType) String() string {
	return [...]string{
		"MapTask",
		"ReduceTask",
	}[et]
}

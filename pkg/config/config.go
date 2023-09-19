package config

type Config[C any] interface {
	LoadConfig(name string) (C, error)
}

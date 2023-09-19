package logger

type (
	Logger interface {
		Infof(message string, args ...any)
		Errorf(message string, args ...any)
		Warnf(message string, args ...any)
		Fatalf(message string, args ...any)

		Logger(name string) Logger
	}
)

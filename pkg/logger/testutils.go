package logger

import "go.uber.org/zap"

type (
	TestZapLogger struct {
		*zapLogger
	}
)

func NewTestZapLogger() Logger {
	log := NewZapLogger("test")
	l := log.(*zapLogger)
	l.log = zap.NewNop()
	return &TestZapLogger{
		zapLogger: l,
	}
}

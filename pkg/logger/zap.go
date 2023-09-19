package logger

import (
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

type (
	zapLogger struct {
		log *zap.Logger
	}
)

func NewZapLogger(name string) Logger {
	encoderCfg := zap.NewProductionEncoderConfig()
	encoderCfg.TimeKey = "timestamp"
	encoderCfg.EncodeTime = zapcore.RFC3339TimeEncoder

	zapCfg := zap.NewProductionConfig()
	zapCfg.EncoderConfig = encoderCfg

	logger := zap.Must(zapCfg.Build())
	return &zapLogger{
		log: logger.Named(name),
	}
}

func (l *zapLogger) Infof(message string, args ...any) {
	l.log.Sugar().Infof(message, args...)
}

func (l *zapLogger) Errorf(message string, args ...any) {
	l.log.Sugar().Errorf(message, args...)
}

func (l *zapLogger) Warnf(message string, args ...any) {
	l.log.Sugar().Warnf(message, args...)
}

func (l *zapLogger) Fatalf(message string, args ...any) {
	l.log.Sugar().Fatalf(message, args...)
}

func (l *zapLogger) Logger(name string) Logger {
	return &zapLogger{
		log: l.log.Named(name),
	}
}

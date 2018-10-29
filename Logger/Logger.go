package Logger

import (
	"engine-socket/Config"
	"fmt"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	lumberjack "gopkg.in/natefinch/lumberjack.v2"
)

var logger *zap.Logger

func init() {
	options := Config.GetOptions()

	config := zapcore.AddSync(&lumberjack.Logger{
		Filename:   fmt.Sprintf("%s/engine-go", options.LogsPath),
		MaxSize:    500, // megabytes
		MaxBackups: 3,
		MaxAge:     30, // days
	})

	encoderCfg := zap.NewProductionEncoderConfig()
	encoderCfg.TimeKey = "time"
	encoderCfg.EncodeTime = zapcore.ISO8601TimeEncoder
	encoderCfg.EncodeDuration = zapcore.StringDurationEncoder

	core := zapcore.NewCore(
		zapcore.NewJSONEncoder(encoderCfg),
		config,
		zap.InfoLevel,
	)

	logger = zap.New(core)
}

func GetLogger() *zap.Logger {
	return logger
}

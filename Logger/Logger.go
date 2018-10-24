package Logger

import (
	"engine-socket/Config"
	"fmt"

	"go.uber.org/zap"
)

var logger *zap.Logger

func init() {
	var (
		err error
		cfg zap.Config
	)

	options := Config.GetOptions()

	if options.Production {
		cfg = zap.NewProductionConfig()
	} else {
		cfg = zap.NewDevelopmentConfig()
	}

	cfg.OutputPaths[0] = fmt.Sprintf("%s/engine-logs", options.LogsPath)
	cfg.ErrorOutputPaths[0] = fmt.Sprintf("%s/engine-errors", options.LogsPath)

	if logger, err = cfg.Build(); err != nil {
		panic(err)
	}
}

func GetLogger() *zap.Logger {
	return logger
}

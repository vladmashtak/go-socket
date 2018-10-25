package Clickhouse

import (
	"database/sql"
	"engine-socket/Logger"
	"fmt"

	"github.com/kshvakov/clickhouse"
	"go.uber.org/zap"
)

func Connect() (*sql.DB, error) {
	logger := Logger.GetLogger()

	connect, err := sql.Open("clickhouse", "tcp://127.0.0.1:9001?database=aqosta")

	if err != nil {
		return nil, err
	}

	if err := connect.Ping(); err != nil {
		if exception, ok := err.(*clickhouse.Exception); ok {
			logger.Info(fmt.Sprintf("[%d] %s \n%s\n", exception.Code, exception.Message, exception.StackTrace))
		} else {
			logger.Info("Clickhouse", zap.Error(err))
		}
		return nil, err
	}

	return connect, nil
}

func PrepareStatement(tx *sql.Tx, stmt string) (*sql.Stmt, error) {
	logger := Logger.GetLogger()

	if stmt, err := tx.Prepare(stmt); err != nil {
		logger.Info("Error while preparing statement", zap.Error(err))
		return nil, err
	} else {
		return stmt, nil
	}
}
 
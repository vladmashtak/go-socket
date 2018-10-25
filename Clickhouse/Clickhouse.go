package Clickhouse

import (
	"database/sql"
	"fmt"
	"log"

	"github.com/kshvakov/clickhouse"
)

func Connect() (*sql.DB, error) {
	connect, err := sql.Open("clickhouse", "tcp://127.0.0.1:9001?database=aqosta")

	if err != nil {
		return nil, err
	}

	if err := connect.Ping(); err != nil {
		if exception, ok := err.(*clickhouse.Exception); ok {
			log.Println(fmt.Sprintf("[%d] %s \n%s\n", exception.Code, exception.Message, exception.StackTrace))
		} else {
			log.Println("Clickhouse", err)
		}
		return nil, err
	}

	return connect, nil
}

func PrepareStatement(tx *sql.Tx, stmt string) (*sql.Stmt, error) {

	if stmt, err := tx.Prepare(stmt); err != nil {
		log.Println("Error while preparing statement", err)
		return nil, err
	} else {
		return stmt, nil
	}
}

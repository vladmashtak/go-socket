package Clickhouse

import (
	"database/sql"
	"fmt"
	"log"

	"github.com/kshvakov/clickhouse"
)

func Connect() (*sql.DB, error)  {
	connect, err := sql.Open("clickhouse", "tcp://127.0.0.1:9000")

	if err != nil {
		log.Fatal(err)
	}

	if err := connect.Ping(); err != nil {
		if exception, ok := err.(*clickhouse.Exception); ok {
			fmt.Printf("[%d] %s \n%s\n", exception.Code, exception.Message, exception.StackTrace)
		} else {
			fmt.Println(err)
		}
		return nil, err
	}

	return connect, nil
}


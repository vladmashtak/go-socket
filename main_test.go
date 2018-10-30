package main

import (
	"io/ioutil"
	"log"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func Test_handleConnection(t *testing.T) {
	type args struct {
		conn *net.TCPConn
	}

	tests := []struct {
		name string
		args args
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			handleConnection(tt.args.conn)
		})
	}
}

func BenchmarkMain(b *testing.B) {
	var (
		err   error
		files []string = make([]string, 4)
		dir   string   = "/home/vlad/work/mocks"
		i     uint32
		in    []byte
	)

	err = filepath.Walk(dir, func(path string, info os.FileInfo, err error) error {

		if info.IsDir() {
			return nil
		}

		files[i] = path
		i++

		return err
	})

	for _, path := range files {
		go func(path string) {
			in, err = ioutil.ReadFile(path)

			insertMessage(in)
		}(path)

		time.Sleep(time.Second)
	}

	if err != nil {
		log.Fatal(err)
	}
}

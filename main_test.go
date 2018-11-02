package main

import (
	"bytes"
	"io"
	"io/ioutil"
	"log"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/klauspost/compress/zlib"
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
		err    error
		reader io.ReadCloser
		buffer []byte
		files  []string = make([]string, 4)
		dir    string   = "/home/vlad/work/mocks"
		i      uint32
		in     []byte
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
			buffer, err = ioutil.ReadFile(path)

			reader, err = zlib.NewReader(bytes.NewReader(buffer[8:]))

			in, err = ioutil.ReadAll(reader)

			insertMessage(in)
		}(path)

		time.Sleep(time.Second)
	}

	if err != nil {
		log.Fatal(err)
	}
}

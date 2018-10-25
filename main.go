package main

import (
	"bufio"
	"engine-socket/Aggregator"
	"engine-socket/Config"
	"engine-socket/Deserializer"
	"engine-socket/Logger"
	"engine-socket/PacketReader"
	"fmt"
	"io/ioutil"
	"net"
	"strings"
	"time"

	"go.uber.org/zap"
)

func main() {
	options := Config.GetOptions()
	logger := Logger.GetLogger()

	server := fmt.Sprintf("%s:%d", options.Host, options.Port)

	ln, err := net.Listen("tcp", server)

	if err != nil {
		logger.Info("Can't create tcp server", zap.Error(err))
	}

	logger.Info("Server start work: " + server)

	defer ln.Close()

	for {
		conn, err := ln.Accept()
		if err != nil {
			logger.Info("Can't create tcp connection", zap.Error(err))
			continue
		}
		go handleConnection(conn)
	}
}

func handleConnection(conn net.Conn) {
	defer conn.Close()

	logger := Logger.GetLogger()
	reader := bufio.NewReader(conn)

	in, err := ioutil.ReadAll(reader)

	if err != nil {
		logger.Info("Can't read input", zap.Error(err))
	}

	packet := PacketReader.NewPacketReader(in)

	message := Deserializer.NewMessage()

	message.Read(packet)

	instance := packet.ReadString()
	logger.Info("Read string", zap.String("instance name", instance))

	portId := packet.ReadString()
	// log.Printf("Read portId: %s", portId)

	size := packet.ReadInt()
	// log.Printf("SZ: %v", size)

	startTime := time.Now()

	var i uint32 = 0

	aggregator := Aggregator.NewAggregator()

	for i < size {
		mapValue := make(map[string]interface{})

		message.ReadObject(packet, mapValue)
		if len(mapValue) != 0 {

			caption := strings.ToLower(message.GetCaption())

			switch caption {
			case "protos":
				aggregator.AddNetIfaceBatch(portId, instance, mapValue)
			case "dns":
				aggregator.AddDnsBatch(portId, instance, mapValue)
			default:
				aggregator.AddNetSessionBatch(portId, instance, mapValue, caption)
			}
		}

		i++
	}

	go aggregator.Execute()

	logger.Info("Clickhouse: ", zap.Duration("sql query: ", time.Now().Sub(startTime)))

	// logger.Sync()
}

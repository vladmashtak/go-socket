package main

import (
	"bufio"
	"engine-socket/Aggregator"
	"engine-socket/Config"
	"engine-socket/Deserializer"
	"engine-socket/PacketReader"
	"fmt"
	"io/ioutil"
	"log"
	"net"
	"strings"
	"time"
)

func main() {
	options := Config.GetOptions()

	server := fmt.Sprintf("%s:%d", options.Host, options.Port)

	ln, err := net.Listen("tcp", server)

	if err != nil {
		log.Println("Can't create tcp server", err)
	}

	log.Println("Server start work: " + server)

	defer ln.Close()

	for {
		conn, err := ln.Accept()
		if err != nil {
			log.Println("Can't create tcp connection", err)
			continue
		}
		go handleConnection(conn)
	}
}

func handleConnection(conn net.Conn) {
	defer conn.Close()

	reader := bufio.NewReader(conn)

	in, err := ioutil.ReadAll(reader)

	if err != nil {
		log.Println("Can't read input", err)
	}

	packet := PacketReader.NewPacketReader(in)

	message := Deserializer.NewMessage()

	message.Read(packet)

	instance := packet.ReadString()
	log.Println("Read instance name", instance)

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

	aggregator.Execute()

	log.Println("Clickhouse sql query: ", time.Now().Sub(startTime))
}

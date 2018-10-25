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
		return
	}

	log.Println("Server start work" + server)

	defer ln.Close()

	for {
		conn, err := ln.Accept()
		if err != nil {
			log.Println("Can't create tcp connection", err)
			continue
		}

		conn.SetWriteDeadline(time.Now().Add(5 * time.Second))

		go handleConnection(conn)
	}
}

func handleConnection(conn net.Conn) {
	var (
		i      uint32 = 0
		err    error
		in     []byte
		reader *bufio.Reader
		packet *PacketReader.PacketReader
	)

	defer conn.Close()

	log.Println("Accept connection from", conn.RemoteAddr())

	reader = bufio.NewReader(conn)

	in, err = ioutil.ReadAll(reader)

	log.Println("Message length", len(in))

	packet, err = PacketReader.NewPacketReader(in)

	if err != nil {
		log.Println("Read input", err)
		return
	}

	message := Deserializer.NewMessage()

	message.Read(packet)

	instance := packet.ReadString()
	log.Println("Read instance name", instance)

	portId := packet.ReadString()
	// log.Printf("Read portId: %s", portId)

	size := packet.ReadInt()
	// log.Printf("SZ: %v", size)

	startTime := time.Now()

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

	log.Println("Clickhouse aggregate batch", time.Now().Sub(startTime))

	go aggregator.Execute()
}

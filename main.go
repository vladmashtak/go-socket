package main

import (
	"bufio"
	"engine-socket/Aggregator"
	"engine-socket/Deserializer"
	"engine-socket/PacketReader"
	"io/ioutil"
	"log"
	"net"
	"strings"
	"time"
)

func main() {
	log.Println("Engine server")

	ln, err := net.Listen("tcp", ":5000")

	if err != nil {
		log.Fatal(err)
	}

	defer ln.Close()

	for {
		conn, err := ln.Accept()
		if err != nil {
			log.Fatal(err)
			return
		}
		go handleConnection(conn)
	}
}

func handleConnection(conn net.Conn) {
	defer conn.Close()

	reader := bufio.NewReader(conn)

	in, err := ioutil.ReadAll(reader)

	if err != nil {
		log.Fatal(err)
	}

	packet := PacketReader.NewPacketReader(in)

	message := Deserializer.NewMessage()

	message.Read(packet)

	instance := packet.ReadString()
	log.Printf("Read instance: %s", instance)

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

	log.Println("Clickhouse time: ", time.Now().Sub(startTime))
}

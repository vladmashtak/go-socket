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
	var (
		err     error
		server  string
		tcpAddr *net.TCPAddr
		ln      *net.TCPListener
		options *Config.Options
	)

	options = Config.GetOptions()

	server = fmt.Sprintf("%s:%d", options.Host, options.Port)

	tcpAddr, err = net.ResolveTCPAddr("tcp4", server)

	ln, err = net.ListenTCP("tcp", tcpAddr)

	if err != nil {
		log.Println("Can't create tcp4 server ", err)
		return
	}

	log.Println("Server start work " + server)

	defer ln.Close()

	for {
		conn, err := ln.AcceptTCP()
		if err != nil {
			log.Println("Can't create connection ", err)
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

	log.Println("Accept connection from ", conn.RemoteAddr())

	reader = bufio.NewReader(conn)

	in, err = ioutil.ReadAll(reader)

	packet, err = PacketReader.NewPacketReader(in)

	if err != nil {
		log.Println("Read input", err)
		return
	}

	message := Deserializer.NewMessage()

	message.Read(packet)

	instance := packet.ReadString()
	log.Println("Read instance name ", instance)

	portId := packet.ReadString()
	// log.Printf("Read portId: %s", portId)

	size := packet.ReadInt()
	// log.Printf("SZ: %v", size)

	startTime := time.Now()

	aggregator := Aggregator.NewAggregator()

	vlanBatch := make([]uint16, size/2, size)

	for i < size {
		mapValue := make(map[string]interface{})

		message.ReadObject(packet, mapValue)

		if len(mapValue) != 0 {

			caption := strings.ToLower(message.GetCaption())

			switch caption {
			case "protos":
				{
					var vlan uint16

					vlan, err = aggregator.AddNetIfaceBatch(portId, instance, mapValue)

					vlanBatch = append(vlanBatch, vlan)
				}
			case "dns":
				{
					err = aggregator.AddDnsBatch(portId, instance, mapValue)
				}
			default:
				{
					err = aggregator.AddNetSessionBatch(portId, instance, mapValue, caption)
				}
			}
		}

		if err != nil {
			log.Println("Close connection")
			return
		}

		i++
	}

	if len(vlanBatch) != 0 {
		go inserVlan(portId, instance, vlanBatch)
	}

	log.Println("Clickhouse aggregate batch ", time.Now().Sub(startTime))

	go aggregator.Execute()

}

func inserVlan(portId string, instance string, batch []uint16) {
	aggr := Aggregator.NewAggregator()

	for _, vlan := range batch {
		if err := aggr.AddVlanBatch(portId, instance, vlan); err != nil {
			log.Println("Error insert vlan")
		}
	}

	aggr.Execute()
}

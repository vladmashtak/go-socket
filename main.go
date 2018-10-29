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
	var (
		err     error
		server  string
		tcpAddr *net.TCPAddr
		ln      *net.TCPListener
		options *Config.Options
		logger  = Logger.GetLogger()
	)

	options = Config.GetOptions()

	server = fmt.Sprintf(":%d", options.Port)

	tcpAddr, err = net.ResolveTCPAddr("tcp4", server)

	ln, err = net.ListenTCP("tcp", tcpAddr)

	if err != nil {
		logger.Info("Can't create tcp4 server ", zap.Error(err))
		return
	}

	logger.Info("Start work ", zap.String("server", server))

	defer logger.Sync()
	defer ln.Close()

	for {
		conn, err := ln.AcceptTCP()

		if err != nil {
			logger.Info("Can't create connection ", zap.Error(err))
			continue
		}

		conn.SetWriteDeadline(time.Now().Add(5 * time.Second))

		go handleConnection(conn)
	}
}

func handleConnection(conn *net.TCPConn) {
	var (
		err    error
		in     []byte
		reader *bufio.Reader
		packet *PacketReader.PacketReader
		logger = Logger.GetLogger()
	)

	logger.Info("Accept connection ", zap.String("address", conn.RemoteAddr().String()))

	reader = bufio.NewReader(conn)

	in, err = ioutil.ReadAll(reader)

	packet, err = PacketReader.NewPacketReader(in)

	if err != nil {
		logger.Info("Read input", zap.Error(err))
		return
	}

	go insertMessage(packet)

	defer func(c *net.TCPConn) {
		logger.Info("End connection ", zap.String("address", c.RemoteAddr().String()))

		c.Close()
	}(conn)
}

func insertMessage(packet *PacketReader.PacketReader) {
	var (
		i      uint32
		err    error
		logger = Logger.GetLogger()
	)

	startTime := time.Now()

	message := Deserializer.NewMessage()

	message.Read(packet)

	instance := packet.ReadString()

	portId := packet.ReadString()

	size := packet.ReadInt()

	aggregator := Aggregator.NewAggregator()

	vlanBatch := make([]uint16, size)

	for i < size {
		mapValue := make(map[string]interface{})

		message.ReadObject(packet, mapValue)

		if len(mapValue) != 0 {

			caption := strings.ToLower(message.GetCaption())

			switch caption {
			case Aggregator.PROTOCOLS:
				{
					var vlan uint16

					vlan, err = aggregator.AddNetIfaceBatch(portId, instance, mapValue)

					vlanBatch[i] = vlan
				}
			case Aggregator.DNS:
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
			logger.Info("Can't create batch", zap.Error(err))
			return
		}

		i++
	}

	if len(vlanBatch) != 0 {
		go inserVlan(portId, instance, vlanBatch)
	}

	aggregator.Execute()

	logger.Info("Aggregate", zap.String("instance name ", instance), zap.Uint32("packet size", size), zap.Duration("time", time.Now().Sub(startTime)))
}

func inserVlan(portId string, instance string, batch []uint16) {
	var (
		aggregator = Aggregator.NewAggregator()
		logger     = Logger.GetLogger()
	)

	for _, vlan := range batch {
		if err := aggregator.AddVlanBatch(portId, instance, vlan); err != nil {
			logger.Error("Error insert vlan", zap.Error(err))
		}
	}

	aggregator.Execute()
}

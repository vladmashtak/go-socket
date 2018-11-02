package main

import (
	"bufio"
	"engine-socket/Aggregator"
	"engine-socket/Config"
	"engine-socket/Deserializer"
	"engine-socket/Logger"
	"engine-socket/PacketReader"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"net"
	"strings"
	"time"

	"github.com/klauspost/compress/zlib"
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
		err         error
		in          []byte
		reader      *bufio.Reader
		decompresor io.ReadCloser
		logger      = Logger.GetLogger()
	)

	logger.Info("Accept connection ", zap.String("address", conn.RemoteAddr().String()))

	reader = bufio.NewReader(conn) // start read connection buffer

	_, err = reader.Discard(8) // skip 8 items

	decompresor, err = zlib.NewReader(reader) // decompress buffer

	in, err = ioutil.ReadAll(decompresor) // read from buffer to []byte

	if err != nil {
		logger.Info("Can't read input stream", zap.Error(err))
		return
	}

	go insertMessage(in)

	defer func(c *net.TCPConn, d io.ReadCloser) {
		logger.Info("End connection ", zap.String("address", c.RemoteAddr().String()))

		d.Close()

		c.Close()
	}(conn, decompresor)
}

func insertMessage(in []byte) {
	var (
		i         uint32
		vlanCount uint32
		err       error
		logger    = Logger.GetLogger()
		packet    *PacketReader.PacketReader
	)

	startTime := time.Now()

	packet, err = PacketReader.NewPacketReader(in)

	if err != nil {
		logger.Info("Can't read packet", zap.Error(err))
		return
	}

	message := Deserializer.NewMessage(packet)

	caption := strings.ToLower(message.GetCaption()) // caption for switch type of message

	instance, portId, packetSize := packet.ReadMetadata()

	aggregator := Aggregator.NewAggregator()

	vlanBatch := make([]uint16, 100)

	mapValue := Deserializer.NewDictionary(message.GetFieldsSize())

	for ; i < packetSize; i++ {

		message.ReadObject(packet, mapValue)

		if len(mapValue) != 0 {

			switch caption {
			case Aggregator.PROTOCOLS:
				{
					var vlan uint16

					vlan, err = aggregator.AddNetIfaceBatch(portId, instance, mapValue)

					if vlan < Aggregator.SHORT_VLAN {
						vlanBatch[vlanCount] = vlan
						vlanCount++
					}
				}
			case Aggregator.DNS:
				{
					err = aggregator.AddDnsBatch(portId, instance, mapValue)
				}
			case Aggregator.HTTP, Aggregator.SSL, Aggregator.UDP, Aggregator.TCP:
				{
					err = aggregator.AddNetSessionBatch(portId, instance, mapValue, caption)
				}
			default:
				{
					err = errors.New("Invalid caption")
				}
			}

			mapValue.Clear() // clear dictionary
		}

		if err != nil {
			logger.Info(
				"Can't create batch",
				zap.Error(err),
				zap.String("interface name ", instance),
				zap.String("type", caption),
				zap.Uint32("packet size", packetSize),
				zap.Duration("time", time.Now().Sub(startTime)),
			)

			aggregator.Close()
			return
		}
	}

	if len(vlanBatch) != 0 {
		go inserVlan(portId, instance, vlanBatch)
	}

	aggregator.Execute()

	logger.Info(
		"Aggregate",
		zap.String("interface name ", instance),
		zap.String("type", caption),
		zap.Uint32("packet size", packetSize),
		zap.Duration("time", time.Now().Sub(startTime)),
	)
}

func inserVlan(portId string, instance string, batch []uint16) {
	var (
		aggregator = Aggregator.NewAggregator()
		logger     = Logger.GetLogger()
	)

	for _, vlan := range batch {
		if err := aggregator.AddVlanBatch(portId, instance, vlan); err != nil {
			logger.Error("Error insert vlan", zap.Error(err))
			aggregator.Close()
			return
		}
	}

	aggregator.Execute()
}

package main

import (
	"github.com/tidwall/evio"
	"log"
	"engine-socket/Deserializer"
	"strings"
	"engine-socket/Aggregator"
	"engine-socket/PacketReader"
)

func main() {
	var events evio.Events

	events.NumLoops = 2

	events.Opened = func(c evio.Conn) (out []byte, opts evio.Options, action evio.Action) {
		c.SetContext(&evio.InputStream{})
		log.Printf("opened: laddr: %v: raddr: %v", c.LocalAddr(), c.RemoteAddr())
		return
	}

	events.Closed = func(c evio.Conn, err error) (action evio.Action) {
		log.Printf("closed: %s: %s", c.LocalAddr().String(), c.RemoteAddr().String())
		return
	}

	events.Data = func(c evio.Conn, in []byte) (out []byte, action evio.Action) {
		if in == nil {
			return
		}

		packet := PacketReader.NewPacketReader(in)

		message := Deserializer.NewMessage()

		message.Read(packet)

		instance := packet.ReadString()
		log.Printf("Read instance: %s", instance)

		portId := packet.ReadString()
		log.Printf("Read port: %s", portId)

		size := packet.ReadInt()
		// log.Printf("SZ: %v", size)

		// list := make([]map[string]interface{}, size)

		var i uint32 = 0

		aggregator := Aggregator.NewAggregator()

		for i < size {
			mapValue := make(map[string]interface{})

			message.ReadObject(packet, mapValue)
			if len(mapValue) != 0 {
				// list = append(list, mapValue)

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

		return
	}

	if err := evio.Serve(events, "tcp://localhost:5000"); err != nil {
		panic(err.Error())
	}
}

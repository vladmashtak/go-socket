package main

import (
	"engine-socket/Aggregator"
	"engine-socket/Deserializer"
	"engine-socket/PacketReader"
	"log"
	"strings"
	"time"

	"github.com/tidwall/evio"
)

func runningtime(s string) (string, time.Time) {
	log.Println("Start:	", s)
	return s, time.Now()
}

func track(s string, startTime time.Time) {
	endTime := time.Now()
	log.Println("End:	", s, "took", endTime.Sub(startTime))
}

func main() {
	var events evio.Events

	events.NumLoops = -1

	events.Opened = func(c evio.Conn) (out []byte, opts evio.Options, action evio.Action) {
		c.SetContext(&evio.InputStream{})
		log.Printf("Opened: laddr: %v: raddr: %v", c.LocalAddr(), c.RemoteAddr())
		return
	}

	events.Closed = func(c evio.Conn, err error) (action evio.Action) {
		log.Printf("Closed: %s: %s", c.LocalAddr().String(), c.RemoteAddr().String())
		return
	}

	events.Data = func(c evio.Conn, in []byte) (out []byte, action evio.Action) {
		if in == nil {
			return
		}

		defer track(runningtime("Execute"))

		packet := PacketReader.NewPacketReader(in)

		message := Deserializer.NewMessage()

		message.Read(packet)

		instance := packet.ReadString()
		// log.Printf("Read instance: %s", instance)

		portId := packet.ReadString()
		// log.Printf("Read portId: %s", portId)

		size := packet.ReadInt()
		// log.Printf("SZ: %v", size)

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

		return
	}

	if err := evio.Serve(events, "tcp://localhost:5000"); err != nil {
		panic(err.Error())
	}
}

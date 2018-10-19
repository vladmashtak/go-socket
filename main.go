package main

import (
	"github.com/tidwall/evio"
	"log"
	"engine-socket/PacketReader"
	"engine-socket/Deserializer"
	"strings"
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

		PacketReader.ReadInput(in)

		return
	}

	if err := evio.Serve(events, "tcp://localhost:5000"); err != nil {
		panic(err.Error())
	}
}

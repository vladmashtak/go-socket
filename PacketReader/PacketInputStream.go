package PacketReader

import (
	"engine-socket/Deserializer"
	"log"
	"strings"
)

func ReadInput(in []byte) {
	packet := NewPacketReader(in)

	message := Deserializer.NewMessage()

	message.Read(packet)

	instance := packet.ReadString()
	log.Printf("Read instance: %s", instance)

	port := packet.ReadString()
	log.Printf("Read port: %s", port)

	size := packet.ReadInt()
	// log.Printf("SZ: %v", size)

	var i uint32 = 0

	for i < size {
		mapValue := make(map[string]interface{})

		message.ReadObject(packet, mapValue)
		i++
	}

	caption := strings.ToUpper(message.GetCaption())

	switch caption {
	case "PROTOS":
	case "DNS":
	default:


	}

}
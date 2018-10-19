package Deserializer

import (
	reader "engine-socket/PacketReader"
)

type Message struct {
	caption string
	serviceType string
	technologyType string
	fieldTable map[uint32]*Field
}

func NewMessage() *Message {
	return &Message{fieldTable: make(map[uint32]*Field)}
}

func (m *Message) Read(packet *reader.PacketReader) {
	m.serviceType = packet.ReadString()
	// log.Printf("Service Type: %v", m.serviceType)

	m.technologyType = packet.ReadString()
	// log.Printf("Technology Type: %v", m.technologyType)

	size := packet.ReadInt()
	// log.Printf("Size: %v", size)

	m.caption = packet.ReadString()
	// log.Printf("caption: %v", m.caption)

	var i uint32 = 0

	for i < size {
		fld := NewField()
		fld.Read(packet)

		m.fieldTable[i] = fld

		i++
	}
}

func (m* Message) ReadObject(packet *reader.PacketReader, mapValue map[string]interface{}) {

	for _, field := range m.fieldTable {

		fieldValue := NewFieldValue()
		fieldValue.Read(packet)

		mapValue[field.id] = fieldValue.value
	}
}

func (m *Message) GetCaption() string {
	return m.caption
}

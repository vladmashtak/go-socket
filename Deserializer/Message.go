package Deserializer

import (
	reader "engine-socket/PacketReader"
)

type Message struct {
	caption        string
	serviceType    string
	technologyType string
	fieldArray     []*Field
}

func NewMessage() *Message {
	return &Message{}
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

	if m.fieldArray == nil {
		m.fieldArray = make([]*Field, size)
	}

	for i, _ := range m.fieldArray {
		fld := NewField()
		fld.Read(packet)

		m.fieldArray[i] = fld
	}
}

func (m *Message) ReadObject(packet *reader.PacketReader, mapValue map[string]interface{}) {

	for _, field := range m.fieldArray {
		// log.Printf("fieldType: %s", field.id)

		fieldValue := ReadValue(packet)
		mapValue[field.id] = fieldValue
	}
}

func (m *Message) GetCaption() string {
	return m.caption
}

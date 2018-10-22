package Deserializer

import (
	reader "engine-socket/PacketReader"
)

type Field struct {
	id        string
	fieldType byte
	must      byte
}

func NewField() *Field {
	return &Field{}
}

func (f *Field) GetId() string {
	return f.id
}

func (f *Field) IsMust() bool {
	return f.must > 0
}

func (f *Field) SetMust(must byte) {
	f.must = must
}

func (f *Field) Read(packet *reader.PacketReader) {
	f.fieldType = packet.ReadByte()
	// log.Printf("fieldType: %v", f.fieldType)

	f.id = packet.ReadString()
	// log.Printf("id: %s", f.id)

	f.must = packet.ReadByte()
	// log.Printf("must: %v", f.must)
}

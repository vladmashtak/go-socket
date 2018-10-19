package Deserializer

import (
	reader "engine-socket/PacketReader"
)

const (
	unknownType byte = 0
	longType byte = 1
	stringType byte = 2
	datetimeType byte = 3
	byteType byte = 4
	doubleType byte = 5
	byteArray byte = 6
)

type FieldValue struct {
	value interface{}
	fieldValueType byte
}

func NewFieldValue() *FieldValue {
	return &FieldValue{nil, unknownType}
}

func (f* FieldValue) Read(packet *reader.PacketReader) {
	f.fieldValueType = packet.ReadByte()
	
	if f.fieldValueType == unknownType {
		return
	}

	switch f.fieldValueType {
	case byteType:
		f.value = packet.ReadByte()
	case byteArray:
		f.value = packet.ReadByteArray()
	case datetimeType, longType:
		f.value = packet.ReadLong()
	case doubleType:
		f.value = packet.ReadDouble()
	case stringType:
		f.value = packet.ReadString()
	}
}

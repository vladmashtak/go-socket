package Deserializer

import (
	reader "engine-socket/PacketReader"
)

const (
	unknownType  byte = 0
	longType     byte = 1
	stringType   byte = 2
	datetimeType byte = 3
	byteType     byte = 4
	doubleType   byte = 5
	byteArray    byte = 6
)

func ReadValue(packet *reader.PacketReader) interface{} {
	var fieldValue interface{}
	fieldValueType := packet.ReadByte()

	if fieldValueType == unknownType {
		return fieldValue
	}

	switch fieldValueType {
	case byteType:
		fieldValue = packet.ReadByte()
	case byteArray:
		fieldValue = packet.ReadByteArray()
	case datetimeType, longType:
		fieldValue = packet.ReadLong()
	case doubleType:
		fieldValue = packet.ReadDouble()
	case stringType:
		fieldValue = packet.ReadString()
	}

	// log.Printf("fieldValue: %v", fieldValue)

	return fieldValue
}

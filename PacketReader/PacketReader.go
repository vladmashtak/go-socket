package PacketReader

import (
	"encoding/binary"
	"compress/zlib"
	"bytes"
	"io/ioutil"
	"log"
)

type PacketReader struct {
	buffer []byte
	index uint32
}

func NewPacketReader(buffer []byte) *PacketReader {
	p := PacketReader{buffer, 0}

	p.decompress()

	return &p
}

func (p *PacketReader) decompress() {
	p.ReadInt()
	p.ReadInt()

	reader, err := zlib.NewReader(bytes.NewReader(p.buffer[p.index:]))
	defer reader.Close()

	if err != nil {
		log.Fatal(err)
	}

	message, err := ioutil.ReadAll(reader)

	if err != nil {
		log.Fatal(err)
	}

	p.index = 0
	p.buffer = message
}

func (p *PacketReader) shiftCursor(offset uint32) []byte {
	endIndex := p.index + offset

	in := p.buffer[p.index : endIndex]
	p.index = endIndex

	return in
}

func (p *PacketReader) ReadInt() uint32 {
	const Integer = 4

	in := p.shiftCursor(Integer)

	return binary.BigEndian.Uint32(in)
}

func (p *PacketReader) ReadLong() uint64 {
	const Long = 8

	in := p.shiftCursor(Long)

	return binary.BigEndian.Uint64(in)
}

func (p *PacketReader) ReadDouble() float64 {
	return float64(p.ReadLong())
}

func (p *PacketReader) ReadString() string {
	strLen := p.ReadInt()

	in := p.shiftCursor(strLen)


	return string(in)
}

func (p *PacketReader) ReadByte() byte {
	b := p.buffer[p.index]
	p.index += 1

	return b
}

func (p *PacketReader) ReadByteArray() []byte {
	size := p.ReadInt()

	if size == 0 {
		return nil
	}

	return p.shiftCursor(size)
}

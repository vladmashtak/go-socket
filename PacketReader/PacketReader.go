package PacketReader

import (
	"bufio"
	"encoding/binary"
)

const (
	Integer uint32 = 4
	Long    uint32 = 8
)

type PacketReader struct {
	buffer *bufio.Reader
}

func NewPacketReader(buffer *bufio.Reader) (*PacketReader, error) {
	p := PacketReader{buffer}

	return &p, nil
}

func (p *PacketReader) shiftCursor(size uint32) ([]byte, error) {
	in := make([]byte, size)

	_, err := p.buffer.Read(in)

	return in, err
}

func (p *PacketReader) ReadInt() (uint32, error) {
	in, err := p.shiftCursor(Integer)

	if err != nil {
		return 0, err
	}

	return binary.BigEndian.Uint32(in), err
}

func (p *PacketReader) ReadLong() (uint64, error) {
	in, err := p.shiftCursor(Long)

	if err != nil {
		return 0, err
	}

	return binary.BigEndian.Uint64(in), err
}

func (p *PacketReader) ReadDouble() (float64, error) {
	l, err := p.ReadLong()

	if err != nil {
		return 0, err
	}

	return float64(l), err
}

func (p *PacketReader) ReadString() (string, error) {
	var (
		err error
		str []byte
	)

	str, err = p.ReadByteArray()

	if err != nil {
		return "", err
	}

	return string(str), err
}

func (p *PacketReader) ReadByte() (byte, error) {
	b, err := p.buffer.ReadByte()

	return b, err
}

func (p *PacketReader) ReadByteArray() ([]byte, error) {
	var (
		err   error
		size  uint32
		slice []byte
	)

	size, err = p.ReadInt()

	if size == 0 {
		return nil, err
	}

	slice, err = p.shiftCursor(size)

	if err != nil {
		return nil, err
	}

	return slice, err
}

func (p *PacketReader) ReadMetadata() (instance string, portId string, packetSize uint32, err error) {
	instance, err = p.ReadString()

	portId, err = p.ReadString()

	packetSize, err = p.ReadInt()

	return
}

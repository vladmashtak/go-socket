package PacketReader

import (
	"bytes"
	"encoding/binary"
	"engine-socket/Logger"
	"io"

	"github.com/klauspost/compress/zlib"
	"go.uber.org/zap"
)

const (
	Integer uint32 = 4
	Long    uint32 = 8
)

type PacketReader struct {
	buffer []byte
	index  uint32
}

func NewPacketReader(buffer []byte) (*PacketReader, error) {
	p := PacketReader{buffer, 0}

	err := p.decompress()

	return &p, err
}

func (p *PacketReader) decompress() error {

	p.ReadInt()
	capacity := p.ReadInt()

	var (
		err     error
		reader  io.ReadCloser
		message []byte = make([]byte, capacity)
		logger         = Logger.GetLogger()
	)

	if reader, err = zlib.NewReader(bytes.NewReader(p.buffer[p.index:])); err != nil {
		return err
	}

	defer reader.Close()

	if _, err = reader.Read(message); err != nil && err.Error() != "EOF" {
		logger.Info("Decompress", zap.Error(err))
	}

	p.index = 0
	p.buffer = message

	return nil
}

func (p *PacketReader) shiftCursor(offset uint32) []byte {
	endIndex := p.index + offset

	in := p.buffer[p.index:endIndex]
	p.index = endIndex

	return in
}

func (p *PacketReader) ReadInt() uint32 {
	in := p.shiftCursor(Integer)

	return binary.BigEndian.Uint32(in)
}

func (p *PacketReader) ReadLong() uint64 {
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

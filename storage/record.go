package storage

import (
	"bytes"
	"encoding/binary"
	"hash/crc32"
	"io"
)

type Operation byte

const (
	Put Operation = 1
	Del Operation = 2
)

type Record struct {
	CRC       uint32
	Op        Operation
	KeyLen    uint32
	ValueLen  uint32
	Key       []byte
	Value     []byte
}

func NewRecord(op Operation, key, value []byte) *Record {
	r := &Record{
		Op:       op,
		KeyLen:   uint32(len(key)),
		ValueLen: uint32(len(value)),
		Key:      key,
		Value:    value,
	}

	r.CRC = r.computeCRC()
	return r
}

func (r *Record) computeCRC() uint32 {
	buf := bytes.Buffer{}

	buf.WriteByte(byte(r.Op))
	binary.Write(&buf, binary.LittleEndian, r.KeyLen)
	binary.Write(&buf, binary.LittleEndian, r.ValueLen)
	buf.Write(r.Key)
	buf.Write(r.Value)

	return crc32.ChecksumIEEE(buf.Bytes())
}

func (r *Record) Encode() ([]byte, error) {
	buf := new(bytes.Buffer)

	binary.Write(buf, binary.LittleEndian, r.CRC)
	buf.WriteByte(byte(r.Op))
	binary.Write(buf, binary.LittleEndian, r.KeyLen)
	binary.Write(buf, binary.LittleEndian, r.ValueLen)

	buf.Write(r.Key)
	buf.Write(r.Value)

	return buf.Bytes(), nil
}

func DecodeRecord(reader io.Reader) (*Record, error) {

	r := &Record{}

	err := binary.Read(reader, binary.LittleEndian, &r.CRC)
	if err != nil {
		return nil, err
	}

	var op byte
	if err := binary.Read(reader, binary.LittleEndian, &op); err != nil {
		return nil, err
	}

	r.Op = Operation(op)

	if err := binary.Read(reader, binary.LittleEndian, &r.KeyLen); err != nil {
		return nil, err
	}

	if err := binary.Read(reader, binary.LittleEndian, &r.ValueLen); err != nil {
		return nil, err
	}

	r.Key = make([]byte, r.KeyLen)
	r.Value = make([]byte, r.ValueLen)

	io.ReadFull(reader, r.Key)
	io.ReadFull(reader, r.Value)

	if r.CRC != r.computeCRC() {
		return nil, io.ErrUnexpectedEOF
	}

	return r, nil
}
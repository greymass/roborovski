package appendlog

import (
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
)

const (
	HeaderSize    = 32
	HeaderMagic   = "HWSLICE"
	HeaderVersion = 1
)

var (
	ErrInvalidMagic   = errors.New("invalid header magic")
	ErrInvalidVersion = errors.New("unsupported header version")
	ErrNoHeader       = errors.New("file has no header (legacy format)")
)

type DataLogHeader struct {
	Magic          [7]byte
	Version        uint8
	BlocksPerSlice uint32
	StartBlock     uint32
	EndBlock       uint32
	Finalized      uint8
	Reserved       [11]byte
}

func NewDataLogHeader(blocksPerSlice, startBlock, endBlock uint32) *DataLogHeader {
	h := &DataLogHeader{
		Version:        HeaderVersion,
		BlocksPerSlice: blocksPerSlice,
		StartBlock:     startBlock,
		EndBlock:       endBlock,
		Finalized:      0,
	}
	copy(h.Magic[:], HeaderMagic)
	return h
}

func (h *DataLogHeader) Bytes() []byte {
	buf := make([]byte, HeaderSize)
	copy(buf[0:7], h.Magic[:])
	buf[7] = h.Version
	binary.LittleEndian.PutUint32(buf[8:12], h.BlocksPerSlice)
	binary.LittleEndian.PutUint32(buf[12:16], h.StartBlock)
	binary.LittleEndian.PutUint32(buf[16:20], h.EndBlock)
	buf[20] = h.Finalized
	return buf
}

func ParseDataLogHeader(data []byte) (*DataLogHeader, error) {
	if len(data) < HeaderSize {
		return nil, fmt.Errorf("header too short: %d bytes", len(data))
	}

	h := &DataLogHeader{}
	copy(h.Magic[:], data[0:7])
	h.Version = data[7]
	h.BlocksPerSlice = binary.LittleEndian.Uint32(data[8:12])
	h.StartBlock = binary.LittleEndian.Uint32(data[12:16])
	h.EndBlock = binary.LittleEndian.Uint32(data[16:20])
	h.Finalized = data[20]
	copy(h.Reserved[:], data[21:32])

	return h, nil
}

func (h *DataLogHeader) Validate() error {
	if string(h.Magic[:]) != HeaderMagic {
		return ErrInvalidMagic
	}
	if h.Version != HeaderVersion {
		return fmt.Errorf("%w: got %d, expected %d", ErrInvalidVersion, h.Version, HeaderVersion)
	}
	return nil
}

func ReadDataLogHeader(path string) (*DataLogHeader, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	buf := make([]byte, HeaderSize)
	n, err := file.Read(buf)
	if err != nil {
		return nil, err
	}
	if n < HeaderSize {
		return nil, ErrNoHeader
	}

	header, err := ParseDataLogHeader(buf)
	if err != nil {
		return nil, err
	}

	if err := header.Validate(); err != nil {
		return nil, err
	}

	return header, nil
}

func UpdateFinalizedFlag(path string, finalized bool) error {
	file, err := os.OpenFile(path, os.O_RDWR, 0644)
	if err != nil {
		return err
	}
	defer file.Close()

	var flag byte
	if finalized {
		flag = 1
	}

	_, err = file.WriteAt([]byte{flag}, 20)
	if err != nil {
		return err
	}

	return file.Sync()
}

func HasHeader(path string) (bool, error) {
	file, err := os.Open(path)
	if err != nil {
		if os.IsNotExist(err) {
			return false, nil
		}
		return false, err
	}
	defer file.Close()

	buf := make([]byte, 7)
	n, err := file.Read(buf)
	if err == io.EOF || n < 7 {
		return false, nil
	}
	if err != nil {
		return false, err
	}

	return string(buf) == HeaderMagic, nil
}

func ComputeFileHash(path string) (string, error) {
	file, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer file.Close()

	hasher := sha256.New()
	if _, err := io.Copy(hasher, file); err != nil {
		return "", err
	}

	return hex.EncodeToString(hasher.Sum(nil)), nil
}

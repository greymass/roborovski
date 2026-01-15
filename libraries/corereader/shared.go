package corereader

import (
	"bytes"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strconv"
	"sync"

	"github.com/greymass/roborovski/libraries/chain"
	"github.com/greymass/roborovski/libraries/compression"
	"github.com/greymass/roborovski/libraries/encoding"
)

const (
	DataLogHeaderSize    = 32
	DataLogHeaderMagic   = "HWSLICE"
	DataLogHeaderVersion = 1
)

var (
	ErrInvalidHeaderMagic   = errors.New("invalid header magic")
	ErrInvalidHeaderVersion = errors.New("unsupported header version")
	ErrNoHeader             = errors.New("file has no header")
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

func ParseDataLogHeader(data []byte) (*DataLogHeader, error) {
	if len(data) < DataLogHeaderSize {
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
	if string(h.Magic[:]) != DataLogHeaderMagic {
		return ErrInvalidHeaderMagic
	}
	if h.Version != DataLogHeaderVersion {
		return fmt.Errorf("%w: got %d, expected %d", ErrInvalidHeaderVersion, h.Version, DataLogHeaderVersion)
	}
	return nil
}

func (h *DataLogHeader) IsFinalized() bool {
	return h.Finalized == 1
}

func ReadDataLogHeader(path string) (*DataLogHeader, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	buf := make([]byte, DataLogHeaderSize)
	n, err := file.Read(buf)
	if err != nil {
		return nil, err
	}
	if n < DataLogHeaderSize {
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

type blockIndexEntry struct {
	Offset  uint64
	Size    uint32
	GlobMin uint64
	GlobMax uint64
}

func loadBlockIndexSimple(path string) (map[uint32]blockIndexEntry, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}

	if len(data) < 4 {
		return nil, fmt.Errorf("block index too small")
	}

	count := binary.LittleEndian.Uint32(data[0:4])

	index := make(map[uint32]blockIndexEntry, count)
	const entrySize = 32

	offset := 4
	for i := uint32(0); i < count; i++ {
		if offset+entrySize > len(data) {
			break
		}

		blockNum := binary.LittleEndian.Uint32(data[offset : offset+4])
		fileOffset := binary.LittleEndian.Uint64(data[offset+4 : offset+12])
		size := binary.LittleEndian.Uint32(data[offset+12 : offset+16])
		globMin := binary.LittleEndian.Uint64(data[offset+16 : offset+24])
		globMax := binary.LittleEndian.Uint64(data[offset+24 : offset+32])

		index[blockNum] = blockIndexEntry{
			Offset:  fileOffset,
			Size:    size,
			GlobMin: globMin,
			GlobMax: globMax,
		}

		offset += entrySize
	}

	return index, nil
}

type trxMeta struct {
	Status         uint8
	CpuUsageUs     uint32
	NetUsageWords  uint32
	Expiration     uint32
	RefBlockNum    uint16
	RefBlockPrefix uint32
	Signatures     [][]byte
}

type blockData struct {
	BlockNum        uint32
	BlockTime       string
	BlockTimeUint32 uint32
	MinGlobInBlock  uint64
	MaxGlobInBlock  uint64
	ProducerBlockID [32]byte
	TrxIDInBlock    [][32]byte
	TrxMetaInBlock  []trxMeta
	DataInBlock     [][]byte
	NamesInBlock    []uint64
}

type blockBlob struct {
	Block      *blockData
	Cats       []*compressedActionTrace
	CatsOffset []uint32
	CatsBytes  [][]byte
}

type compressedActionTrace struct {
	GlobalSequence    uint64
	Elapsed           int64
	RecvSequence      uint64
	ActionOrdinal     uint32
	CreatorAO         uint32
	ClosestUAAO       uint32
	CodeSequence      uint32
	AbiSequence       uint32
	ReceiverIndex     uint32
	ContractNameIndex uint32
	ActionNameIndex   uint32
	TrxIDIndex        uint32
	DataIndex         uint32
	ContextFree       bool
	Auths             []authTriple
	AccountRAMDeltas  []accountDelta
}

type authTriple struct {
	AccountIndex    uint32
	PermissionIndex uint32
	Seq             uint64
}

type accountDelta struct {
	AccountIndex uint32
	Delta        int64
}

// syncBlockData is a minimal block structure optimized for bulk sync.
// It only contains fields needed for indexing, avoiding allocations for
// TrxMetaInBlock signatures and DataInBlock which are not needed during sync.
type syncBlockData struct {
	BlockNum        uint32
	BlockTimeUint32 uint32
	MinGlobInBlock  uint64
	MaxGlobInBlock  uint64
	NamesInBlock    []uint64
	TrxCount        uint32
}

// syncActionInfo contains minimal action info needed for indexing.
type syncActionInfo struct {
	ActionOrdinal      uint32
	CreatorAO          uint32
	ReceiverIndex      uint32
	ContractNameIndex  uint32
	ActionNameIndex    uint32
	TrxIDIndex         uint32
	DataIndex          uint32
	AuthAccountIndexes []uint32
	CatOffset          uint32
}

// byteSliceReader is a zero-allocation reader for byte slices.
// It implements io.Reader and io.ByteReader for use with binary.ReadUvarint.
type byteSliceReader struct {
	data []byte
	pos  int
}

func (r *byteSliceReader) Reset(data []byte) {
	r.data = data
	r.pos = 0
}

func (r *byteSliceReader) ReadByte() (byte, error) {
	if r.pos >= len(r.data) {
		return 0, fmt.Errorf("EOF")
	}
	b := r.data[r.pos]
	r.pos++
	return b, nil
}

func (r *byteSliceReader) Read(p []byte) (int, error) {
	if r.pos >= len(r.data) {
		return 0, fmt.Errorf("EOF")
	}
	n := copy(p, r.data[r.pos:])
	r.pos += n
	return n, nil
}

func (r *byteSliceReader) Skip(n int) {
	r.pos += n
}

func (r *byteSliceReader) ReadUvarint() uint64 {
	val, _ := binary.ReadUvarint(r)
	return val
}

func (r *byteSliceReader) ReadVarint() int64 {
	val, _ := binary.ReadVarint(r)
	return val
}

// syncBlockBlob is a pooled structure for sync-optimized block parsing.
type syncBlockBlob struct {
	Block      syncBlockData
	Actions    []syncActionInfo
	CatsOffset []uint32
	authBuf    []uint32
	catLengths []uint64
	reader     byteSliceReader
}

var syncBlockBlobPool = sync.Pool{
	New: func() interface{} {
		return &syncBlockBlob{
			Block: syncBlockData{
				NamesInBlock: make([]uint64, 0, 64),
			},
			Actions:    make([]syncActionInfo, 0, 128),
			CatsOffset: make([]uint32, 0, 128),
			authBuf:    make([]uint32, 0, 256),
			catLengths: make([]uint64, 0, 128),
		}
	},
}

func (b *syncBlockBlob) reset() {
	b.Block.NamesInBlock = b.Block.NamesInBlock[:0]
	b.Actions = b.Actions[:0]
	b.CatsOffset = b.CatsOffset[:0]
	b.authBuf = b.authBuf[:0]
	b.catLengths = b.catLengths[:0]
}

// bytesToSyncBlockBlob parses block data into a pooled structure optimized for sync.
func bytesToSyncBlockBlob(inBytes []byte) *syncBlockBlob {
	blob := syncBlockBlobPool.Get().(*syncBlockBlob)
	blob.reset()
	blob.reader.Reset(inBytes)
	r := &blob.reader

	const maxReasonableSize = 100_000_000

	var tmp4 [4]byte
	var tmp8 [8]byte

	r.Read(tmp4[:])
	blob.Block.BlockNum = binary.LittleEndian.Uint32(tmp4[:])

	r.Read(tmp4[:])
	blob.Block.BlockTimeUint32 = binary.LittleEndian.Uint32(tmp4[:])

	r.Skip(32)

	r.Read(tmp8[:])
	blob.Block.MaxGlobInBlock = binary.LittleEndian.Uint64(tmp8[:])

	globCount := r.ReadUvarint()
	blob.Block.MinGlobInBlock = blob.Block.MaxGlobInBlock - globCount

	txLen := r.ReadUvarint()
	blob.Block.TrxCount = uint32(txLen)

	for i := uint64(0); i < txLen; i++ {
		r.Skip(32)
	}

	for i := uint64(0); i < txLen; i++ {
		r.ReadByte()
		r.Skip(4)
		r.ReadUvarint()
		r.Skip(4)
		r.Skip(2)
		r.Skip(4)
		sigCount := r.ReadUvarint()
		for j := uint64(0); j < sigCount; j++ {
			sigLen := r.ReadUvarint()
			r.Skip(int(sigLen))
		}
	}

	dataLen := r.ReadUvarint()
	for i := uint64(0); i < dataLen; i++ {
		dataIndexLen := r.ReadUvarint()
		r.Skip(int(dataIndexLen))
	}

	nameLen := r.ReadUvarint()
	if cap(blob.Block.NamesInBlock) < int(nameLen) {
		blob.Block.NamesInBlock = make([]uint64, nameLen)
	} else {
		blob.Block.NamesInBlock = blob.Block.NamesInBlock[:nameLen]
	}
	for i := uint64(0); i < nameLen; i++ {
		r.Read(tmp8[:])
		blob.Block.NamesInBlock[i] = binary.LittleEndian.Uint64(tmp8[:])
	}

	catsLen := r.ReadUvarint()
	if catsLen > maxReasonableSize {
		releaseSyncBlockBlob(blob)
		panic(fmt.Sprintf("Block %d: catsLen=%d exceeds reasonable limit", blob.Block.BlockNum, catsLen))
	}

	if cap(blob.CatsOffset) < int(catsLen) {
		blob.CatsOffset = make([]uint32, catsLen)
	} else {
		blob.CatsOffset = blob.CatsOffset[:catsLen]
	}
	for i := uint64(0); i < catsLen; i++ {
		blob.CatsOffset[i] = uint32(r.ReadUvarint())
	}

	if cap(blob.catLengths) < int(catsLen) {
		blob.catLengths = make([]uint64, catsLen)
	} else {
		blob.catLengths = blob.catLengths[:catsLen]
	}
	for i := uint64(0); i < catsLen; i++ {
		blob.catLengths[i] = r.ReadUvarint()
	}

	if cap(blob.Actions) < int(catsLen) {
		blob.Actions = make([]syncActionInfo, catsLen)
	} else {
		blob.Actions = blob.Actions[:catsLen]
	}

	authBufOffset := 0
	actionDataStart := r.pos
	for i := uint64(0); i < catsLen; i++ {
		catLen := int(blob.catLengths[i])
		catEnd := r.pos + catLen
		action := parseSyncActionDirect(r, blob, &authBufOffset)
		action.CatOffset = blob.CatsOffset[i]
		blob.Actions[i] = action
		r.pos = catEnd
	}
	_ = actionDataStart

	return blob
}

func parseSyncAction(inBytes []byte, blob *syncBlockBlob, authBufOffset *int) syncActionInfo {
	action := syncActionInfo{}
	buff := bytes.NewReader(inBytes)

	magicmask1, _ := buff.ReadByte()
	magicmask2 := byte(0)
	if (magicmask1 & (1 << 4)) > 0 {
		magicmask2, _ = buff.ReadByte()
	}

	encoding.GetAsVarint(buff)
	encoding.GetAsUVarint(buff)
	encoding.GetAsUVarint(buff)

	action.ActionOrdinal = 1
	if (magicmask1 & (1 << 0)) == 0 {
		action.ActionOrdinal = uint32(encoding.GetAsUVarint(buff)) + 2
	}

	action.CreatorAO = action.ActionOrdinal - 1
	if (magicmask2 & (1 << 0)) == 0 {
		action.CreatorAO = uint32(encoding.GetAsUVarint(buff))
	}

	if (magicmask1 & (1 << 3)) == 0 {
		encoding.GetAsUVarint(buff)
	}

	if (magicmask1 & (1 << 2)) == 0 {
		encoding.GetAsUVarint(buff)
	}

	if (magicmask1 & (1 << 1)) == 0 {
		encoding.GetAsUVarint(buff)
	}

	action.ReceiverIndex = 0
	if (magicmask2 & (1 << 6)) == 0 {
		action.ReceiverIndex = uint32(encoding.GetAsUVarint(buff))
	}

	action.ContractNameIndex = action.ReceiverIndex
	if (magicmask1 & (1 << 7)) == 0 {
		action.ContractNameIndex = uint32(encoding.GetAsUVarint(buff))
	}

	action.ActionNameIndex = uint32(encoding.GetAsUVarint(buff))

	action.TrxIDIndex = 0
	if (magicmask2 & (1 << 2)) == 0 {
		action.TrxIDIndex = uint32(encoding.GetAsUVarint(buff))
	}

	action.DataIndex = 0
	if (magicmask2 & (1 << 3)) == 0 {
		action.DataIndex = uint32(encoding.GetAsUVarint(buff))
	}

	authsLen := uint32(1)
	if (magicmask1 & (1 << 6)) == 0 {
		authsLen = uint32(encoding.GetAsUVarint(buff) + 1)
	}

	startOffset := *authBufOffset
	for len(blob.authBuf) < startOffset+int(authsLen) {
		blob.authBuf = append(blob.authBuf, 0)
	}

	for i := uint32(0); i < authsLen; i++ {
		accountIdx := action.ContractNameIndex
		if i != 0 || ((magicmask2 & (1 << 4)) == 0) {
			accountIdx = uint32(encoding.GetAsUVarint(buff))
		}
		blob.authBuf[startOffset+int(i)] = accountIdx
		encoding.GetAsUVarint(buff)
		encoding.GetAsUVarint(buff)
	}

	action.AuthAccountIndexes = blob.authBuf[startOffset : startOffset+int(authsLen)]
	*authBufOffset = startOffset + int(authsLen)

	return action
}

// parseSyncActionDirect parses action data directly from a byteSliceReader without allocation.
func parseSyncActionDirect(r *byteSliceReader, blob *syncBlockBlob, authBufOffset *int) syncActionInfo {
	action := syncActionInfo{}

	magicmask1, _ := r.ReadByte()
	magicmask2 := byte(0)
	if (magicmask1 & (1 << 4)) > 0 {
		magicmask2, _ = r.ReadByte()
	}

	r.ReadVarint()
	r.ReadUvarint()
	r.ReadUvarint()

	action.ActionOrdinal = 1
	if (magicmask1 & (1 << 0)) == 0 {
		action.ActionOrdinal = uint32(r.ReadUvarint()) + 2
	}

	action.CreatorAO = action.ActionOrdinal - 1
	if (magicmask2 & (1 << 0)) == 0 {
		action.CreatorAO = uint32(r.ReadUvarint())
	}

	if (magicmask1 & (1 << 3)) == 0 {
		r.ReadUvarint()
	}

	if (magicmask1 & (1 << 2)) == 0 {
		r.ReadUvarint()
	}

	if (magicmask1 & (1 << 1)) == 0 {
		r.ReadUvarint()
	}

	action.ReceiverIndex = 0
	if (magicmask2 & (1 << 6)) == 0 {
		action.ReceiverIndex = uint32(r.ReadUvarint())
	}

	action.ContractNameIndex = action.ReceiverIndex
	if (magicmask1 & (1 << 7)) == 0 {
		action.ContractNameIndex = uint32(r.ReadUvarint())
	}

	action.ActionNameIndex = uint32(r.ReadUvarint())

	action.TrxIDIndex = 0
	if (magicmask2 & (1 << 2)) == 0 {
		action.TrxIDIndex = uint32(r.ReadUvarint())
	}

	action.DataIndex = 0
	if (magicmask2 & (1 << 3)) == 0 {
		action.DataIndex = uint32(r.ReadUvarint())
	}

	authsLen := uint32(1)
	if (magicmask1 & (1 << 6)) == 0 {
		authsLen = uint32(r.ReadUvarint() + 1)
	}

	startOffset := *authBufOffset
	for len(blob.authBuf) < startOffset+int(authsLen) {
		blob.authBuf = append(blob.authBuf, 0)
	}

	for i := uint32(0); i < authsLen; i++ {
		accountIdx := action.ContractNameIndex
		if i != 0 || ((magicmask2 & (1 << 4)) == 0) {
			accountIdx = uint32(r.ReadUvarint())
		}
		blob.authBuf[startOffset+int(i)] = accountIdx
		r.ReadUvarint()
		r.ReadUvarint()
	}

	action.AuthAccountIndexes = blob.authBuf[startOffset : startOffset+int(authsLen)]
	*authBufOffset = startOffset + int(authsLen)

	return action
}

func releaseSyncBlockBlob(blob *syncBlockBlob) {
	if blob != nil {
		syncBlockBlobPool.Put(blob)
	}
}

func bytesToBlockBlobBlockOnly(inBytes []byte) *blockData {
	buff := bytes.NewReader(inBytes)
	return reader2Block(buff)
}

func ExtractTransactionIDs(blockData []byte) []string {
	blob := reader2BlockTrxIDsOnly(bytes.NewReader(blockData))
	trxIDs := make([]string, len(blob.TrxIDInBlock))
	for i, trxID := range blob.TrxIDInBlock {
		trxIDs[i] = fmt.Sprintf("%x", trxID[:])
	}
	return trxIDs
}

func ExtractTransactionIDsRaw(blockData []byte) [][32]byte {
	blob := reader2BlockTrxIDsOnly(bytes.NewReader(blockData))
	return blob.TrxIDInBlock
}

// ExtractTransactionIDsFromRaw extracts transaction IDs from raw (possibly compressed) block data.
// Returns transaction IDs as raw 32-byte arrays, or nil on error.
func ExtractTransactionIDsFromRaw(rawData []byte) [][32]byte {
	var blockData []byte

	if len(rawData) >= 4 && binary.LittleEndian.Uint32(rawData[0:4]) == 0xFD2FB528 {
		decompressed, err := compression.ZstdDecompress(nil, rawData)
		if err != nil {
			return nil
		}
		blockData = decompressed
	} else {
		blockData = rawData
	}

	return extractTransactionIDsFromBytes(blockData)
}

// extractTransactionIDsFromBytes extracts transaction IDs from decompressed block bytes.
// This is a minimal parser that only reads the header and transaction IDs.
// Returns raw 32-byte transaction IDs (not hex encoded) for efficiency.
func extractTransactionIDsFromBytes(data []byte) [][32]byte {
	if len(data) < 48 {
		return nil
	}

	pos := 48

	if pos >= len(data) {
		return nil
	}
	_, varintLen := binary.Uvarint(data[pos:])
	if varintLen <= 0 {
		return nil
	}
	pos += varintLen

	if pos >= len(data) {
		return nil
	}
	txLen, varintLen := binary.Uvarint(data[pos:])
	if varintLen <= 0 {
		return nil
	}
	pos += varintLen

	if txLen > 10000 || pos+int(txLen)*32 > len(data) {
		return nil
	}

	trxIDs := make([][32]byte, txLen)
	for i := uint64(0); i < txLen; i++ {
		copy(trxIDs[i][:], data[pos:pos+32])
		pos += 32
	}

	return trxIDs
}

func reader2BlockTrxIDsOnly(buff *bytes.Reader) *blockData {
	blkData := blockData{}

	var tmp4 [4]byte
	buff.Read(tmp4[:])
	blkData.BlockNum = binary.LittleEndian.Uint32(tmp4[:])

	buff.Read(tmp4[:])
	blkData.BlockTimeUint32 = binary.LittleEndian.Uint32(tmp4[:])

	buff.Read(blkData.ProducerBlockID[:])

	var tmp8 [8]byte
	buff.Read(tmp8[:])
	blkData.MaxGlobInBlock = binary.LittleEndian.Uint64(tmp8[:])

	globCount := encoding.GetAsUVarint(buff)
	blkData.MinGlobInBlock = blkData.MaxGlobInBlock - globCount

	txLen := encoding.GetAsUVarint(buff)
	blkData.TrxIDInBlock = make([][32]byte, txLen)
	for i := uint64(0); i < txLen; i++ {
		buff.Read(blkData.TrxIDInBlock[i][:])
	}

	return &blkData
}

func bytesToBlockBlob(inBytes []byte) *blockBlob {
	const maxReasonableSize = 100_000_000 // 100M - sanity check for varint values

	buff := bytes.NewReader(inBytes)
	blob := blockBlob{}

	blob.Block = reader2Block(buff)

	catsLen := encoding.GetAsUVarint(buff)
	if catsLen > maxReasonableSize {
		panic(fmt.Sprintf("Block %d: catsLen=%d exceeds reasonable limit (corrupted data or parse error)", blob.Block.BlockNum, catsLen))
	}

	blob.CatsOffset = make([]uint32, catsLen)
	blob.CatsBytes = make([][]byte, catsLen)
	blob.Cats = make([]*compressedActionTrace, catsLen)

	for i := uint64(0); i < catsLen; i++ {
		blob.CatsOffset[i] = uint32(encoding.GetAsUVarint(buff))
	}

	catLengths := make([]uint64, catsLen)
	for i := uint64(0); i < catsLen; i++ {
		catLen := encoding.GetAsUVarint(buff)
		if catLen > maxReasonableSize {
			panic(fmt.Sprintf("Block %d: catLen=%d exceeds reasonable limit (corrupted data or parse error)", blob.Block.BlockNum, catLen))
		}
		catLengths[i] = catLen
	}

	for i := uint64(0); i < catsLen; i++ {
		blob.CatsBytes[i] = make([]byte, catLengths[i])
		buff.Read(blob.CatsBytes[i])
		blob.Cats[i] = catFromBytes(blob.CatsBytes[i])
	}

	return &blob
}

func bytesToBlockBlobLazy(inBytes []byte) *lazyBlockBlob {
	const maxReasonableSize = 100_000_000

	buff := bytes.NewReader(inBytes)
	blob := lazyBlockBlob{}

	blob.Block = reader2Block(buff)

	catsLen := encoding.GetAsUVarint(buff)
	if catsLen > maxReasonableSize {
		panic(fmt.Sprintf("Block %d: catsLen=%d exceeds reasonable limit", blob.Block.BlockNum, catsLen))
	}

	blob.CatsOffset = make([]uint32, catsLen)
	blob.CatsBytes = make([][]byte, catsLen)
	blob.GlobalSeqs = make([]uint64, catsLen)

	for i := uint64(0); i < catsLen; i++ {
		blob.CatsOffset[i] = uint32(encoding.GetAsUVarint(buff))
	}

	catLengths := make([]uint64, catsLen)
	for i := uint64(0); i < catsLen; i++ {
		catLen := encoding.GetAsUVarint(buff)
		if catLen > maxReasonableSize {
			panic(fmt.Sprintf("Block %d: catLen=%d exceeds reasonable limit", blob.Block.BlockNum, catLen))
		}
		catLengths[i] = catLen
	}

	for i := uint64(0); i < catsLen; i++ {
		blob.CatsBytes[i] = make([]byte, catLengths[i])
		buff.Read(blob.CatsBytes[i])
		blob.GlobalSeqs[i] = extractGlobalSeqOnly(blob.CatsBytes[i])
	}

	return &blob
}

type lazyBlockBlob struct {
	Block      *blockData
	CatsOffset []uint32
	CatsBytes  [][]byte
	GlobalSeqs []uint64
}

func (b *lazyBlockBlob) GetAction(i int) *compressedActionTrace {
	return catFromBytes(b.CatsBytes[i])
}

func extractGlobalSeqOnly(inBytes []byte) uint64 {
	pos := 0
	magicmask1 := inBytes[pos]
	pos++
	if (magicmask1 & (1 << 4)) > 0 {
		pos++
	}

	_, n := binary.Varint(inBytes[pos:])
	pos += n

	_, n = binary.Uvarint(inBytes[pos:])
	pos += n

	globalSeq, _ := binary.Uvarint(inBytes[pos:])
	return globalSeq
}

func reader2Block(buff *bytes.Reader) *blockData {
	const maxReasonableSize = 100_000_000 // 100M - sanity check for varint values

	blkData := blockData{}

	var tmp4 [4]byte
	buff.Read(tmp4[:])
	blkData.BlockNum = binary.LittleEndian.Uint32(tmp4[:])

	buff.Read(tmp4[:])
	time := binary.LittleEndian.Uint32(tmp4[:])
	blkData.BlockTime = chain.Uint32ToTime(time)
	blkData.BlockTimeUint32 = time

	buff.Read(blkData.ProducerBlockID[:])

	var tmp8 [8]byte
	buff.Read(tmp8[:])
	blkData.MaxGlobInBlock = binary.LittleEndian.Uint64(tmp8[:])

	globCount := encoding.GetAsUVarint(buff)
	blkData.MinGlobInBlock = blkData.MaxGlobInBlock - globCount

	txLen := encoding.GetAsUVarint(buff)
	if txLen > maxReasonableSize {
		panic(fmt.Sprintf("Block %d: txLen=%d exceeds reasonable limit (corrupted data or parse error)", blkData.BlockNum, txLen))
	}
	blkData.TrxIDInBlock = make([][32]byte, txLen)
	for i := uint64(0); i < txLen; i++ {
		buff.Read(blkData.TrxIDInBlock[i][:])
	}

	blkData.TrxMetaInBlock = make([]trxMeta, txLen)
	for i := uint64(0); i < txLen; i++ {
		meta := &blkData.TrxMetaInBlock[i]
		status, _ := buff.ReadByte()
		meta.Status = status
		buff.Read(tmp4[:])
		meta.CpuUsageUs = binary.LittleEndian.Uint32(tmp4[:])
		meta.NetUsageWords = uint32(encoding.GetAsUVarint(buff))
		buff.Read(tmp4[:])
		meta.Expiration = binary.LittleEndian.Uint32(tmp4[:])
		var tmp2 [2]byte
		buff.Read(tmp2[:])
		meta.RefBlockNum = binary.LittleEndian.Uint16(tmp2[:])
		buff.Read(tmp4[:])
		meta.RefBlockPrefix = binary.LittleEndian.Uint32(tmp4[:])
		sigCount := encoding.GetAsUVarint(buff)
		meta.Signatures = make([][]byte, sigCount)
		for j := uint64(0); j < sigCount; j++ {
			sigLen := encoding.GetAsUVarint(buff)
			meta.Signatures[j] = make([]byte, sigLen)
			buff.Read(meta.Signatures[j])
		}
	}

	dataLen := encoding.GetAsUVarint(buff)
	if dataLen > maxReasonableSize {
		panic(fmt.Sprintf("Block %d: dataLen=%d exceeds reasonable limit (corrupted data or parse error)", blkData.BlockNum, dataLen))
	}
	blkData.DataInBlock = make([][]byte, dataLen)
	for i := uint64(0); i < dataLen; i++ {
		dataIndexLen := encoding.GetAsUVarint(buff)
		if dataIndexLen > maxReasonableSize {
			panic(fmt.Sprintf("Block %d: dataIndexLen=%d exceeds reasonable limit (corrupted data or parse error)", blkData.BlockNum, dataIndexLen))
		}
		blkData.DataInBlock[i] = make([]byte, dataIndexLen)
		buff.Read(blkData.DataInBlock[i])
	}

	nameLen := encoding.GetAsUVarint(buff)
	if nameLen > maxReasonableSize {
		panic(fmt.Sprintf("Block %d: nameLen=%d exceeds reasonable limit (corrupted data or parse error)", blkData.BlockNum, nameLen))
	}
	blkData.NamesInBlock = make([]uint64, nameLen)

	for i := uint64(0); i < nameLen; i++ {
		buff.Read(tmp8[:])
		blkData.NamesInBlock[i] = binary.LittleEndian.Uint64(tmp8[:])
	}

	return &blkData
}

func (cat *compressedActionTrace) At(blkData *blockData, blockNum uint32, glob uint64) *chain.ActionTrace {
	pl := make([]chain.PermissionLevel, len(cat.Auths))
	as := make([][]interface{}, len(cat.Auths))
	ard := make([]chain.AccountDelta, len(cat.AccountRAMDeltas))

	for i := 0; i < len(cat.Auths); i++ {
		accAuth := chain.NameToString(blkData.NamesInBlock[cat.Auths[i].AccountIndex])
		accPerm := chain.NameToString(blkData.NamesInBlock[cat.Auths[i].PermissionIndex])
		pl[i] = chain.PermissionLevel{Actor: accAuth, Permission: accPerm}
		as[i] = []interface{}{accAuth, cat.Auths[i].Seq}
	}

	for i := 0; i < len(cat.AccountRAMDeltas); i++ {
		acc := chain.NameToString(blkData.NamesInBlock[cat.AccountRAMDeltas[i].AccountIndex])
		ard[i] = chain.AccountDelta{Account: acc, Delta: cat.AccountRAMDeltas[i].Delta}
	}

	rcvr := chain.NameToString(blkData.NamesInBlock[cat.ReceiverIndex])

	act := chain.Action{
		Account:       chain.NameToString(blkData.NamesInBlock[cat.ContractNameIndex]),
		Name:          chain.NameToString(blkData.NamesInBlock[cat.ActionNameIndex]),
		Authorization: pl,
		Data:          hex.EncodeToString(blkData.DataInBlock[cat.DataIndex]),
	}

	receipt := chain.ActionReceipt{
		Receiver:       rcvr,
		ActDigest:      act.Digest(),
		GlobalSequence: json.Number(strconv.FormatUint(glob, 10)),
		RecvSequence:   json.Number(strconv.FormatUint(cat.RecvSequence, 10)),
		AuthSequence:   as,
		CodeSequence:   cat.CodeSequence,
		AbiSequence:    cat.AbiSequence,
	}

	return &chain.ActionTrace{
		ActionOrdinal:    cat.ActionOrdinal,
		CreatorAO:        cat.CreatorAO,
		ClosestUAAO:      cat.ClosestUAAO,
		Receipt:          receipt,
		Receiver:         rcvr,
		Act:              act,
		ContextFree:      cat.ContextFree,
		Elapsed:          cat.Elapsed,
		TrxID:            hex.EncodeToString(blkData.TrxIDInBlock[cat.TrxIDIndex][:]),
		BlockNum:         blockNum,
		BlockTime:        blkData.BlockTime,
		ProducerBlockID:  hex.EncodeToString(blkData.ProducerBlockID[:]),
		AccountRAMDeltas: ard,
		GlobalSeqUint64:  glob,
	}
}

func (cat *compressedActionTrace) CanonicalAt(blkData *blockData, glob uint64) CanonicalAction {

	authAccountIndexes := make([]uint32, len(cat.Auths))
	for i, auth := range cat.Auths {
		authAccountIndexes[i] = auth.AccountIndex
	}

	return CanonicalAction{
		ActionOrdinal:      cat.ActionOrdinal,
		CreatorAO:          cat.CreatorAO,
		ReceiverUint64:     blkData.NamesInBlock[cat.ReceiverIndex],
		DataIndex:          cat.DataIndex,
		AuthAccountIndexes: authAccountIndexes,
		GlobalSeqUint64:    glob,
		TrxIndex:           cat.TrxIDIndex,
		ContractUint64:     blkData.NamesInBlock[cat.ContractNameIndex],
		ActionUint64:       blkData.NamesInBlock[cat.ActionNameIndex],
	}
}

func catFromBytes(inBytes []byte) *compressedActionTrace {
	cat := compressedActionTrace{}
	buff := bytes.NewReader(inBytes)

	magicmask1, _ := buff.ReadByte()
	magicmask2 := byte(0)

	if (magicmask1 & (1 << 4)) > 0 {
		magicmask2, _ = buff.ReadByte()
	}

	if (magicmask2 & (1 << 7)) > 0 {
		cat.ContextFree = true
	}

	cat.Elapsed = encoding.GetAsVarint(buff)
	cat.RecvSequence = encoding.GetAsUVarint(buff)
	cat.GlobalSequence = encoding.GetAsUVarint(buff)

	cat.ActionOrdinal = 1
	if (magicmask1 & (1 << 0)) == 0 {
		cat.ActionOrdinal = uint32(encoding.GetAsUVarint(buff)) + 2
	}

	cat.CreatorAO = cat.ActionOrdinal - 1
	if (magicmask2 & (1 << 0)) == 0 {
		cat.CreatorAO = uint32(encoding.GetAsUVarint(buff))
	}

	cat.ClosestUAAO = cat.CreatorAO
	if (magicmask1 & (1 << 3)) == 0 {
		cat.ClosestUAAO = cat.CreatorAO ^ uint32(encoding.GetAsUVarint(buff))
	}

	cat.CodeSequence = 1
	if (magicmask1 & (1 << 2)) == 0 {
		cat.CodeSequence = uint32(encoding.GetAsUVarint(buff)) + 2
	}

	cat.AbiSequence = cat.CodeSequence
	if (magicmask1 & (1 << 1)) == 0 {
		cat.AbiSequence = uint32(encoding.GetAsUVarint(buff)) ^ cat.CodeSequence
	}

	cat.ReceiverIndex = 0
	if (magicmask2 & (1 << 6)) == 0 {
		cat.ReceiverIndex = uint32(encoding.GetAsUVarint(buff))
	}

	cat.ContractNameIndex = cat.ReceiverIndex
	if (magicmask1 & (1 << 7)) == 0 {
		cat.ContractNameIndex = uint32(encoding.GetAsUVarint(buff))
	}

	cat.ActionNameIndex = uint32(encoding.GetAsUVarint(buff))

	cat.TrxIDIndex = 0
	if (magicmask2 & (1 << 2)) == 0 {
		cat.TrxIDIndex = uint32(encoding.GetAsUVarint(buff))
	}

	cat.DataIndex = 0
	if (magicmask2 & (1 << 3)) == 0 {
		cat.DataIndex = uint32(encoding.GetAsUVarint(buff))
	}

	authsLen := uint32(1)
	if (magicmask1 & (1 << 6)) == 0 {
		authsLen = uint32(encoding.GetAsUVarint(buff) + 1)
	}

	cat.Auths = make([]authTriple, authsLen)
	for i := uint32(0); i < authsLen; i++ {
		cat.Auths[i].AccountIndex = cat.ContractNameIndex
		if i != 0 || ((magicmask2 & (1 << 4)) == 0) {
			cat.Auths[i].AccountIndex = uint32(encoding.GetAsUVarint(buff))
		}
		cat.Auths[i].PermissionIndex = uint32(encoding.GetAsUVarint(buff))
		cat.Auths[i].Seq = encoding.GetAsUVarint(buff)
	}

	deltaLen := uint32(0)
	if (magicmask1 & (1 << 5)) == 0 {
		if (magicmask2 & (1 << 1)) == 0 {
			deltaLen = uint32(encoding.GetAsUVarint(buff) + 1)
		} else {
			deltaLen = 1
		}
	}

	if deltaLen > 0 {
		cat.AccountRAMDeltas = make([]accountDelta, deltaLen)
		for i := uint32(0); i < deltaLen; i++ {
			if i != 0 || ((magicmask2 & (1 << 5)) == 0) {
				cat.AccountRAMDeltas[i].AccountIndex = uint32(encoding.GetAsUVarint(buff))
			} else {
				cat.AccountRAMDeltas[i].AccountIndex = cat.Auths[0].AccountIndex
			}
			cat.AccountRAMDeltas[i].Delta = encoding.GetAsVarint(buff)
		}
	}
	return &cat
}

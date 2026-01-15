package internal

import (
	"time"

	"github.com/greymass/roborovski/libraries/corereader"
)

type BlockData struct {
	BlockNum uint32
	TrxIDs   []string
}

type BlockSource interface {
	GetNextBlock() (*BlockData, error)
	GetNextBlockWithTimeout(timeout time.Duration) (*BlockData, error)
	GetStateProps() (head, lib uint32, err error)
	Close() error
}

type PollingBlockSource struct {
	reader       corereader.Reader
	currentBlock uint32
	skipOnblock  bool
}

func NewPollingBlockSource(reader corereader.Reader, startBlock uint32, skipOnblock bool) *PollingBlockSource {
	return &PollingBlockSource{
		reader:       reader,
		currentBlock: startBlock,
		skipOnblock:  skipOnblock,
	}
}

func (p *PollingBlockSource) GetNextBlock() (*BlockData, error) {
	_, lib, err := p.reader.GetStateProps(true)
	if err != nil {
		return nil, err
	}

	if p.currentBlock > lib {
		return nil, nil
	}

	trxIDs, _, _, err := p.reader.GetTransactionIDsOnly(p.currentBlock, p.skipOnblock)
	if err != nil {
		return nil, err
	}

	block := &BlockData{
		BlockNum: p.currentBlock,
		TrxIDs:   trxIDs,
	}
	p.currentBlock++
	return block, nil
}

func (p *PollingBlockSource) GetNextBlockWithTimeout(timeout time.Duration) (*BlockData, error) {
	block, err := p.GetNextBlock()
	if err != nil {
		return nil, err
	}
	if block == nil {
		time.Sleep(timeout)
	}
	return block, nil
}

func (p *PollingBlockSource) GetStateProps() (head, lib uint32, err error) {
	return p.reader.GetStateProps(true)
}

func (p *PollingBlockSource) Close() error {
	return nil
}

var _ BlockSource = (*PollingBlockSource)(nil)

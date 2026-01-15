package corereader

import (
	"io"
	"strconv"
	"testing"

	"github.com/greymass/roborovski/libraries/logger"
)

func init() {
	logger.SetOutput(io.Discard)
}

// generateRealisticRawBlock creates a block with realistic action distribution.
func generateRealisticRawBlock(actionsPerBlock int) RawBlock {
	const (
		uniqueAccounts   = 80
		uniqueContracts  = 30
		uniqueActions    = 15
		maxActionsPerTrx = 40
		baseSeq          = uint64(100000000)
	)

	accounts := make([]uint64, uniqueAccounts)
	for i := range accounts {
		accounts[i] = uint64(1000000 + i)
	}
	contracts := make([]uint64, uniqueContracts)
	for i := range contracts {
		contracts[i] = uint64(2000000 + i)
	}
	actionNames := make([]uint64, uniqueActions)
	for i := range actionNames {
		actionNames[i] = uint64(3000000 + i)
	}

	namesInBlock := make([]uint64, 0, uniqueAccounts+uniqueContracts+uniqueActions)
	namesInBlock = append(namesInBlock, accounts...)
	namesInBlock = append(namesInBlock, contracts...)
	namesInBlock = append(namesInBlock, actionNames...)

	notifs := make(map[uint64][]uint64)
	actionMeta := make([]ActionMetadata, 0, actionsPerBlock)
	actions := make([]CanonicalAction, 0, actionsPerBlock)

	trxIndex := uint32(0)
	actionsInTrx := 0

	for i := 0; i < actionsPerBlock; i++ {
		globalSeq := baseSeq + uint64(i)
		accountIdx := i % uniqueAccounts
		contractIdx := i % uniqueContracts
		actionIdx := i % uniqueActions

		account := accounts[accountIdx]
		contract := contracts[contractIdx]
		actionName := actionNames[actionIdx]

		creatorAO := uint32(0)
		if actionsInTrx > 0 {
			creatorAO = 1
		}

		actions = append(actions, CanonicalAction{
			ActionOrdinal:      uint32(actionsInTrx + 1),
			CreatorAO:          creatorAO,
			ReceiverUint64:     account,
			DataIndex:          uint32(i),
			AuthAccountIndexes: []uint32{uint32(accountIdx)},
			GlobalSeqUint64:    globalSeq,
			TrxIndex:           trxIndex,
			ContractUint64:     contract,
			ActionUint64:       actionName,
		})

		actionMeta = append(actionMeta, ActionMetadata{
			GlobalSeq: globalSeq,
			Contract:  contract,
			Action:    actionName,
		})

		notifs[account] = append(notifs[account], globalSeq)

		actionsInTrx++
		if actionsInTrx >= maxActionsPerTrx {
			actionsInTrx = 0
			trxIndex++
		}
	}

	return RawBlock{
		BlockNum:      1000000,
		BlockTime:     500000000,
		Notifications: notifs,
		ActionMeta:    actionMeta,
		Actions:       actions,
		NamesInBlock:  namesInBlock,
	}
}

func BenchmarkFilterRawBlock(b *testing.B) {
	for _, size := range []int{330, 1000} {
		b.Run(strconv.Itoa(size), func(b *testing.B) {
			block := generateRealisticRawBlock(size)
			actionsBuf := make([]Action, 0, size)
			execBuf := make([]ContractExecution, 0, size/4)

			b.ResetTimer()
			b.ReportAllocs()

			for i := 0; i < b.N; i++ {
				_, actionsBuf, execBuf = FilterRawBlockInto(block, nil, actionsBuf, execBuf)
			}
		})
	}
}

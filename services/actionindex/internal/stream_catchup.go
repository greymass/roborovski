package internal

import (
	"context"
	"encoding/hex"

	"github.com/greymass/roborovski/libraries/chain"
	"github.com/greymass/roborovski/libraries/corereader"
	"github.com/greymass/roborovski/libraries/logger"
)

const (
	StreamCatchupBatchSize = 1000
	unlimitedLimit         = 1<<31 - 1
)

type StreamCatchup struct {
	indexes  *Indexes
	reader   corereader.BaseReader
	filter   ActionFilter
	startSeq uint64
	endSeq   uint64
}

func NewStreamCatchup(
	indexes *Indexes,
	reader corereader.BaseReader,
	filter ActionFilter,
	startSeq, endSeq uint64,
) *StreamCatchup {
	return &StreamCatchup{
		indexes:  indexes,
		reader:   reader,
		filter:   filter,
		startSeq: startSeq,
		endSeq:   endSeq,
	}
}

func (c *StreamCatchup) Run(ctx context.Context, sendAction func(StreamedAction) error) error {
	if c.canUseContractActionIndex() {
		return c.streamWithContractActionIndex(ctx, sendAction)
	}

	accounts := c.determineAccounts()
	if len(accounts) == 0 {
		logger.Printf("stream", "Catchup: no accounts determined from filter (contracts=%d, receivers=%d)",
			len(c.filter.Contracts), len(c.filter.Receivers))
		return nil
	}

	logger.Printf("stream", "Catchup: streaming %d accounts in seq range [%d, %d]", len(accounts), c.startSeq, c.endSeq)

	// For single account, stream directly without collecting all sequences first
	if len(accounts) == 1 {
		return c.streamSingleAccount(ctx, accounts[0], sendAction)
	}

	// For multiple accounts, we need to merge - fall back to collecting
	return c.streamMultipleAccounts(ctx, accounts, sendAction)
}

func (c *StreamCatchup) canUseContractActionIndex() bool {
	return len(c.filter.Contracts) == 1 && len(c.filter.Actions) > 0 && len(c.filter.Receivers) == 0
}

func (c *StreamCatchup) streamWithContractActionIndex(ctx context.Context, sendAction func(StreamedAction) error) error {
	var contract uint64
	for k := range c.filter.Contracts {
		contract = k
		break
	}

	var actions []uint64
	for k := range c.filter.Actions {
		actions = append(actions, k)
	}

	logger.Printf("stream", "Catchup: using contract+action index for %s with %d actions in seq range [%d, %d]",
		chain.NameToString(contract), len(actions), c.startSeq, c.endSeq)

	var allSeqs []uint64

	for _, action := range actions {
		seqs, err := c.indexes.chunkReader.GetContractActionWithSeqRange(
			contract, contract, action,
			c.startSeq, c.endSeq,
			unlimitedLimit, false,
		)
		if err != nil {
			return err
		}
		allSeqs = mergeAndDedupe(allSeqs, seqs)
	}

	if len(allSeqs) == 0 {
		logger.Printf("stream", "Catchup: no sequences found for contract+action filter")
		return nil
	}

	logger.Printf("stream", "Catchup: found %d sequences from contract+action index", len(allSeqs))

	var totalSent int
	for i := 0; i < len(allSeqs); i += StreamCatchupBatchSize {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		end := i + StreamCatchupBatchSize
		if end > len(allSeqs) {
			end = len(allSeqs)
		}

		sent, err := c.processBatch(allSeqs[i:end], sendAction)
		if err != nil {
			return err
		}
		totalSent += sent
	}

	logger.Printf("stream", "Catchup: sent %d actions using contract+action index", totalSent)
	return nil
}

func (c *StreamCatchup) streamSingleAccount(ctx context.Context, account uint64, sendAction func(StreamedAction) error) error {
	var totalSent int
	var batch []uint64

	// Stream through chunks, sending actions as we collect batches
	err := c.indexes.chunkReader.StreamWithSeqRange(account, c.startSeq, c.endSeq, func(seq uint64) error {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		batch = append(batch, seq)

		if len(batch) >= StreamCatchupBatchSize {
			sent, err := c.processBatch(batch, sendAction)
			if err != nil {
				return err
			}
			totalSent += sent
			batch = batch[:0]
		}
		return nil
	})

	if err != nil {
		return err
	}

	// Process remaining batch
	if len(batch) > 0 {
		sent, err := c.processBatch(batch, sendAction)
		if err != nil {
			return err
		}
		totalSent += sent
	}

	logger.Printf("stream", "Catchup: streamed %d actions for account %s", totalSent, chain.NameToString(account))
	return nil
}

func (c *StreamCatchup) streamMultipleAccounts(ctx context.Context, accounts []uint64, sendAction func(StreamedAction) error) error {
	var allSeqs []uint64
	for _, account := range accounts {
		seqs, err := c.indexes.chunkReader.GetWithSeqRange(account, c.startSeq, c.endSeq, unlimitedLimit, false)
		if err != nil {
			return err
		}
		allSeqs = mergeAndDedupe(allSeqs, seqs)
	}

	if len(allSeqs) == 0 {
		logger.Printf("stream", "Catchup: no sequences found for %d accounts", len(accounts))
		return nil
	}

	logger.Printf("stream", "Catchup: %d merged sequences from %d accounts", len(allSeqs), len(accounts))

	var totalSent int
	for i := 0; i < len(allSeqs); i += StreamCatchupBatchSize {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		end := i + StreamCatchupBatchSize
		if end > len(allSeqs) {
			end = len(allSeqs)
		}

		sent, err := c.processBatch(allSeqs[i:end], sendAction)
		if err != nil {
			return err
		}
		totalSent += sent
	}

	logger.Printf("stream", "Catchup: sent %d actions from %d accounts", totalSent, len(accounts))
	return nil
}

func (c *StreamCatchup) processBatch(seqs []uint64, sendAction func(StreamedAction) error) (int, error) {
	actions, _, err := c.reader.GetActionsByGlobalSeqs(seqs)
	if err != nil {
		if len(seqs) == 1 {
			return 0, nil
		}
		sent := 0
		for _, seq := range seqs {
			n, err := c.processBatch([]uint64{seq}, sendAction)
			if err != nil {
				return sent, err
			}
			sent += n
		}
		return sent, nil
	}

	sent := 0
	for j, at := range actions {
		action := StreamedAction{
			GlobalSeq: seqs[j],
			BlockNum:  at.BlockNum,
			BlockTime: chain.TimeToUint32(at.BlockTime),
			Contract:  chain.StringToName(at.Act.Account),
			Action:    chain.StringToName(at.Act.Name),
			Receiver:  chain.StringToName(at.Receiver),
		}

		if !c.filter.Matches(action) {
			continue
		}

		if at.Act.Data != "" {
			if actionData, err := hex.DecodeString(at.Act.Data); err == nil {
				action.ActionData = actionData
			}
		}

		if err := sendAction(action); err != nil {
			return sent, err
		}
		sent++
	}

	return sent, nil
}

func (c *StreamCatchup) determineAccounts() []uint64 {
	if len(c.filter.Receivers) == 0 {
		accounts := make([]uint64, 0, len(c.filter.Contracts))
		for contract := range c.filter.Contracts {
			accounts = append(accounts, contract)
		}
		return accounts
	}
	accounts := make([]uint64, 0, len(c.filter.Receivers))
	for receiver := range c.filter.Receivers {
		accounts = append(accounts, receiver)
	}
	return accounts
}

package internal

import (
	"container/heap"
	"context"
	"encoding/hex"

	"github.com/greymass/roborovski/libraries/corereader"
	"github.com/greymass/roborovski/libraries/chain"
	"github.com/greymass/roborovski/libraries/logger"
)

const StreamCatchupBatchSize = 1000

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
	accounts := c.determineAccounts()
	if len(accounts) == 0 {
		return nil
	}

	allSeqs, err := c.collectSeqs(accounts)
	if err != nil {
		return err
	}

	if len(allSeqs) == 0 {
		return nil
	}

	logger.Printf("stream", "Catchup: %d actions from %d accounts (seq %d to %d)",
		len(allSeqs), len(accounts), c.startSeq, c.endSeq)

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
		batchSeqs := allSeqs[i:end]

		actions, _, err := c.reader.GetActionsByGlobalSeqs(batchSeqs)
		if err != nil {
			return err
		}

		for j, at := range actions {
			action := StreamedAction{
				GlobalSeq: batchSeqs[j],
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
				return err
			}
		}
	}

	return nil
}

func (c *StreamCatchup) determineAccounts() []uint64 {
	seen := make(map[uint64]struct{})
	var accounts []uint64

	if len(c.filter.Receivers) == 0 {
		for contract := range c.filter.Contracts {
			if _, ok := seen[contract]; !ok {
				seen[contract] = struct{}{}
				accounts = append(accounts, contract)
			}
		}
	} else {
		for receiver := range c.filter.Receivers {
			if _, ok := seen[receiver]; !ok {
				seen[receiver] = struct{}{}
				accounts = append(accounts, receiver)
			}
		}
	}

	return accounts
}

func (c *StreamCatchup) collectSeqs(accounts []uint64) ([]uint64, error) {
	if len(accounts) == 1 {
		return c.indexes.chunkReader.GetWithSeqRange(accounts[0], c.startSeq, c.endSeq, 0, false)
	}

	type seqSource struct {
		seqs []uint64
		idx  int
	}

	sources := make([]*seqSource, 0, len(accounts))
	for _, account := range accounts {
		seqs, err := c.indexes.chunkReader.GetWithSeqRange(account, c.startSeq, c.endSeq, 0, false)
		if err != nil {
			return nil, err
		}
		if len(seqs) > 0 {
			sources = append(sources, &seqSource{seqs: seqs, idx: 0})
		}
	}

	if len(sources) == 0 {
		return nil, nil
	}

	if len(sources) == 1 {
		return sources[0].seqs, nil
	}

	h := &seqHeap{}
	heap.Init(h)

	for _, src := range sources {
		heap.Push(h, src)
	}

	var result []uint64
	var lastSeq uint64

	for h.Len() > 0 {
		src := heap.Pop(h).(*seqSource)
		seq := src.seqs[src.idx]

		if seq != lastSeq {
			result = append(result, seq)
			lastSeq = seq
		}

		src.idx++
		if src.idx < len(src.seqs) {
			heap.Push(h, src)
		}
	}

	return result, nil
}

type seqHeap []*struct {
	seqs []uint64
	idx  int
}

func (h seqHeap) Len() int { return len(h) }
func (h seqHeap) Less(i, j int) bool {
	return h[i].seqs[h[i].idx] < h[j].seqs[h[j].idx]
}
func (h seqHeap) Swap(i, j int) { h[i], h[j] = h[j], h[i] }

func (h *seqHeap) Push(x any) {
	*h = append(*h, x.(*struct {
		seqs []uint64
		idx  int
	}))
}

func (h *seqHeap) Pop() any {
	old := *h
	n := len(old)
	x := old[n-1]
	old[n-1] = nil
	*h = old[0 : n-1]
	return x
}

package sequencer

import (
	"context"
	"fmt"
	"math/rand"
	"sync"

	"cloud.google.com/go/spanner"
)

const ShardSequencerShardSize = 3
const ShardSequencerBatchSize = 10

type ShardSequencer struct {
	SequencerKey string
	Client       *spanner.Client
	mu           sync.Mutex
	current      int64
	max          int64
}

func (s *ShardSequencer) Next(ctx context.Context) (int64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.current != 0 && s.current < s.max {
		s.current += 1
		return s.current, nil
	}

	shardNum := int64(rand.Intn(ShardSequencerShardSize) + 1)
	_, err := s.Client.ReadWriteTransaction(ctx, func(ctx context.Context, txn *spanner.ReadWriteTransaction) error {
		var current int64
		shardKey := fmt.Sprintf("%s_%d", s.SequencerKey, shardNum)
		row, err := txn.ReadRow(ctx, "Sequences", spanner.Key{shardKey}, []string{"Id"})
		if err != nil {
			return err
		}
		if err := row.Column(0, &current); err != nil {
			return err
		}

		if current == 0 {
			s.max = shardNum * ShardSequencerBatchSize
		} else {
			s.max = current + (ShardSequencerShardSize * ShardSequencerBatchSize)
		}
		s.current = s.max - ShardSequencerBatchSize + 1

		m := spanner.Update("Sequences", []string{"Key", "Id"}, []interface{}{shardKey, s.max})
		return txn.BufferWrite([]*spanner.Mutation{m})
	})

	if err != nil {
		return 0, err
	}

	return s.current, nil
}

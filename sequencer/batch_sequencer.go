package sequencer

import (
	"context"
	"sync"

	"cloud.google.com/go/spanner"
)

const BatchSequencerBatchSize = 10

type BatchSequencer struct {
	SequencerKey string
	Client       *spanner.Client
	mu           sync.Mutex
	current      int64
	max          int64
}

func (s *BatchSequencer) Next(ctx context.Context) (int64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.current != 0 && s.current < s.max {
		s.current += 1
		return s.current, nil
	}

	_, err := s.Client.ReadWriteTransaction(ctx, func(ctx context.Context, txn *spanner.ReadWriteTransaction) error {
		var current int64
		row, err := txn.ReadRow(ctx, "Sequences", spanner.Key{s.SequencerKey}, []string{"Id"})
		if err != nil {
			return err
		}
		if err := row.Column(0, &current); err != nil {
			return err
		}

		s.current = current + 1
		s.max = current + BatchSequencerBatchSize

		m := spanner.Update("Sequences", []string{"Key", "Id"}, []interface{}{s.SequencerKey, s.max})
		return txn.BufferWrite([]*spanner.Mutation{m})
	})

	if err != nil {
		return 0, err
	}

	return s.current, nil
}

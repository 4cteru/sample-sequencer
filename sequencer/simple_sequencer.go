package sequencer

import (
	"context"

	"cloud.google.com/go/spanner"
)

type SimpleSequencer struct {
	SequencerKey string
	Client       *spanner.Client
}

func (s *SimpleSequencer) Next(ctx context.Context) (int64, error) {
	var next int64

	_, err := s.Client.ReadWriteTransaction(ctx, func(ctx context.Context, txn *spanner.ReadWriteTransaction) error {
		var current int64
		row, err := txn.ReadRow(ctx, "Sequences", spanner.Key{s.SequencerKey}, []string{"Id"})
		if err != nil {
			return err
		}
		if err := row.Column(0, &current); err != nil {
			return err
		}

		next = current + 1
		m := spanner.Update("Sequences", []string{"Key", "Id"}, []interface{}{s.SequencerKey, next})
		return txn.BufferWrite([]*spanner.Mutation{m})
	})

	return next, err
}

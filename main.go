package main

import (
	"context"
	"fmt"
	"github.com/4cteru/sample-sequencer/sequencer"
	"log"
	"os"

	"cloud.google.com/go/spanner"
)

func main() {
	ctx := context.Background()

	dbPath := os.Getenv("DB_SPANNER_PATH")
	client, err := spanner.NewClient(ctx, dbPath)
	if err != nil {
		log.Fatal(err)
	}
	defer client.Close()

	//seq := sequencer.SimpleSequencer{Client: client, SequencerKey: "simple-sequencer"}
	//seq := sequencer.BatchSequencer{Client: client, SequencerKey: "batch-sequencer"}
	seq := sequencer.ShardSequencer{Client: client, SequencerKey: "shard-sequencer"}

	for i := 0; i < 100; i++ {
		id, err := seq.Next(ctx)
		if err != nil {
			log.Fatal(err)
		}
		fmt.Println(id)
	}
}

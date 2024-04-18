package main

import (
	"consumer/streamConsumer"
	"context"
	"fmt"
	"os"
	"strconv"
	"time"
)

func main() {
	// Redis client configuration
	redisArgs := streamConsumer.RedisArgs{
		RedisHost: "localhost",
		RedisPort: 6379,
		Db:        0,
	}

	claimBatchSize := os.Getenv("CLAIM_BATCH_SIZE")
	cbs, err := strconv.ParseInt(claimBatchSize, 10, 64)
	if err != nil {
		panic(err)
	}

	pendingBatchSize := os.Getenv("PENDING_BATCH_SIZE")
	pbs, err := strconv.ParseInt(pendingBatchSize, 10, 64)
	if err != nil {
		panic(err)
	}

	batchSize := os.Getenv("MESSAGE_BATCH_SIZE")
	bs, err := strconv.ParseInt(batchSize, 10, 64)
	if err != nil {
		panic(err)
	}

	minDurationToClaim := os.Getenv("MIN_MILLISECONDS_TO_CLAIM_IDLE")
	mdtc, err := strconv.ParseInt(minDurationToClaim, 10, 64)
	if err != nil {
		panic(err)
	}

	block := os.Getenv("BLOCK")
	b, err := strconv.ParseInt(block, 10, 64)
	if err != nil {
		panic(err)
	}

	consumerArgs := streamConsumer.ConsumerArgs{
		// stream, group and consumer names
		StreamName:   os.Getenv("STREAM_NAME"),
		GroupName:    os.Getenv("GROUP_NAME"),
		ConsumerName: os.Getenv("NAME"),
		// batch of messages to new messages
		BatchSize: bs,
		// batch of messages to claim, if is nil, it will dont claim messages
		ClaimBatchSize: &cbs,
		// batch of messages to pending, if is nil, it will dont pending messages
		PendingBatchSize: &pbs,
		// time to block the connection
		Block: time.Millisecond * time.Duration(b),
		// MinDurationToClaim is the minimum time that a message must be in the pending state to be claimed
		MinDurationToClaim: time.Second * time.Duration(mdtc),
		// IdleStillMine is the time that a message is still mine after the last ack
		IdleStillMine: 0,
		// MaxTries is the maximum number of tries to wait for the stream to be created
		Tries: []int{1, 2, 3, 10, 15},
	}

	ctx := context.Background()
	// Create a new consumer
	consumerClient, err := streamConsumer.NewConsumer(ctx, redisArgs, consumerArgs)
	if err != nil {
		panic(err)
	}

	for {
		// Consume messages, get messages news, pending and claimed
		messages, err := consumerClient.Consume(ctx)
		if err != nil {
			fmt.Println(err)
		}
		// Process messages
		for _, message := range messages {
			// Check if the message is still mine
			if ok, _ := consumerClient.StillMine(ctx, message.ID); !ok {
				fmt.Println("Message", message.ID, "is not mine anymore")
				continue
			}
			fmt.Println(message.ID, message.Values)
			// Acknowledge the message
			err = consumerClient.Ack(ctx, message.ID)
			if err != nil {
				fmt.Println(err)
			}
		}
	}
}

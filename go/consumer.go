package consumer

import (
	"context"
	"fmt"
	"time"

	"github.com/elpablete/refactored-computing-machine/go/consumer/stream_consumer"
)

func main() {
	// Redis client configuration
	redisArgs := client.RedisArgs{
		RedisHost: "localhost",
		RedisPort: 6379,
		Db:        0,
	}

	var claimBatchSize int64 = 1
	var pendingBatchSize int64 = 1
	consumerArgs := stream_consumer.ConsumerArgs{
		// stream, group and consumer names
		StreamName:   "stream_name",
		GroupName:    "group_name",
		ConsumerName: "consumer_name",
		// batch of messages to new messages
		BatchSize: 1,
		// batch of messages to claim, if is nil, it will dont claim messages
		ClaimBatchSize: &claimBatchSize,
		// batch of messages to pending, if is nil, it will dont pending messages
		PendingBatchSize: &pendingBatchSize,
		// time to block the connection
		Block: time.Millisecond * 1,
		// MinDurationToClaim is the minimum time that a message must be in the pending state to be claimed
		MinDurationToClaim: time.Second * 1,
		// IdleStillMine is the time that a message is still mine after the last ack
		IdleStillMine: 0,
		// MaxTries is the maximum number of tries to wait for the stream to be created
		Tries: []int{1, 2, 3, 10, 15},
	}

	ctx := context.Background()
	// Create a new consumer
	consumerClient, err := stream_consumer.NewConsumer(ctx, redisArgs, consumerArgs)
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
			if ok, _ := client.StillMine(ctx, message.ID); !ok {
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

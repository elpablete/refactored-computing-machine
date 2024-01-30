# refactored-computing-machine

## Purpose

Test code for `redis` streams consumer to avoid multiple processing of messages by the same consumer group while keeping the ability to scale horizontally.

## Assumptions

Processing of messages is non deterministic in essence, thus the processing time for each message is random, and thus we are using *lognormal distributions*.

The system should be robust to consumers (from the point of view from the queue) or processors disappearing unexpectedly. We user random chance to simulate this.

## Setup

Environment variables are used to configure the test cases.

### Environment variables

Defined in `k8s/test_config.yaml`.

- `STREAM_NAME`: Name of the message stream to process.
- `CONSUMER_NAME`: Name of the consumer, taken from the pod name.
- `CONSUMER_GROUP_NAME`: Name of the consumer group in which we test scalability.
- `CONSUMER_MESSAGE_BATCH_SIZE`: number of messages requested by consumer on each cycle.
- `PROCESS_TIME_MEAN`: Mean parameter for the process time distribution of each message.
- `PROCESS_TIME_VARIANCE`: Variance parameter for process time distribution of each message.
- `CONSUMER_PER_CYCLE_FAILUIRE_CHANCE`: During each consumer cycle there is a chance the consumer will die.

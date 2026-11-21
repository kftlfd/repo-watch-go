# Dead-letter queue message replay

1. Inspect without changing the queue:

   ```bash
   export RABBITMQ_URL='amqp://user:pass@host:5672/'
   go run ./cmd/dlqctl -limit 50
   ```

   It does not replay or delete anything.

2. Fix the root cause before replaying. Replaying into a broken consumer just refills the DLQ.

3. Replay a batch of events

   Use `-apply` to replay events:

   ```bash
   go run ./cmd/dlqctl -apply -limit 25 [-yes]
   ```

   Only **safe** events (refer to `internal/events/replay.go`) are replayed by default, unsafe events are sent to a quarantine queue.

   Use `-allow-unsafe` to also replay unsafe events.

4. Review unsafe events in quarantine queue

   Use RabbitMQ management UI, or:

   ```bash
   rabbitmqadmin get queue=repo-watch.dlq.quarantine count=10 ackmode=ack_requeue_true
   ```

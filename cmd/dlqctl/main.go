// Command dlqctl inspects and replays messages from the shared dead-letter
// queue. It is an operator tool, not a service: run it as a one-shot job with
// RabbitMQ credentials, inspect first, then apply in small batches.
package main

import (
	"bufio"
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"maps"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/rabbitmq/amqp091-go"

	"repo-watch/internal/events"
	"repo-watch/internal/observability"
	"repo-watch/internal/rabbitmq"
)

const (
	defaultLimit          = 100
	defaultMaxReplays     = 3
	defaultPublishTimeout = 10 * time.Second
)

type config struct {
	url             string
	queue           string
	exchange        string
	quarantineQueue string
	limit           int
	apply           bool
	yes             bool
	allowUnsafe     bool
	maxReplays      int
	timeout         time.Duration
}

type result int

const (
	resultReplayed result = iota
	resultQuarantined
)

type stats struct {
	processed   int
	replayed    int
	quarantined int
}

func main() {
	cfg, err := parseFlags()
	if err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(2)
	}

	if err := run(cfg); err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}
}

func parseFlags() (config, error) {
	var cfg config

	flag.StringVar(&cfg.url, "url", os.Getenv("RABBITMQ_URL"), "RabbitMQ AMQP URL (default $RABBITMQ_URL)")
	flag.StringVar(&cfg.queue, "queue", rabbitmq.QueueDeadLetter, "dead-letter queue to read")
	flag.StringVar(&cfg.exchange, "exchange", rabbitmq.ExchangeTopic, "exchange to republish events to")
	flag.StringVar(&cfg.quarantineQueue, "quarantine-queue", rabbitmq.QueueDLQQuarantine, "queue for invalid or unsafe messages")
	flag.IntVar(&cfg.limit, "limit", defaultLimit, "maximum number of messages to process in this run")
	flag.BoolVar(&cfg.apply, "apply", false, "replay messages; without this flag dlqctl only inspects")
	flag.BoolVar(&cfg.yes, "yes", false, "skip the interactive replay confirmation")
	flag.BoolVar(&cfg.allowUnsafe, "allow-unsafe", false, "allow replay of repository.removed and email.queued")
	flag.IntVar(&cfg.maxReplays, "max-replays", defaultMaxReplays, "quarantine messages that already reached this replay count")
	flag.DurationVar(&cfg.timeout, "timeout", defaultPublishTimeout, "timeout for each publish confirm")

	flag.Usage = func() {
		out := flag.CommandLine.Output()
		_, _ = fmt.Fprintf(out, "Usage: %s [flags]\n\n", os.Args[0])
		_, _ = fmt.Fprintln(out, "Inspects or replays messages from the shared dead-letter queue.")
		_, _ = fmt.Fprintln(out, "Without -apply, messages are only printed and requeued.")
		_, _ = fmt.Fprintln(out)
		flag.PrintDefaults()
	}
	flag.Parse()

	if cfg.url == "" {
		return cfg, errors.New("missing RabbitMQ URL (use -url or RABBITMQ_URL)")
	}
	if cfg.queue == "" {
		return cfg, errors.New("missing queue")
	}
	if cfg.exchange == "" {
		return cfg, errors.New("missing exchange")
	}
	if cfg.quarantineQueue == "" {
		return cfg, errors.New("missing quarantine queue")
	}
	if cfg.quarantineQueue == cfg.queue {
		return cfg, errors.New("quarantine queue must differ from the source queue")
	}
	if cfg.limit <= 0 {
		return cfg, errors.New("limit must be greater than zero")
	}
	if cfg.maxReplays < 0 {
		return cfg, errors.New("max-replays must not be negative")
	}
	if cfg.timeout <= 0 {
		return cfg, errors.New("timeout must be greater than zero")
	}

	return cfg, nil
}

func run(cfg config) error {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	conn, err := amqp091.DialConfig(cfg.url, amqp091.Config{
		Heartbeat: 10 * time.Second,
		Locale:    "en_US",
	})
	if err != nil {
		return fmt.Errorf("connect to RabbitMQ: %w", err)
	}
	defer func() { _ = conn.Close() }()

	consumeCh, err := conn.Channel()
	if err != nil {
		return fmt.Errorf("open consumer channel: %w", err)
	}
	defer func() { _ = consumeCh.Close() }()

	pubCh, err := conn.Channel()
	if err != nil {
		return fmt.Errorf("open publisher channel: %w", err)
	}
	defer func() { _ = pubCh.Close() }()

	publisher, err := rabbitmq.NewConfirmedPublisher(pubCh)
	if err != nil {
		return err
	}
	publisher.Timeout = cfg.timeout

	if cfg.apply {
		if err := rabbitmq.DeclareQuarantineQueue(consumeCh, cfg.quarantineQueue); err != nil {
			return err
		}
		if cfg.allowUnsafe {
			fmt.Println("warning: -allow-unsafe is set; events classified unsafe by internal/events may have side effects")
		}
		if !cfg.yes {
			if !confirmReplay(cfg) {
				return errors.New("replay cancelled")
			}
		}
	}

	mode := "inspecting"
	if cfg.apply {
		mode = "replaying"
	}
	fmt.Printf("%s up to %d messages from %s\n", mode, cfg.limit, cfg.queue)

	var totals stats
	var held []amqp091.Delivery
	defer func() {
		// Inspection does not consume messages; requeue everything only after
		// the run so repeated basic.get calls return distinct messages.
		for i := range held {
			_ = held[i].Nack(false, true)
		}
		fmt.Printf(
			"processed=%d replayed=%d quarantined=%d\n",
			totals.processed,
			totals.replayed,
			totals.quarantined,
		)
	}()

	for totals.processed < cfg.limit {
		if ctx.Err() != nil {
			fmt.Println("interrupted")
			return nil
		}

		delivery, ok, err := consumeCh.Get(cfg.queue, false)
		if err != nil {
			return fmt.Errorf("get from queue %q: %w", cfg.queue, err)
		}
		if !ok {
			break
		}

		totals.processed++
		if !cfg.apply {
			previewMessage(&delivery)
			held = append(held, delivery)
			continue
		}

		outcome, err := handleDelivery(ctx, cfg, publisher, &delivery)
		if err != nil {
			_ = delivery.Nack(false, true)
			return err
		}

		switch outcome {
		case resultReplayed:
			totals.replayed++
		case resultQuarantined:
			totals.quarantined++
		}
	}

	return nil
}

func handleDelivery(
	ctx context.Context,
	cfg config,
	publisher *rabbitmq.ConfirmedPublisher,
	delivery *amqp091.Delivery,
) (result, error) {
	envelope, err := parseEnvelope(delivery.Body)
	if err != nil {
		return quarantine(ctx, cfg, publisher, delivery, "invalid-envelope: "+err.Error())
	}

	replays := rabbitmq.Int32Header(delivery.Headers, rabbitmq.ReplayCountHeader)
	policy := events.ReplayPolicyFor(envelope.EventType)

	if replays >= int32(cfg.maxReplays) {
		return quarantine(ctx, cfg, publisher, delivery, "replay-limit")
	}
	if policy.Unsafe && !cfg.allowUnsafe {
		fmt.Printf("quarantining id=%s: %s\n", envelope.EventID, policy.Reason)
		return quarantine(ctx, cfg, publisher, delivery, "unsafe-event-type")
	}

	if err := replay(ctx, cfg, publisher, delivery, envelope, replays+1); err != nil {
		return resultReplayed, err
	}
	if err := delivery.Ack(false); err != nil {
		return resultReplayed, fmt.Errorf("ack replayed message: %w", err)
	}

	fmt.Printf("replayed id=%s type=%s correlation_id=%s attempt=%d\n",
		envelope.EventID, envelope.EventType, delivery.CorrelationId, replays+1)
	return resultReplayed, nil
}

func replay(
	ctx context.Context,
	cfg config,
	publisher *rabbitmq.ConfirmedPublisher,
	delivery *amqp091.Delivery,
	envelope events.Envelope,
	attempt int32,
) error {
	headers := maps.Clone(delivery.Headers)
	if headers == nil {
		headers = make(amqp091.Table, 3)
	}

	// The original delivery may have exhausted its retry budget before it was
	// dead-lettered. A replay gets a fresh set of retry attempts; the replay
	// count still prevents an endless replay loop.
	delete(headers, rabbitmq.RetryCountHeader)

	// A replay is a new operator-initiated action, possibly days after the
	// original event. Drop the old trace context so the consumer starts a
	// fresh trace instead of extending the original trace across the delay.
	delete(headers, observability.TraceParentHeader)
	delete(headers, observability.TraceStateHeader)

	headers[rabbitmq.ReplayCountHeader] = attempt
	headers["x-replayed-from"] = cfg.queue
	headers["x-replayed-at"] = time.Now().UTC().Format(time.RFC3339)

	correlationID := delivery.CorrelationId
	if correlationID == "" {
		correlationID = envelope.CorrelationID
	}

	msg := amqp091.Publishing{
		ContentType:   "application/json",
		DeliveryMode:  amqp091.Persistent,
		MessageId:     envelope.EventID,
		Timestamp:     time.Now(),
		CorrelationId: correlationID,
		Headers:       headers,
		Body:          delivery.Body,
	}

	if err := publisher.Publish(ctx, cfg.exchange, envelope.EventType, msg); err != nil {
		return fmt.Errorf("replay %s: %w", envelope.EventID, err)
	}

	return nil
}

func previewMessage(delivery *amqp091.Delivery) {
	envelope, err := parseEnvelope(delivery.Body)
	if err != nil {
		fmt.Printf("id=%s invalid=true reason=%v\n", delivery.MessageId, err)
		return
	}

	policy := events.ReplayPolicyFor(envelope.EventType)
	unsafe := policy.Unsafe

	correlationID := delivery.CorrelationId
	if correlationID == "" {
		correlationID = envelope.CorrelationID
	}

	fmt.Printf(
		"id=%s type=%s correlation_id=%s version=%d replay=%d unsafe=%t routing_key=%s deaths=%v",
		envelope.EventID,
		envelope.EventType,
		correlationID,
		envelope.Version,
		rabbitmq.Int32Header(delivery.Headers, rabbitmq.ReplayCountHeader),
		unsafe,
		delivery.RoutingKey,
		delivery.Headers["x-death"],
	)
	if unsafe && policy.Reason != "" {
		fmt.Printf(" reason=%q", policy.Reason)
	}
	fmt.Println()
}

func quarantine(
	ctx context.Context,
	cfg config,
	publisher *rabbitmq.ConfirmedPublisher,
	delivery *amqp091.Delivery,
	reason string,
) (result, error) {
	headers := amqp091.Table{}
	maps.Copy(headers, delivery.Headers)
	headers["x-quarantine-reason"] = reason
	headers["x-quarantined-at"] = time.Now().UTC().Format(time.RFC3339)

	msg := amqp091.Publishing{
		ContentType:   "application/json",
		DeliveryMode:  amqp091.Persistent,
		MessageId:     delivery.MessageId,
		Timestamp:     time.Now(),
		CorrelationId: delivery.CorrelationId,
		Headers:       headers,
		Body:          delivery.Body,
	}

	if err := publisher.Publish(ctx, "", cfg.quarantineQueue, msg); err != nil {
		return resultQuarantined, fmt.Errorf("quarantine message: %w", err)
	}
	if err := delivery.Ack(false); err != nil {
		return resultQuarantined, fmt.Errorf("ack quarantined message: %w", err)
	}

	fmt.Printf("quarantined id=%s reason=%s\n", delivery.MessageId, reason)
	return resultQuarantined, nil
}

func parseEnvelope(body []byte) (events.Envelope, error) {
	envelope, err := events.DecodeEnvelope(body)
	if err != nil {
		return envelope, err
	}
	if err := events.Validate(envelope.EventType, envelope.Version); err != nil {
		return envelope, err
	}
	return envelope, nil
}

func confirmReplay(cfg config) bool {
	fmt.Printf(
		"Replay up to %d messages from %s? Unsafe event types will be %s. [y/N] ",
		cfg.limit,
		cfg.queue,
		unsafeAction(cfg.allowUnsafe),
	)

	line, err := bufio.NewReader(os.Stdin).ReadString('\n')
	if err != nil && !errors.Is(err, io.EOF) {
		return false
	}

	switch strings.ToLower(strings.TrimSpace(line)) {
	case "y", "yes":
		return true
	default:
		return false
	}
}

func unsafeAction(allowUnsafe bool) string {
	if allowUnsafe {
		return "replayed as well"
	}
	return "quarantined"
}

package broker

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"

	"github.com/RichardKnop/machinery/v2/brokers/errs"
	"github.com/RichardKnop/machinery/v2/brokers/iface"
	common "github.com/RichardKnop/machinery/v2/common"
	"github.com/RichardKnop/machinery/v2/config"
	"github.com/RichardKnop/machinery/v2/log"
	"github.com/RichardKnop/machinery/v2/tasks"
	"github.com/nats-io/nats.go"
)

type Broker struct {
	common.Broker
	cfg *config.Config
	js  nats.JetStreamContext

	// Queue where consumed tasks are pushed.
	tskQueue chan *nats.Msg
	// Queue where errors while processing tasks are pushed.
	errQueue chan error

	// tasksWG is used to wait on tasks processed. If consumer
	// closes it waits for waitgroup to finish before closing.
	tasksWG sync.WaitGroup

	// consumerWG is used to wait on consumer to finish closing
	// before fully closing.
	consumerWG sync.WaitGroup

	// Global context which gets cancelled when StopConsuming is called.
	gctx    context.Context
	gcancel context.CancelFunc
}

func New(bcfg *config.Config, cfg Config) (iface.Broker, error) {
	opt := []nats.Option{}

	if cfg.EnabledAuth {
		opt = append(opt, nats.UserInfo(cfg.Username, cfg.Password))
	}

	conn, err := nats.Connect(cfg.URL, opt...)
	if err != nil {
		return nil, err
	}

	js, err := conn.JetStream()
	if err != nil {
		return nil, err
	}

	gctx, gcancel := context.WithCancel(context.Background())

	return &Broker{
		cfg:     bcfg,
		js:      js,
		gctx:    gctx,
		gcancel: gcancel,
		Broker:  common.NewBroker(bcfg),
	}, nil
}

func (b *Broker) GetConfig() *config.Config {
	return b.cfg
}

func (b *Broker) GetPendingTasks(queue string) ([]*tasks.Signature, error) {
	return nil, errors.New("not implemented")
}

func (b *Broker) tskWorker(id int, taskPr iface.TaskProcessor) {
	for msg := range b.tskQueue {
		// Process the task.
		if err := b.processTask(msg.Data, taskPr); err != nil {
			b.errQueue <- err
		}
		msg.Ack()

		// Mark task as done in wait group once task is processed.
		b.tasksWG.Done()
	}
}

func (b *Broker) processTask(msg []byte, tskPr iface.TaskProcessor) error {
	// Create a machinery task from received byte messages.
	tsk := new(tasks.Signature)
	decoder := json.NewDecoder(bytes.NewReader(msg))
	decoder.UseNumber()
	if err := decoder.Decode(tsk); err != nil {
		return errs.NewErrCouldNotUnmarshalTaskSignature(msg, err)
	}
	if !b.IsTaskRegistered(tsk.Name) {
		return fmt.Errorf("task not registered - name: %v, id: %v, routing key: %v", tsk.Name, tsk.UUID, tsk.RoutingKey)
	}
	return tskPr.Process(tsk)
}

func (b *Broker) StartConsuming(cTag string, con int, tskPr iface.TaskProcessor) (bool, error) {
	// If concurrency is not defined then its set to 1.
	// At a time single message is processed.
	if con < 1 {
		con = 1
	}

	// Call brokerfactory which initialized channels for stop and retry consumer.
	b.Broker.StartConsuming(cTag, con, tskPr)
	// Initialize task queue.
	b.tskQueue = make(chan *nats.Msg, con)
	// Initialize error queue.
	b.errQueue = make(chan error, con*2)

	// Initialize go routine to log errors from task queue.
	go func() {
		for err := range b.errQueue {
			log.ERROR.Printf("error processing tasks: %v", err)
		}
	}()

	// Initialize worker pools
	for i := 0; i < con; i++ {
		go b.tskWorker(i, tskPr)
	}

	b.consumerWG.Add(1)

	_, err := b.js.QueueSubscribe(b.getTopic(tskPr), b.getTopic(tskPr), func(msg *nats.Msg) {
		// Add tasks wait group. Once message is processed its marked as done.
		// This way when consumer is closing we wait for this wait group to finish before exiting.
		b.tasksWG.Add(1)

		// Add a message to task queue which is consumed elsewhere.
		b.tskQueue <- msg
	}, nats.Durable(b.getTopic(tskPr)), nats.AckExplicit(), nats.MaxAckPending(con*2))
	if err != nil {
		return false, fmt.Errorf("error while subscribing: %v", err)
	}

	<-b.gctx.Done()
	b.consumerWG.Done()
	return b.GetRetry(), nil
}

func (b *Broker) StopConsuming() {
	b.Broker.StopConsuming()
	b.gcancel()

	// Wait for consmer group to exit.
	b.consumerWG.Wait()
	// Waiting for any tasks being processed to finish.
	b.tasksWG.Wait()
	// Close the error channel when all tasks are processed.
	close(b.errQueue)
}

func (b *Broker) Publish(ctx context.Context, task *tasks.Signature) error {
	b.Broker.AdjustRoutingKey(task)

	msg, err := json.Marshal(task)
	if err != nil {
		return fmt.Errorf("JSON Marshal Error: %v", err)
	}

	_, err = b.js.Publish(task.RoutingKey, msg)
	if err != nil {
		return fmt.Errorf("error while publishing message: %v", err)
	}

	return nil
}

// getTopic gets current kafka topics.
func (b *Broker) getTopic(tskPr iface.TaskProcessor) string {
	if tskPr.CustomQueue() != "" {
		return tskPr.CustomQueue()
	}
	return b.GetConfig().DefaultQueue
}

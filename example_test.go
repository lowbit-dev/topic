package topic_test

import (
	"context"
	"errors"
	"fmt"

	"lowbit.dev/topic"
)

// Example demonstrates the basic pub/sub workflow: create a typed topic,
// register a handler, publish an event, then unsubscribe.
func Example() {
	tp := topic.New[string]()

	cancel := tp.Subscribe(func(_ context.Context, msg string) error {
		fmt.Println("received:", msg)
		return nil
	})
	defer cancel()

	tp.Publish(context.Background(), "hello, world")
	// Output:
	// received: hello, world
}

// ExampleNew shows how to instantiate a topic for a custom event type.
func ExampleNew() {
	type OrderPlaced struct {
		ID    int
		Total float64
	}

	tp := topic.New[OrderPlaced]()

	cancel := tp.Subscribe(func(_ context.Context, o OrderPlaced) error {
		fmt.Printf("order %d: $%.2f\n", o.ID, o.Total)
		return nil
	})
	defer cancel()

	tp.Publish(context.Background(), OrderPlaced{ID: 42, Total: 19.99})
	// Output:
	// order 42: $19.99
}

// ExampleTopic_Subscribe shows that the cancel func removes only its own handler.
func ExampleTopic_Subscribe() {
	tp := topic.New[int]()

	cancel := tp.Subscribe(func(_ context.Context, v int) error {
		fmt.Println("A:", v)
		return nil
	})
	tp.Subscribe(func(_ context.Context, v int) error {
		fmt.Println("B:", v)
		return nil
	})

	tp.Publish(context.Background(), 1)
	cancel() // remove only handler A
	tp.Publish(context.Background(), 2)
	// Output:
	// A: 1
	// B: 1
	// B: 2
}

// ExampleTopic_Publish shows fanout: every subscriber receives the event in
// subscription order.
func ExampleTopic_Publish() {
	tp := topic.New[string]()

	for _, name := range []string{"alice", "bob", "carol"} {
		tp.Subscribe(func(_ context.Context, msg string) error {
			fmt.Printf("%s got: %s\n", name, msg)
			return nil
		})
	}

	tp.Publish(context.Background(), "ping")
	// Output:
	// alice got: ping
	// bob got: ping
	// carol got: ping
}

// ExampleTopic_Publish_errors shows that all handlers run even when one fails,
// and that all errors are returned as a single joined error.
func ExampleTopic_Publish_errors() {
	tp := topic.New[int]()

	tp.Subscribe(func(_ context.Context, _ int) error {
		fmt.Println("handler A ran")
		return errors.New("A failed")
	})
	tp.Subscribe(func(_ context.Context, _ int) error {
		fmt.Println("handler B ran")
		return nil
	})
	tp.Subscribe(func(_ context.Context, _ int) error {
		fmt.Println("handler C ran")
		return errors.New("C failed")
	})

	err := tp.Publish(context.Background(), 0)
	fmt.Println(err)
	// Output:
	// handler A ran
	// handler B ran
	// handler C ran
	// A failed
	// C failed
}

// ExampleTopic_WithRetry shows that a failing handler is retried up to the
// configured number of attempts before the error is returned to the caller.
func ExampleTopic_WithRetry() {
	tp := topic.New[string]()

	attempts := 0
	cancel := tp.Subscribe(
		func(_ context.Context, msg string) error {
			attempts++
			if attempts < 3 {
				return errors.New("transient error")
			}
			fmt.Println("delivered:", msg)
			return nil
		},
		tp.WithRetry(3, 0), // zero delay keeps the example instantaneous
	)
	defer cancel()

	if err := tp.Publish(context.Background(), "important event"); err != nil {
		fmt.Println("error:", err)
	}
	// Output:
	// delivered: important event
}

// ExampleTopic_WithRecovery shows that a panicking handler is converted to an
// error instead of crashing the publisher goroutine.
func ExampleTopic_WithRecovery() {
	tp := topic.New[int]()

	cancel := tp.Subscribe(
		func(_ context.Context, _ int) error {
			panic("something went wrong")
		},
		tp.WithRecovery(),
	)
	defer cancel()

	err := tp.Publish(context.Background(), 1)
	fmt.Println(err)
	// Output:
	// handler panic recovered: something went wrong
}

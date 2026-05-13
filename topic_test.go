package topic_test

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"lowbit.dev/topic"
)

var ctx = context.Background()

// --- Publish ---

func TestPublish_NoSubscribers(t *testing.T) {
	tp := topic.New[int]()
	if err := tp.Publish(ctx, 1); err != nil {
		t.Fatalf("expected nil, got %v", err)
	}
}

func TestPublish_SingleSubscriber(t *testing.T) {
	tp := topic.New[int]()
	var got int
	tp.Subscribe(func(_ context.Context, v int) error {
		got = v
		return nil
	})

	if err := tp.Publish(ctx, 42); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got != 42 {
		t.Fatalf("expected 42, got %d", got)
	}
}

func TestPublish_FanoutAllHandlersRun(t *testing.T) {
	tp := topic.New[int]()
	var calls int
	for range 5 {
		tp.Subscribe(func(_ context.Context, _ int) error {
			calls++
			return nil
		})
	}

	if err := tp.Publish(ctx, 0); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if calls != 5 {
		t.Fatalf("expected 5 calls, got %d", calls)
	}
}

func TestPublish_AllHandlersRunEvenIfOneFails(t *testing.T) {
	tp := topic.New[int]()
	var calls int
	sentinel := errors.New("boom")

	tp.Subscribe(func(_ context.Context, _ int) error { calls++; return sentinel })
	tp.Subscribe(func(_ context.Context, _ int) error { calls++; return nil })
	tp.Subscribe(func(_ context.Context, _ int) error { calls++; return sentinel })

	err := tp.Publish(ctx, 0)
	if calls != 3 {
		t.Fatalf("expected all 3 handlers to run, got %d", calls)
	}
	if err == nil {
		t.Fatal("expected joined error, got nil")
	}
	if !errors.Is(err, sentinel) {
		t.Fatalf("expected sentinel in error, got %v", err)
	}
}

func TestPublish_ErrorsAreJoined(t *testing.T) {
	tp := topic.New[int]()
	errA := errors.New("A")
	errB := errors.New("B")
	tp.Subscribe(func(_ context.Context, _ int) error { return errA })
	tp.Subscribe(func(_ context.Context, _ int) error { return errB })

	err := tp.Publish(ctx, 0)
	if !errors.Is(err, errA) {
		t.Errorf("expected errA in joined error, got %v", err)
	}
	if !errors.Is(err, errB) {
		t.Errorf("expected errB in joined error, got %v", err)
	}
}

func TestPublish_EventIsPassedCorrectly(t *testing.T) {
	type payload struct{ Value string }
	tp := topic.New[payload]()
	var received payload
	tp.Subscribe(func(_ context.Context, p payload) error {
		received = p
		return nil
	})

	tp.Publish(ctx, payload{Value: "hello"}) //nolint:errcheck
	if received.Value != "hello" {
		t.Fatalf("expected 'hello', got %q", received.Value)
	}
}

func TestPublish_ContextPassedToHandler(t *testing.T) {
	type key struct{}
	tp := topic.New[int]()
	var got any
	tp.Subscribe(func(ctx context.Context, _ int) error {
		got = ctx.Value(key{})
		return nil
	})

	ctx := context.WithValue(context.Background(), key{}, "marker")
	tp.Publish(ctx, 0) //nolint:errcheck
	if got != "marker" {
		t.Fatalf("expected context value to propagate, got %v", got)
	}
}

// --- Subscribe / Unsubscribe ---

func TestSubscribe_CancelRemovesHandler(t *testing.T) {
	tp := topic.New[int]()
	var calls int
	cancel := tp.Subscribe(func(_ context.Context, _ int) error {
		calls++
		return nil
	})

	tp.Publish(ctx, 0) //nolint:errcheck
	cancel()
	tp.Publish(ctx, 0) //nolint:errcheck

	if calls != 1 {
		t.Fatalf("expected 1 call after cancel, got %d", calls)
	}
}

func TestSubscribe_CancelIsIdempotent(t *testing.T) {
	tp := topic.New[int]()
	cancel := tp.Subscribe(func(_ context.Context, _ int) error { return nil })
	// Calling cancel multiple times must not panic.
	cancel()
	cancel()
	cancel()
}

func TestSubscribe_CancelOnlyRemovesOwnHandler(t *testing.T) {
	tp := topic.New[int]()
	var aRan, bRan bool

	cancelA := tp.Subscribe(func(_ context.Context, _ int) error { aRan = true; return nil })
	tp.Subscribe(func(_ context.Context, _ int) error { bRan = true; return nil })

	cancelA()
	tp.Publish(ctx, 0) //nolint:errcheck

	if aRan {
		t.Error("handler A should not run after cancel")
	}
	if !bRan {
		t.Error("handler B should still run")
	}
}

func TestSubscribe_DispatchOrderIsSubscriptionOrder(t *testing.T) {
	tp := topic.New[int]()
	var order []int
	tp.Subscribe(func(_ context.Context, _ int) error { order = append(order, 1); return nil })
	tp.Subscribe(func(_ context.Context, _ int) error { order = append(order, 2); return nil })
	tp.Subscribe(func(_ context.Context, _ int) error { order = append(order, 3); return nil })

	tp.Publish(ctx, 0) //nolint:errcheck

	for i, v := range order {
		if v != i+1 {
			t.Fatalf("expected order [1 2 3], got %v", order)
		}
	}
}

// --- Concurrency ---

func TestPublish_ConcurrentPublishersDoNotRace(t *testing.T) {
	tp := topic.New[int]()
	tp.Subscribe(func(_ context.Context, _ int) error { return nil })

	var wg sync.WaitGroup
	for range 100 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			tp.Publish(ctx, 1) //nolint:errcheck
		}()
	}
	wg.Wait()
}

func TestSubscribe_ConcurrentSubscribePublishDoNotRace(t *testing.T) {
	tp := topic.New[int]()
	var wg sync.WaitGroup

	for range 50 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			cancel := tp.Subscribe(func(_ context.Context, _ int) error { return nil })
			tp.Publish(ctx, 0) //nolint:errcheck
			cancel()
		}()
	}
	wg.Wait()
}

func TestSubscribe_SnapshotIsolation(t *testing.T) {
	// A handler that subscribes during its own dispatch must not affect
	// the in-flight snapshot — the newly registered handler should not
	// run for the current event.
	tp := topic.New[int]()
	var secondCalls int

	tp.Subscribe(func(_ context.Context, _ int) error {
		tp.Subscribe(func(_ context.Context, _ int) error {
			secondCalls++
			return nil
		})
		return nil
	})

	tp.Publish(ctx, 0) //nolint:errcheck

	if secondCalls != 0 {
		t.Fatalf("handler subscribed during dispatch should not run for current event, got %d calls", secondCalls)
	}
}

// --- WithRetry ---

func TestWithRetry_SucceedsFirstAttempt(t *testing.T) {
	tp := topic.New[int]()
	var calls int
	tp.Subscribe(func(_ context.Context, _ int) error {
		calls++
		return nil
	}, tp.WithRetry(3, time.Millisecond))

	if err := tp.Publish(ctx, 0); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if calls != 1 {
		t.Fatalf("expected 1 call, got %d", calls)
	}
}

func TestWithRetry_RetriesOnFailure(t *testing.T) {
	tp := topic.New[int]()
	var calls int
	tp.Subscribe(func(_ context.Context, _ int) error {
		calls++
		if calls < 3 {
			return errors.New("transient")
		}
		return nil
	}, tp.WithRetry(5, time.Millisecond))

	if err := tp.Publish(ctx, 0); err != nil {
		t.Fatalf("unexpected error after eventual success: %v", err)
	}
	if calls != 3 {
		t.Fatalf("expected 3 calls, got %d", calls)
	}
}

func TestWithRetry_ExhaustsAndReturnsLastError(t *testing.T) {
	tp := topic.New[int]()
	sentinel := errors.New("permanent")
	tp.Subscribe(func(_ context.Context, _ int) error {
		return sentinel
	}, tp.WithRetry(3, time.Millisecond))

	err := tp.Publish(ctx, 0)
	if !errors.Is(err, sentinel) {
		t.Fatalf("expected sentinel error, got %v", err)
	}
}

func TestWithRetry_RespectsContextCancellation(t *testing.T) {
	tp := topic.New[int]()
	tp.Subscribe(func(_ context.Context, _ int) error {
		return errors.New("fail")
	}, tp.WithRetry(10, 100*time.Millisecond))

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	start := time.Now()
	err := tp.Publish(ctx, 0)
	elapsed := time.Since(start)

	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("expected DeadlineExceeded, got %v", err)
	}
	// Should have exited well before all 10 retries × 100ms = 1s.
	if elapsed > 200*time.Millisecond {
		t.Fatalf("WithRetry did not respect context cancellation, took %v", elapsed)
	}
}

// --- WithRecovery ---

func TestWithRecovery_CatchesPanic(t *testing.T) {
	tp := topic.New[int]()
	tp.Subscribe(func(_ context.Context, _ int) error {
		panic("explosion")
	}, tp.WithRecovery())

	err := tp.Publish(ctx, 0)
	if err == nil {
		t.Fatal("expected error from recovered panic, got nil")
	}
	if err.Error() != "handler panic recovered: explosion" {
		t.Fatalf("unexpected error message: %v", err)
	}
}

func TestWithRecovery_DoesNotAffectSuccessfulHandler(t *testing.T) {
	tp := topic.New[int]()
	var called bool
	tp.Subscribe(func(_ context.Context, _ int) error {
		called = true
		return nil
	}, tp.WithRecovery())

	if err := tp.Publish(ctx, 0); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !called {
		t.Fatal("handler was not called")
	}
}

func TestWithRecovery_OtherHandlersRunAfterPanic(t *testing.T) {
	tp := topic.New[int]()
	var afterRan bool

	tp.Subscribe(func(_ context.Context, _ int) error {
		panic("boom")
	}, tp.WithRecovery())
	tp.Subscribe(func(_ context.Context, _ int) error {
		afterRan = true
		return nil
	})

	tp.Publish(ctx, 0) //nolint:errcheck

	if !afterRan {
		t.Fatal("handler after panicking handler should still run")
	}
}

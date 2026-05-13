// Package topic provides a zero-dependency, explicitly-typed pub/sub primitive.
//
// Publish dispatches to all subscribers (fanout). Every handler always runs;
// errors are collected and returned as a joined error.
//
// Async or buffered dispatch is intentionally not provided as a built-in — the
// caller owns that concern. A handler that needs buffering can receive events
// over a channel it manages itself:
//
//	ch := make(chan MyEvent, 64)
//	go func() {
//	    for evt := range ch {
//	        _ = processEvent(context.Background(), evt)
//	    }
//	}()
//	myTopic.Subscribe(func(ctx context.Context, evt MyEvent) error {
//	    select {
//	    case ch <- evt:
//	    default:
//	        return topic.ErrBufferFull
//	    }
//	    return nil
//	})
//
// Keeping lifetime and error visibility in the caller's hands is deliberate.
package topic

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"
)

// Handler defines the signature for event processing.
type Handler[T any] func(ctx context.Context, event T) error

// Option allows wrapping handlers for cross-cutting concerns (logging, retries, etc).
type Option[T any] func(Handler[T]) Handler[T]

// Topic is a zero-dependency, explicitly-typed event dispatcher.
//
// The hot path (Publish) is lock-free: it performs a single atomic pointer load
// and iterates over the snapshot. Writes (Subscribe, unsubscribe) pay the cost
// of a mutex and a slice copy — they are expected to be rare relative to reads.
type Topic[T any] struct {
	// snap holds a *[]entry[T] managed via copy-on-write.
	// Stored and loaded with atomic operations; never mutated in-place.
	snap atomic.Pointer[[]entry[T]]
	mu   sync.Mutex // serialises writes only
	next uint64     // monotonic subscription ID, incremented under mu
}

// entry pairs a handler with a unique ID so it can be removed individually.
type entry[T any] struct {
	id      uint64
	handler Handler[T]
}

// New creates a new instance of a typed Topic.
func New[T any]() *Topic[T] {
	return &Topic[T]{}
}

// Subscribe registers a handler for this topic and returns a cancel func that
// removes it. Call the returned func to unsubscribe; subsequent calls are no-ops.
func (t *Topic[T]) Subscribe(h Handler[T], opts ...Option[T]) (cancel func()) {
	for _, opt := range opts {
		h = opt(h)
	}

	t.mu.Lock()
	id := t.next
	t.next++
	old := t.load()
	snap := make([]entry[T], len(old)+1)
	copy(snap, old)
	snap[len(old)] = entry[T]{id: id, handler: h}
	t.store(snap)
	t.mu.Unlock()

	var once sync.Once
	return func() { once.Do(func() { t.remove(id) }) }
}

func (t *Topic[T]) remove(id uint64) {
	t.mu.Lock()
	old := t.load()
	snap := make([]entry[T], 0, len(old))
	for _, e := range old {
		if e.id != id {
			snap = append(snap, e)
		}
	}
	t.store(snap)
	t.mu.Unlock()
}

// Publish synchronously dispatches event to all registered subscribers.
// Every handler always runs (fanout). All errors are collected and returned
// as a single joined error. Returns nil if all handlers succeed.
//
// The hot path is lock-free: one atomic pointer load, then a direct iteration
// over the snapshot. Zero heap allocations when there are no errors.
func (t *Topic[T]) Publish(ctx context.Context, event T) error {
	snap := t.load()
	if len(snap) == 0 {
		return nil
	}

	var errs []error
	for _, e := range snap {
		if err := e.handler(ctx, event); err != nil {
			errs = append(errs, err)
		}
	}

	if len(errs) == 0 {
		return nil
	}
	return errors.Join(errs...)
}

// load returns the current snapshot via an atomic pointer load.
func (t *Topic[T]) load() []entry[T] {
	p := t.snap.Load()
	if p == nil {
		return nil
	}
	return *p
}

// store atomically replaces the snapshot pointer.
// Must be called with t.mu held.
func (t *Topic[T]) store(s []entry[T]) {
	t.snap.Store(&s)
}

// --- Middlewares ---

// WithRetry returns an Option that retries a failing handler up to attempts times,
// waiting delay between each attempt. Worst-case blocking time is attempts×delay.
// The delay is context-aware: cancellation during a wait returns ctx.Err() immediately.
//
// Called as a method so that T is inferred from the topic:
//
//	t.Subscribe(handler, t.WithRetry(3, time.Second))
func (t *Topic[T]) WithRetry(attempts int, delay time.Duration) Option[T] {
	return func(next Handler[T]) Handler[T] {
		return func(ctx context.Context, event T) error {
			var err error
			timer := time.NewTimer(delay)
			defer timer.Stop()

			for i := 0; i < attempts; i++ {
				if err = next(ctx, event); err == nil {
					return nil
				}
				if i < attempts-1 {
					timer.Reset(delay)
					select {
					case <-timer.C:
					case <-ctx.Done():
						return ctx.Err()
					}
				}
			}
			return err
		}
	}
}

// WithRecovery returns an Option that catches handler panics and converts them to errors,
// preventing a panicking subscriber from crashing the publisher.
//
// Called as a method so that T is inferred from the topic:
//
//	t.Subscribe(handler, t.WithRecovery())
func (t *Topic[T]) WithRecovery() Option[T] {
	return func(next Handler[T]) Handler[T] {
		return func(ctx context.Context, event T) (err error) {
			defer func() {
				if r := recover(); r != nil {
					err = fmt.Errorf("handler panic recovered: %v", r)
				}
			}()
			return next(ctx, event)
		}
	}
}

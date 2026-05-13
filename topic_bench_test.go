package topic_test

import (
	"context"
	"testing"

	"lowbit.dev/topic"
)

var sink error // prevents dead-code elimination

func noop(_ context.Context, _ struct{}) error { return nil }

// BenchmarkPublish_NoSubs measures the cost of publishing when nothing is subscribed.
// Should approach zero — just an atomic load and a nil check.
func BenchmarkPublish_NoSubs(b *testing.B) {
	t := topic.New[struct{}]()
	ctx := context.Background()
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		sink = t.Publish(ctx, struct{}{})
	}
}

// BenchmarkPublish_1Handler measures the common single-subscriber case.
func BenchmarkPublish_1Handler(b *testing.B) {
	t := topic.New[struct{}]()
	t.Subscribe(noop)
	ctx := context.Background()
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		sink = t.Publish(ctx, struct{}{})
	}
}

// BenchmarkPublish_10Handlers measures fanout to ten subscribers.
func BenchmarkPublish_10Handlers(b *testing.B) {
	t := topic.New[struct{}]()
	for range 10 {
		t.Subscribe(noop)
	}
	ctx := context.Background()
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		sink = t.Publish(ctx, struct{}{})
	}
}

// BenchmarkSubscribeCancel measures the write path: one subscribe + immediate cancel.
func BenchmarkSubscribeCancel(b *testing.B) {
	t := topic.New[struct{}]()
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		cancel := t.Subscribe(noop)
		cancel()
	}
}

// BenchmarkPublish_50Handlers measures fanout to fifty subscribers.
func BenchmarkPublish_50Handlers(b *testing.B) {
	t := topic.New[struct{}]()
	for range 50 {
		t.Subscribe(noop)
	}
	ctx := context.Background()
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		sink = t.Publish(ctx, struct{}{})
	}
}

// BenchmarkPublish_100Handlers measures fanout to one hundred subscribers.
func BenchmarkPublish_100Handlers(b *testing.B) {
	t := topic.New[struct{}]()
	for range 100 {
		t.Subscribe(noop)
	}
	ctx := context.Background()
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		sink = t.Publish(ctx, struct{}{})
	}
}

// BenchmarkPublish_Parallel measures concurrent publishing with no lock contention.
func BenchmarkPublish_Parallel(b *testing.B) {
	t := topic.New[struct{}]()
	t.Subscribe(noop)
	ctx := context.Background()
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			_ = t.Publish(ctx, struct{}{})
		}
	})
}

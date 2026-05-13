# topic

A single channel, typed pub/sub event primitive for Go. Each `Topic[T]` dispatches one event type to any number of subscribers — fully typed, no reflection, no global registry. Wire your own collection of topics and you have an event bus the compiler can reason about.

- **Fanout** — all subscribers always run; errors are collected and joined
- **Typed middleware** — `WithRetry`, `WithRecovery`, and a plain function type for your own
- **Unsubscribe** — `Subscribe` returns a cancel func; safe to call multiple times
- **Zero allocations** — lock-free publish path; only allocates when a handler errors
- **Zero dependencies**

## Install

```sh
go get lowbit.dev/topic
```

Requires Go 1.23+.

## Usage

Define your event type, create a topic, and subscribe:

```go
type OrderPaid struct {
    OrderID string
    Amount  int
}

orders := topic.New[OrderPaid]()

// Subscribe returns a cancel func. Call it to stop receiving events.
cancel := orders.Subscribe(func(ctx context.Context, evt OrderPaid) error {
    return sendEmail(ctx, evt)
})
defer cancel()

orders.Subscribe(func(ctx context.Context, evt OrderPaid) error {
    return updateLedger(ctx, evt)
})

// Publish dispatches to all subscribers. Both handlers always run.
// Errors from all handlers are collected and returned as a joined error.
if err := orders.Publish(ctx, OrderPaid{OrderID: "123", Amount: 9900}); err != nil {
    log.Println(err)
}
```

## Event bus

`Topic[T]` is a single typed channel. To build an event bus, collect topics into a struct you own and wire it explicitly at startup:

```go
type AppEvents struct {
    OrderPaid   *topic.Topic[OrderPaid]
    UserCreated *topic.Topic[UserCreated]
}

func NewAppEvents() AppEvents {
    return AppEvents{
        OrderPaid:   topic.New[OrderPaid](),
        UserCreated: topic.New[UserCreated](),
    }
}
```

Pass the struct — or individual topics — to the parts of your application that need them:

```go
func main() {
    events := NewAppEvents()

    // Wire subscribers at startup.
    cancel := events.OrderPaid.Subscribe(billing.HandleOrderPaid)
    defer cancel()

    events.UserCreated.Subscribe(mailer.SendWelcomeEmail)

    // Publishers only receive the topic they need.
    orderSvc := orders.NewService(events.OrderPaid)
    userSvc  := users.NewService(events.UserCreated)

    // ...
}
```

There is no global registry, no reflection, and no string-keyed lookups. The topology of your event system is visible in one place and enforced by the compiler.

For larger applications, group related topics and collect subscription cancels together so a service can unsubscribe from everything it owns in one call:

```go
// Group related topics by domain.
type UserTopics struct {
    Created *topic.Topic[UserCreated]
    Updated *topic.Topic[UserUpdated]
    Deleted *topic.Topic[UserDeleted]
}

// Collect cancels so the service can clean up in one place.
type UserService struct {
    cancels []func()
}

func (s *UserService) Register(t *UserTopics) {
    s.cancels = append(s.cancels,
        t.Created.Subscribe(s.onCreated),
        t.Updated.Subscribe(s.onUpdated),
        t.Deleted.Subscribe(s.onDeleted),
    )
}

func (s *UserService) Shutdown() {
    for _, c := range s.cancels {
        c()
    }
}
```

## Middleware

Options wrap a handler at subscription time. They are methods on `*Topic[T]`, so `T` is always inferred — no explicit type parameters needed at the call site:

```go
cancel := orders.Subscribe(handler,
    orders.WithRecovery(),
    orders.WithRetry(3, 500*time.Millisecond),
)
```

Options compose left-to-right. In the example above, `WithRecovery` wraps the handler first, then `WithRetry` wraps that — so panics are caught before the retry loop sees them.

### WithRetry

```go
orders.WithRetry(attempts int, delay time.Duration) Option[T]
```

Retries a failing handler up to `attempts` times, waiting `delay` between each attempt. Worst-case blocking time is `attempts × delay`. If the context is cancelled during a wait, it returns `ctx.Err()` immediately rather than sleeping to completion.

```go
orders.Subscribe(handler, orders.WithRetry(5, 200*time.Millisecond))
```

### WithRecovery

```go
orders.WithRecovery() Option[T]
```

Catches handler panics and converts them to errors, preventing a panicking subscriber from crashing the publisher. Other handlers still run.

```go
orders.Subscribe(handler, orders.WithRecovery())
```

### Custom middleware

`Option[T]` is a plain function type. Write your own and pass it alongside the built-ins:

```go
func WithLogging[T any](logger *slog.Logger) topic.Option[T] {
    return func(next topic.Handler[T]) topic.Handler[T] {
        return func(ctx context.Context, event T) error {
            err := next(ctx, event)
            logger.Info("event dispatched",
                "type", fmt.Sprintf("%T", event),
                "err", err,
            )
            return err
        }
    }
}

// Usage — custom middleware composes with built-ins naturally:
cancel := orders.Subscribe(handler,
    WithLogging[OrderPaid](logger),
    orders.WithRetry(3, time.Second),
)
```

## Buffered dispatch

Async and buffered dispatch are not built in — the caller owns that concern and its goroutine lifetime. The pattern is a channel-backed handler:

```go
ch := make(chan MyEvent, 64)

// Start the worker before subscribing.
go func() {
    for evt := range ch {
        _ = processEvent(context.Background(), evt)
    }
}()

myTopic.Subscribe(func(ctx context.Context, evt MyEvent) error {
    select {
    case ch <- evt:
        return nil
    default:
        return topic.ErrBufferFull // caller decides what to do with this
    }
})
```

`topic.ErrBufferFull` is provided as a conventional sentinel. The goroutine lifecycle — when it starts, when it stops — is your responsibility, which means it is under your control.

## Performance

Apple M1 Pro, `go test -bench . -benchmem`:

| Scenario                         | ns/op  | allocs/op |
| -------------------------------- | ------ | --------- |
| Publish, 0 subscribers           | 2.2 ns | 0         |
| Publish, 1 subscriber            | 3.7 ns | 0         |
| Publish, 10 subscribers          | 20 ns  | 0         |
| Publish, 100 subscribers         | 175 ns | 0         |
| Publish, parallel (8 goroutines) | 0.6 ns | 0         |
| Subscribe + cancel               | 124 ns | 6         |

The hot path is lock-free. `Publish` performs a single atomic pointer load and iterates the snapshot — no mutex, no allocation. Concurrent publishers never contend with each other, which is why parallel throughput exceeds single-threaded throughput.

`Publish` only allocates when a handler returns an error (lazy `[]error` append). On the happy path, allocation is zero at any subscriber count.

Writes (subscribe, unsubscribe) acquire a mutex and copy the handler slice. They are expected to be rare relative to reads — typically only at startup and shutdown.


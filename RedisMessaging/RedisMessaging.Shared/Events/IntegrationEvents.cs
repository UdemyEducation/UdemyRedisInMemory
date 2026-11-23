namespace RedisMessaging.Shared.Events;

// Tüm event'lerin ortak sözleşmesi
public interface IEvent
{
    Guid EventId { get; }
    DateTime OccurredAt { get; }
}

public abstract record EventBase : IEvent
{
    public Guid EventId { get; init; } = Guid.NewGuid();
    public DateTime OccurredAt { get; init; } = DateTime.UtcNow;
}

// Event-driven örneği: Sipariş -> Ödeme -> Stok zinciri
public record OrderCreatedEvent(string OrderId, decimal Total) : EventBase;

public record PaymentProcessedEvent(string PaymentId, string OrderId, bool Success) : EventBase;

public record OrderPaidEvent(string OrderId) : EventBase;

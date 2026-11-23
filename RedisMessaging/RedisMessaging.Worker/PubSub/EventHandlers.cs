using RedisMessaging.Shared.Events;
using RedisMessaging.Shared.PubSub;

namespace RedisMessaging.Worker.PubSub;

// Event-driven mimari örneği (EventBus = Redis Pub/Sub)
//
//  API --OrderCreated-->      Inventory: stok rezerve et
//  API --PaymentProcessed-->  Order: durumu "Paid" yap --OrderPaid--> Inventory: stok düş
//
// Servisler birbirini tanımaz, sadece event'leri bilir (Decoupled, Slayt 35).
//
// ⚠️ DİKKAT: Worker 3 replika çalışır ve Pub/Sub'da consumer group YOKTUR (Slayt 46 "Basic Consumer Model").
// Her replika event'in kopyasını alır => PaymentProcessed 3 kez işlenir, 3 adet OrderPaid yayınlanır,
// her replika da bu 3 event'i alır => stok 9 kez düşülür!
// Pub/Sub "herkese haber ver" içindir. "İşi bir kez yap" gerekiyorsa Streams + consumer group kullanılır.

public class OrderEventHandlers(EventBus eventBus, ILogger<OrderEventHandlers> logger) : BackgroundService
{
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        await eventBus.SubscribeAsync<PaymentProcessedEvent>(async @event =>
        {
            logger.LogInformation("💳 [Order] Ödeme sonucu - OrderId: {OrderId}, Success: {Success}",
                @event.OrderId, @event.Success);

            if (!@event.Success)
            {
                logger.LogWarning("📝 [Order] {OrderId} -> PaymentFailed", @event.OrderId);
                return;
            }

            logger.LogInformation("📝 [Order] {OrderId} -> Paid", @event.OrderId);
            await eventBus.PublishAsync(new OrderPaidEvent(@event.OrderId));
        });
    }
}

public class InventoryEventHandlers(EventBus eventBus, ILogger<InventoryEventHandlers> logger) : BackgroundService
{
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        await eventBus.SubscribeAsync<OrderCreatedEvent>(@event =>
        {
            logger.LogInformation("🔒 [Inventory] Stok rezerve edildi - OrderId: {OrderId}, Tutar: {Total}",
                @event.OrderId, @event.Total);
            return Task.CompletedTask;
        });

        await eventBus.SubscribeAsync<OrderPaidEvent>(@event =>
        {
            logger.LogInformation("📉 [Inventory] Stok düşüldü - OrderId: {OrderId} (EventId: {EventId})",
                @event.OrderId, @event.EventId);
            return Task.CompletedTask;
        });
    }
}

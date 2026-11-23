using RedisMessaging.Shared;
using RedisMessaging.Shared.Events;
using RedisMessaging.Shared.PubSub;
using StackExchange.Redis;

namespace RedisMessaging.Api.PubSub;

// ===================== Redis Pub/Sub - PUBLISHER tarafı =====================
// Publisher mesajı alıcıya değil KANALA gönderir; kimin dinlediğini bilmez (Slayt 35).
// Abonelerin (subscriber) hepsi RedisMessaging.Worker projesinde.
public static class PubSubEndpoints
{
    public static void MapPubSubEndpoints(this WebApplication app)
    {
        var group = app.MapGroup("api/pubsub").WithTags("1. Redis Pub/Sub");

        // PUBLISH ch message  => dönen değer: mesajı alan abone sayısı (Slayt 36: "(integer) 1")
        // Worker 2 replika çalıştığı için 'notifications' kanalına gönderilen mesajı
        // HER replika alır => fan-out (bir mesaj -> tüm dinleyiciler).
        group.MapPost("publish", async (IConnectionMultiplexer redis, string channel, string message) =>
        {
            var subscriber = redis.GetSubscriber();
            var receivedBy = await subscriber.PublishAsync(RedisChannel.Literal(channel), message);

            // receivedBy == 0 => o an dinleyen yok, mesaj KAYBOLDU (No Persistence, Slayt 46)
            return Results.Ok(new { channel, message, receivedBy, lost = receivedBy == 0 });
        }).WithSummary("PUBLISH channel message (örn: channel=notifications)");

        // PSUBSCRIBE örneği için farklı kanallara mesajlar gönderir.
        // Worker'daki PatternSubscriber: user.* | order.*.created | logs.*.error
        group.MapPost("publish-samples", async (IConnectionMultiplexer redis) =>
        {
            var subscriber = redis.GetSubscriber();
            (string Channel, string Message)[] samples =
            [
                ("user.login", "User123 logged in"),
                ("user.logout", "User123 logged out"),
                ("order.product.created", """{"orderId":1001}"""),
                ("order.service.created", """{"orderId":1002}"""),
                ("order.product.cancelled", """{"orderId":1003}"""), // hiçbir pattern'e uymaz
                ("logs.api.error", "500 Internal Server Error"),
                ("logs.payment.error", "Payment gateway timeout")
            ];

            var results = new List<object>();
            foreach (var (channel, message) in samples)
            {
                var receivedBy = await subscriber.PublishAsync(RedisChannel.Literal(channel), message);
                results.Add(new { channel, receivedBy });
            }

            return Results.Ok(results);
        }).WithSummary("Pattern subscription (PSUBSCRIBE) örnekleri için toplu yayın");

        // ---------------- Kullanım senaryoları (Slayt 38) ----------------

        // 1) Cache invalidation: veri değişti => tüm servisler local cache'ten silsin
        group.MapPost("use-cases/cache-invalidate/{key}", async (IConnectionMultiplexer redis, string key) =>
        {
            var receivedBy = await redis.GetSubscriber()
                .PublishAsync(RedisChannel.Literal(RedisNames.Channels.CacheInvalidation), key);
            return Results.Ok(new { invalidatedKey = key, receivedBy });
        }).WithSummary("Cache invalidation yayını (Worker local cache'i temizler)");

        // 2) Config change broadcast: ayar değişikliğini tüm servislere duyur
        group.MapPost("use-cases/config-changed", async (IConnectionMultiplexer redis, string name, string value) =>
        {
            var receivedBy = await redis.GetSubscriber()
                .PublishAsync(RedisChannel.Literal(RedisNames.Channels.ConfigChanged), $"{name}={value}");
            return Results.Ok(new { name, value, receivedBy });
        }).WithSummary("Config change broadcast");

        // ---------------- SSUBSCRIBE / SPUBLISH (Redis 7.0+) ----------------
        // Sharded Pub/Sub: Cluster'da mesaj sadece kanalın slot'una sahip shard'da kalır,
        // tüm cluster'a yayılmaz. Tek node'da da çalışır.
        group.MapPost("sharded/publish", async (IConnectionMultiplexer redis, string message) =>
        {
            var receivedBy = await redis.GetSubscriber()
                .PublishAsync(RedisChannel.Sharded(RedisNames.Channels.ShardedLiveScores), message);
            return Results.Ok(new { channel = RedisNames.Channels.ShardedLiveScores, receivedBy });
        }).WithSummary("SPUBLISH live.scores message");

        // ---------------- PUBSUB CHANNELS | NUMSUB | NUMPAT ----------------
        group.MapGet("channels", async (IConnectionMultiplexer redis) =>
        {
            var server = redis.GetServers().First();

            // PUBSUB CHANNELS * => aktif (en az bir abonesi olan) kanallar
            var channels = await server.SubscriptionChannelsAsync(RedisChannel.Pattern("*"));

            var result = new List<object>();
            foreach (var channel in channels)
            {
                // PUBSUB NUMSUB channel => kanalın abone sayısı
                var count = await server.SubscriptionSubscriberCountAsync(channel);
                result.Add(new { channel = channel.ToString(), subscribers = count });
            }

            // PUBSUB NUMPAT => aktif pattern aboneliği sayısı
            var patternCount = await server.SubscriptionPatternCountAsync();

            return Results.Ok(new { channels = result, patternSubscriptions = patternCount });
        }).WithSummary("PUBSUB CHANNELS / NUMSUB / NUMPAT");

        // ---------------- Event-driven örneği (EventBus) ----------------
        // Order servisi OrderCreated yayınlar -> Inventory stok ayırır
        // Payment PaymentProcessed yayınlar -> Order "Paid" yapar ve OrderPaid yayınlar -> Inventory stok düşer
        var events = app.MapGroup("api/events").WithTags("2. Pub/Sub EventBus");

        events.MapPost("order-created", async (EventBus bus, decimal total) =>
        {
            var orderId = $"ORD-{Random.Shared.Next(1000, 9999)}";
            var receivedBy = await bus.PublishAsync(new OrderCreatedEvent(orderId, total));
            return Results.Ok(new { orderId, receivedBy });
        });

        events.MapPost("payment-processed", async (EventBus bus, string orderId, bool success) =>
        {
            var receivedBy = await bus.PublishAsync(
                new PaymentProcessedEvent($"PAY-{Guid.NewGuid():N}"[..12], orderId, success));
            return Results.Ok(new { orderId, success, receivedBy });
        });
    }
}

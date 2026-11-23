namespace RedisMessaging.Shared;

// API (publisher) ve Worker (subscriber/consumer) aynı isimleri kullanmalı.
// Kanal / stream isimleri tek yerde tutulur, yazım hatası riski ortadan kalkar.
public static class RedisNames
{
    // Aspire AppHost'ta tanımlanan Redis kaynağının adı (builder.AddRedis("redis"))
    public const string ConnectionName = "redis";

    // ---------------- Pub/Sub kanalları (Slayt 36-38) ----------------
    public static class Channels
    {
        // SUBSCRIBE notifications  =>  basit fan-out örneği
        public const string Notifications = "notifications";

        // PSUBSCRIBE pattern örnekleri: user.* | order.*.created | logs.*.error
        public const string UserPattern = "user.*";
        public const string OrderCreatedPattern = "order.*.created";
        public const string ErrorLogPattern = "logs.*.error";

        // Kullanım senaryoları (Slayt 38)
        public const string CacheInvalidation = "cache.invalidate";
        public const string ConfigChanged = "config.changed";

        // SSUBSCRIBE / SPUBLISH => Sharded Pub/Sub (Redis 7.0+)
        public const string ShardedLiveScores = "live.scores";

        // EventBus => her event tipi kendi kanalına yayınlanır: events.OrderCreatedEvent
        public const string EventPrefix = "events.";
    }

    // ---------------- Streams (Slayt 40-45) ----------------
    public static class Streams
    {
        // XADD sensors * sensor-id 1234 temp 19.8  +  XREAD BLOCK ile canlı takip
        public const string Sensors = "sensors";

        // Consumer group örneği: orders stream'i, g1 grubu, birden çok consumer
        public const string Orders = "orders";
        public const string OrdersGroup = "g1";

        // Aynı stream'e ikinci bir grup: her grup TÜM mesajları alır (noAck örneği)
        public const string OrdersAnalyticsGroup = "analytics";

        // At-least-once örneği: consumer bazen çöker, XAUTOCLAIM ile kurtarılır
        public const string Payments = "payments";
        public const string PaymentsGroup = "payment-processors";
        public const string PaymentsDeadLetter = "payments:dlq";

        // XADD + MAXLEN ~ 1000 örneği
        public const string Logs = "logs";
    }
}

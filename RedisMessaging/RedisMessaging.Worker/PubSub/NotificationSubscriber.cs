using RedisMessaging.Shared;
using StackExchange.Redis;

namespace RedisMessaging.Worker.PubSub;

// SUBSCRIBE notifications
//
// Fan-out: Worker AppHost'ta 2 replika çalışır. API'den 'notifications' kanalına
// gönderilen tek mesajı İKİ replika da alır (PUBLISH => 2 döner).
// Worker durdurulursa PUBLISH => 0 döner ve mesaj kaybolur (Fire-and-forget).
public class NotificationSubscriber(IConnectionMultiplexer redis, ILogger<NotificationSubscriber> logger)
    : BackgroundService
{
    private readonly ISubscriber _subscriber = redis.GetSubscriber();
    private readonly RedisChannel _channel = RedisChannel.Literal(RedisNames.Channels.Notifications);

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        // Not: StackExchange.Redis subscribe işlemleri için arka planda AYRI bir bağlantı kullanır.
        // (RESP2'de subscribe moddaki bağlantı sadece (P|S)SUBSCRIBE, PING, QUIT... çalıştırabilir, Slayt 37)
        await _subscriber.SubscribeAsync(_channel, (channel, message) =>
        {
            logger.LogInformation("📨 [{Instance}] Mesaj alındı - Kanal: {Channel}, İçerik: {Message}",
                Environment.ProcessId, channel, message);
        });

        logger.LogInformation("✅ '{Channel}' kanalına abone olundu", RedisNames.Channels.Notifications);
    }

    public override async Task StopAsync(CancellationToken cancellationToken)
    {
        // UNSUBSCRIBE notifications
        await _subscriber.UnsubscribeAsync(_channel);
        logger.LogWarning("⚠️ '{Channel}' aboneliği iptal edildi", RedisNames.Channels.Notifications);
        await base.StopAsync(cancellationToken);
    }
}

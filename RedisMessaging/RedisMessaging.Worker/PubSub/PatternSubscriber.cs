using RedisMessaging.Shared;
using StackExchange.Redis;

namespace RedisMessaging.Worker.PubSub;

// PSUBSCRIBE pattern => tek abonelikle birden çok kanalı dinle.
// news.* => news.sports, news.tech ... (Slayt 37)
public class PatternSubscriber(IConnectionMultiplexer redis, ILogger<PatternSubscriber> logger)
    : BackgroundService
{
    private readonly ISubscriber _subscriber = redis.GetSubscriber();

    private readonly RedisChannel[] _patterns =
    [
        RedisChannel.Pattern(RedisNames.Channels.UserPattern),
        RedisChannel.Pattern(RedisNames.Channels.OrderCreatedPattern),
        RedisChannel.Pattern(RedisNames.Channels.ErrorLogPattern)
    ];

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        // user.* => user.login, user.logout, user.register ...
        await _subscriber.SubscribeAsync(_patterns[0], (channel, message) =>
            logger.LogInformation("👤 Kullanıcı olayı - Kanal: {Channel}, Mesaj: {Message}", channel, message));

        // order.*.created => order.product.created, order.service.created
        // (order.product.cancelled bu pattern'e UYMAZ)
        await _subscriber.SubscribeAsync(_patterns[1], (channel, message) =>
            logger.LogInformation("🛒 Sipariş oluşturuldu - Kanal: {Channel}, Mesaj: {Message}", channel, message));

        // logs.*.error => logs.api.error, logs.payment.error
        await _subscriber.SubscribeAsync(_patterns[2], (channel, message) =>
        {
            logger.LogError("❌ Hata logu - Kanal: {Channel}, Detay: {Message}", channel, message);

            if (channel.ToString().Contains("payment"))
                logger.LogCritical("🚨 KRİTİK: Ödeme sistemi hatası - {Message}", message);
        });

        logger.LogInformation("✅ Pattern abonelikleri aktif: {Patterns}", string.Join(", ", _patterns));
    }

    public override async Task StopAsync(CancellationToken cancellationToken)
    {
        // PUNSUBSCRIBE
        foreach (var pattern in _patterns) await _subscriber.UnsubscribeAsync(pattern);
        await base.StopAsync(cancellationToken);
    }
}

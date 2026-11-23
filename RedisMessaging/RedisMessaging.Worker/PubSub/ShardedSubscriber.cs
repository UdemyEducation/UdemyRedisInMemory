using RedisMessaging.Shared;
using StackExchange.Redis;

namespace RedisMessaging.Worker.PubSub;

// SSUBSCRIBE live.scores  (Sharded Pub/Sub, Redis 7.0+)
//
// Klasik Pub/Sub'da Redis Cluster her mesajı TÜM node'lara yayar (cluster bus trafiği).
// Sharded Pub/Sub'da kanal da bir key gibi hash slot'a atanır; mesaj sadece
// o slot'un sahibi olan shard'da kalır => cluster ölçeklendikçe daha verimli.
public class ShardedSubscriber(IConnectionMultiplexer redis, ILogger<ShardedSubscriber> logger)
    : BackgroundService
{
    private readonly ISubscriber _subscriber = redis.GetSubscriber();
    private readonly RedisChannel _channel = RedisChannel.Sharded(RedisNames.Channels.ShardedLiveScores);

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        await _subscriber.SubscribeAsync(_channel, (channel, message) =>
            logger.LogInformation("⚽ [Sharded] Kanal: {Channel}, Skor: {Message}", channel, message));

        logger.LogInformation("✅ SSUBSCRIBE '{Channel}' aktif", RedisNames.Channels.ShardedLiveScores);
    }

    public override async Task StopAsync(CancellationToken cancellationToken)
    {
        // SUNSUBSCRIBE
        await _subscriber.UnsubscribeAsync(_channel);
        await base.StopAsync(cancellationToken);
    }
}

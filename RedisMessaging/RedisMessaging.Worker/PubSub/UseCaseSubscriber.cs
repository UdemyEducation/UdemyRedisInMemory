using Microsoft.Extensions.Caching.Memory;
using RedisMessaging.Shared;
using StackExchange.Redis;

namespace RedisMessaging.Worker.PubSub;

// Pub/Sub kullanım senaryoları (Slayt 38):
// "Mesaj kaçarsa sorun değil, bir sonrakinde yakalarız" denebilen durumlar.
//
// 1) Cache invalidation: Her servis kendi local (IMemoryCache) cache'ini tutar.
//    Veri değişince bir servis 'cache.invalidate' yayınlar, TÜM replikalar silme yapar.
//    Mesaj kaçsa bile local cache'teki kısa TTL (30 sn) veriyi zaten tazeler.
// 2) Config change broadcast: ayar güncellemesini tüm servislere duyur.
public class UseCaseSubscriber(
    IConnectionMultiplexer redis,
    IMemoryCache localCache,
    ILogger<UseCaseSubscriber> logger) : BackgroundService
{
    private readonly ISubscriber _subscriber = redis.GetSubscriber();

    // Kanal => handler eşlemesi (birden çok kanala tek yerden abone ol)
    private Dictionary<string, Action<string>> Handlers => new()
    {
        [RedisNames.Channels.CacheInvalidation] = HandleCacheInvalidation,
        [RedisNames.Channels.ConfigChanged] = HandleConfigChanged
    };

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        // Demo için local cache'e örnek veriler koy
        foreach (var key in new[] { "product:1", "product:2", "product:3" })
            localCache.Set(key, $"{key} verisi", TimeSpan.FromSeconds(30));

        foreach (var (channelName, handler) in Handlers)
        {
            await _subscriber.SubscribeAsync(RedisChannel.Literal(channelName),
                (_, message) => handler(message.ToString()));

            logger.LogInformation("✅ '{Channel}' kanalına abone olundu", channelName);
        }
    }

    private void HandleCacheInvalidation(string key)
    {
        var existed = localCache.TryGetValue(key, out _);
        localCache.Remove(key);

        logger.LogInformation("🧹 [{Instance}] Local cache temizlendi: {Key} (cache'te var mıydı: {Existed})",
            Environment.ProcessId, key, existed);
    }

    private void HandleConfigChanged(string setting)
    {
        logger.LogInformation("⚙️ [{Instance}] Config güncellendi: {Setting}", Environment.ProcessId, setting);
    }

    public override async Task StopAsync(CancellationToken cancellationToken)
    {
        foreach (var channelName in Handlers.Keys)
            await _subscriber.UnsubscribeAsync(RedisChannel.Literal(channelName));

        await base.StopAsync(cancellationToken);
    }
}

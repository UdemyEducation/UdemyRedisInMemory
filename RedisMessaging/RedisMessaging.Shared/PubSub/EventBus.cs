using System.Text.Json;
using Microsoft.Extensions.Logging;
using RedisMessaging.Shared.Events;
using StackExchange.Redis;

namespace RedisMessaging.Shared.PubSub;

// Redis Pub/Sub üzerine kurulmuş basit bir EventBus.
// Her event tipi kendi kanalına yayınlanır: events.{EventTipi}
//
// DİKKAT: Pub/Sub "fire-and-forget"tır (at-most-once, Slayt 46).
// O anda dinleyen servis yoksa event KAYBOLUR. Kaybı kabul edilemeyen
// event'ler için Redis Streams (consumer group + XACK) kullanılmalıdır.
public class EventBus(IConnectionMultiplexer redis, ILogger<EventBus> logger)
{
    private readonly ISubscriber _subscriber = redis.GetSubscriber();

    public static RedisChannel ChannelFor<T>() where T : IEvent
        => RedisChannel.Literal($"{RedisNames.Channels.EventPrefix}{typeof(T).Name}");

    public async Task<long> PublishAsync<T>(T @event) where T : IEvent
    {
        var json = JsonSerializer.Serialize(@event);

        // PUBLISH => dönen değer: mesajı alan abone (client) sayısı
        var receivedBy = await _subscriber.PublishAsync(ChannelFor<T>(), json);

        logger.LogInformation("📤 Event yayınlandı: {EventType} ({EventId}) -> {Count} abone aldı",
            typeof(T).Name, @event.EventId, receivedBy);

        return receivedBy;
    }

    public async Task SubscribeAsync<T>(Func<T, Task> handler) where T : IEvent
    {
        // ChannelMessageQueue => mesajlar SIRAYLA işlenir (callback'li overload'da sıra garanti değildir)
        var queue = await _subscriber.SubscribeAsync(ChannelFor<T>());

        queue.OnMessage(async message =>
        {
            try
            {
                var @event = JsonSerializer.Deserialize<T>(message.Message.ToString());
                if (@event is not null) await handler(@event);
            }
            catch (Exception ex)
            {
                // Pub/Sub'da retry yok: hata olursa mesaj tekrar gelmez.
                logger.LogError(ex, "❌ Event işlenemedi: {EventType}", typeof(T).Name);
            }
        });

        logger.LogInformation("👂 Event dinleniyor: {EventType}", typeof(T).Name);
    }
}

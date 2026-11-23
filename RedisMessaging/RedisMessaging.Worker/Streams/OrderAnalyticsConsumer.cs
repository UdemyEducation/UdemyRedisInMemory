using RedisMessaging.Shared;
using RedisMessaging.Shared.Streams;
using StackExchange.Redis;

namespace RedisMessaging.Worker.Streams;

// Aynı stream'de İKİNCİ bir consumer group: "analytics"
//
// - Her grup stream'in TÜM mesajlarını alır (g1 ve analytics aynı mesajları bağımsız okur).
// - Grup içinde ise mesajlar consumer'lar arasında paylaştırılır.
//
// noAck: true  =>  XREADGROUP ... NOACK
//   Mesaj PEL'e EKLENMEZ, XACK gerekmez.
//   + Daha az ağ trafiği, daha hızlı
//   - Consumer işlerken çökerse mesaj KAYBOLUR (yeniden teslim edilemez)
//   Analitik/sayaç gibi ara sıra kayıp kabul edilebilen işler için uygundur.
public class OrderAnalyticsConsumer(IConnectionMultiplexer redis, ILogger<OrderAnalyticsConsumer> logger)
    : BackgroundService
{
    private const string Stream = RedisNames.Streams.Orders;
    private const string Group = RedisNames.Streams.OrdersAnalyticsGroup;

    private readonly string _consumerName = $"analytics-{Environment.MachineName}-{Environment.ProcessId}";

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        var db = redis.GetDatabase();
        await db.EnsureConsumerGroupAsync(Stream, Group);

        while (!stoppingToken.IsCancellationRequested)
        {
            StreamEntry[] entries;
            try
            {
                entries = await db.StreamReadGroupAsync(Stream, Group, _consumerName, ">",
                    count: 10, noAck: true);
            }
            catch (RedisServerException ex) when (ex.Message.Contains("NOGROUP"))
            {
                await db.EnsureConsumerGroupAsync(Stream, Group);
                continue;
            }

            if (entries.Length == 0)
            {
                await Task.Delay(500, stoppingToken);
                continue;
            }

            foreach (var entry in entries)
            {
                // Toplam ciroyu Redis'te tut (INCRBY)
                var total = await db.StringIncrementAsync("analytics:orders:total",
                    long.Parse(entry.Get("amount") ?? "0"));

                // XACK YOK => mesaj zaten otomatik onaylı (PEL'de tutulmaz)
                logger.LogInformation("📊 [analytics/NOACK] {OrderId} işlendi, toplam ciro: {Total}",
                    entry.Get("orderId"), total);
            }
        }
    }
}

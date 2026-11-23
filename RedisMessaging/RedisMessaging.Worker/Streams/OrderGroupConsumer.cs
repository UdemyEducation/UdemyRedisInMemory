using RedisMessaging.Shared;
using RedisMessaging.Shared.Streams;
using StackExchange.Redis;

namespace RedisMessaging.Worker.Streams;

// Consumer group - işi paylaşmak (Slayt 42)
//
//   XGROUP CREATE orders g1 $ MKSTREAM          => grubu oluştur
//   XREADGROUP GROUP g1 <consumer> STREAMS orders >  => yeni mesajları oku
//   XACK orders g1 <id>                          => işlendiğini onayla (PEL'den çıkar)
//
// Worker AppHost'ta 3 replika çalışır => g1 grubunda 3 consumer olur (C1, C2, C3).
// Bir mesaj gruptan SADECE BİR consumer'a gider: iş bölünür, kopyalanmaz.
// Daha fazla throughput? Replika sayısını artır, Redis yeni mesajları dağıtır.
public class OrderGroupConsumer(IConnectionMultiplexer redis, ILogger<OrderGroupConsumer> logger)
    : BackgroundService
{
    private const string Stream = RedisNames.Streams.Orders;
    private const string Group = RedisNames.Streams.OrdersGroup;

    // Consumer adı grup içinde benzersiz olmalı
    private readonly string _consumerName = $"consumer-{Environment.MachineName}-{Environment.ProcessId}";

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        var db = redis.GetDatabase();
        await db.EnsureConsumerGroupAsync(Stream, Group);

        logger.LogInformation("✅ {Consumer} '{Group}' grubuna katıldı", _consumerName, Group);

        while (!stoppingToken.IsCancellationRequested)
        {
            StreamEntry[] entries;
            try
            {
                // ">" => bu gruba daha önce hiç teslim edilmemiş YENİ mesajlar
                // noAck: false (varsayılan) => mesaj PEL'e eklenir, XACK gerekir
                // count: 1 => her seferinde tek mesaj al; böylece mesajların consumer'lar
                // arasında dağıldığı loglarda net görülür (count: 10 olsaydı ilk okuyan hepsini alırdı).
                entries = await db.StreamReadGroupAsync(Stream, Group, _consumerName, ">", count: 1);
            }
            catch (RedisServerException ex) when (ex.Message.Contains("NOGROUP"))
            {
                // Stream silindiyse grup da silinir => yeniden oluştur
                await db.EnsureConsumerGroupAsync(Stream, Group);
                continue;
            }

            if (entries.Length == 0)
            {
                // StackExchange.Redis'te BLOCK yok => kısa bir bekleme ile polling
                await Task.Delay(500, stoppingToken);
                continue;
            }

            foreach (var entry in entries)
            {
                logger.LogInformation("📦 [g1] {Consumer} => {OrderId} (id: {Id}, tutar: {Amount})",
                    _consumerName, entry.Get("orderId"), entry.Id, entry.Get("amount"));

                // ... iş mantığı (simülasyon) ...
                await Task.Delay(300, stoppingToken);

                // XACK => mesaj PEL'den silinir, "işlendi" olarak işaretlenir
                await db.StreamAcknowledgeAsync(Stream, Group, entry.Id);
            }
        }
    }
}

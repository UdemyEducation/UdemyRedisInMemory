using RedisMessaging.Shared;
using StackExchange.Redis;

namespace RedisMessaging.Worker.Streams;

// Takılı kalmış (stuck) mesajları kurtarma servisi (Slayt 42 & 44)
//
//   XPENDING payments payment-processors - + 100   => ACK bekleyenleri ve teslim sayılarını gör
//   XAUTOCLAIM payments payment-processors <recovery-consumer> 10000 0-0 COUNT 10
//       => 10 sn'den uzun süredir ACK'lenmemiş mesajları bu consumer'a devral
//
// Bir mesaj MaxDeliveries kez denenip hâlâ işlenemiyorsa "poison message"dır:
// sonsuz döngüye girmemesi için dead-letter stream'e (payments:dlq) taşınır ve ACK'lenir.
public class PendingRecoveryService(
    IConnectionMultiplexer redis,
    PaymentProcessor processor,
    ILogger<PendingRecoveryService> logger) : BackgroundService
{
    private const string Stream = RedisNames.Streams.Payments;
    private const string Group = RedisNames.Streams.PaymentsGroup;

    private const long MinIdleTimeMilliseconds = 10_000; // 10 sn'den uzun bekleyen = takılmış
    private const int MaxDeliveries = 3;

    private readonly string _recoveryConsumerName = $"recovery-{Environment.MachineName}-{Environment.ProcessId}";

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        var db = redis.GetDatabase();

        while (!stoppingToken.IsCancellationRequested)
        {
            await Task.Delay(TimeSpan.FromSeconds(15), stoppingToken);

            try
            {
                await RecoverAsync(db);
            }
            catch (RedisServerException ex) when (ex.Message.Contains("NOGROUP"))
            {
                // Stream/grup henüz yok
            }
            catch (Exception ex)
            {
                logger.LogError(ex, "Recovery servisinde hata");
            }
        }
    }

    private async Task RecoverAsync(IDatabase db)
    {
        // XPENDING (detaylı) => hangi mesaj kaç kez teslim edildi?
        var pending = await db.StreamPendingMessagesAsync(Stream, Group, 100, RedisValue.Null);
        if (pending.Length == 0) return;

        var deliveryCounts = pending.ToDictionary(p => p.MessageId.ToString(), p => p.DeliveryCount);

        // XAUTOCLAIM => idle süresi dolanları atomik olarak sahiplen.
        // Birden çok replika aynı anda çalışsa da bir mesajı sadece biri alır.
        var claimed = await db.StreamAutoClaimAsync(Stream, Group, _recoveryConsumerName,
            MinIdleTimeMilliseconds, "0-0", 10);

        if (claimed.ClaimedEntries.Length == 0) return;

        logger.LogWarning("🛟 {Count} takılı mesaj sahiplenildi ({Consumer})",
            claimed.ClaimedEntries.Length, _recoveryConsumerName);

        foreach (var entry in claimed.ClaimedEntries)
        {
            // XAUTOCLAIM teslim sayısını 1 artırır
            var deliveries = deliveryCounts.GetValueOrDefault(entry.Id.ToString()) + 1;

            if (deliveries > MaxDeliveries)
            {
                await MoveToDeadLetterAsync(db, entry, deliveries);
                continue;
            }

            logger.LogInformation("🔁 Yeniden deneniyor: {Id} ({Deliveries}. teslim)", entry.Id, deliveries);

            var result = await processor.ProcessAsync(entry, _recoveryConsumerName);
            if (result is not PaymentProcessor.Result.Crashed)
                await db.StreamAcknowledgeAsync(Stream, Group, entry.Id);
        }
    }

    private async Task MoveToDeadLetterAsync(IDatabase db, StreamEntry entry, int deliveries)
    {
        NameValueEntry[] values =
        [
            .. entry.Values,
            new("originalId", entry.Id),
            new("deliveries", deliveries),
            new("reason", $"{MaxDeliveries} denemede işlenemedi")
        ];

        // DLQ'ya yaz, sonra orijinal mesajı ACK'le (artık PEL'de beklemesin)
        await db.StreamAddAsync(RedisNames.Streams.PaymentsDeadLetter, values);
        await db.StreamAcknowledgeAsync(Stream, Group, entry.Id);

        logger.LogError("☠️ {Id} {Deliveries} denemeden sonra '{Dlq}' stream'ine taşındı",
            entry.Id, deliveries, RedisNames.Streams.PaymentsDeadLetter);
    }
}

using RedisMessaging.Shared;
using RedisMessaging.Shared.Streams;
using StackExchange.Redis;

namespace RedisMessaging.Worker.Streams;

// Message Delivery - at-least-once (Slayt 44)
//
//  1. XREADGROUP => mesaj gruptaki bir consumer'a teslim edilir
//  2. Pending (PEL) => ACK gelene kadar Pending Entries List'te tutulur
//  3. Process => iş mantığı çalışır
//  4. XACK => onaylanır, PEL'den çıkar
//
// Consumer XACK'ten ÖNCE çökerse mesaj PEL'de kalır =>
// PendingRecoveryService XPENDING ile bulur, XAUTOCLAIM ile sağlıklı bir consumer'a atar.
public class PaymentConsumer(
    IConnectionMultiplexer redis,
    PaymentProcessor processor,
    ILogger<PaymentConsumer> logger) : BackgroundService
{
    private const string Stream = RedisNames.Streams.Payments;
    private const string Group = RedisNames.Streams.PaymentsGroup;

    private readonly string _consumerName = $"payment-{Environment.MachineName}-{Environment.ProcessId}";

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
                entries = await db.StreamReadGroupAsync(Stream, Group, _consumerName, ">", count: 1);
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
                await Task.Delay(200, stoppingToken); // iş simülasyonu
                var result = await processor.ProcessAsync(entry, _consumerName);

                // Crashed => XACK YOK. Gerçekte process çökmüştür, buraya hiç gelinmez.
                if (result is PaymentProcessor.Result.Crashed) continue;

                await db.StreamAcknowledgeAsync(Stream, Group, entry.Id);
            }
        }
    }
}

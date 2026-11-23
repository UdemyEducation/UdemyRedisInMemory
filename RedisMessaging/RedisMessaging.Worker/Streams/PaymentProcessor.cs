using RedisMessaging.Shared.Streams;
using StackExchange.Redis;

namespace RedisMessaging.Worker.Streams;

// Ödeme işleme mantığı: hem PaymentConsumer hem PendingRecoveryService kullanır.
//
// At-least-once teslimatta bir mesaj BİRDEN FAZLA kez işlenebilir (Slayt 44).
// Bu yüzden consumer IDEMPOTENT olmalı: aynı ödeme iki kez tahsil edilmemeli.
// Burada Redis'te "SET processed:payment:{id} 1 NX EX 3600" ile kontrol ediyoruz.
public class PaymentProcessor(IConnectionMultiplexer redis, ILogger<PaymentProcessor> logger)
{
    public enum Result
    {
        Processed, // başarılı => XACK
        Duplicate, // daha önce işlenmiş => sadece XACK
        Crashed // consumer çöktü => XACK YOK, mesaj PEL'de kalır
    }

    public async Task<Result> ProcessAsync(StreamEntry entry, string consumerName)
    {
        var paymentId = entry.Get("paymentId")!;
        var amount = int.Parse(entry.Get("amount") ?? "0");

        // Simülasyon:
        // - Tutarı 7'nin katı olanlar "poison message": her denemede çöker => sonunda DLQ'ya gider
        // - Diğerleri %30 ihtimalle çöker => recovery servisi tekrar dener
        if (amount % 7 == 0 || Random.Shared.NextDouble() < 0.3)
        {
            logger.LogWarning("💥 {Consumer} {PaymentId} işlerken ÇÖKTÜ (id: {Id}) - XACK gönderilmedi, mesaj PEL'de",
                consumerName, paymentId, entry.Id);
            return Result.Crashed;
        }

        // Idempotency kontrolü: SET key 1 NX EX 3600
        var firstTime = await redis.GetDatabase().StringSetAsync(
            $"processed:payment:{paymentId}", consumerName, TimeSpan.FromHours(1), When.NotExists);

        if (!firstTime)
        {
            logger.LogInformation("♻️ {PaymentId} zaten işlenmiş, tekrar tahsil edilmedi (duplicate)", paymentId);
            return Result.Duplicate;
        }

        logger.LogInformation("💰 {Consumer} {PaymentId} tahsil edildi: {Amount} TL", consumerName, paymentId, amount);
        return Result.Processed;
    }
}

using System.Globalization;
using RedisMessaging.Shared;
using RedisMessaging.Shared.Streams;
using StackExchange.Redis;

namespace RedisMessaging.Api.Streams;

// ===================== Redis Streams - PRODUCER + yönetim tarafı =====================
// Stream: append-only log. Her entry'nin benzersiz, artan bir ID'si vardır:
//   1700000000000-0  =>  <unix zaman (ms)>-<sıra no>   (Slayt 41)
// Consumer'lar RedisMessaging.Worker projesinde.
public static class StreamEndpoints
{
    public static void MapStreamEndpoints(this WebApplication app)
    {
        var producer = app.MapGroup("api/streams").WithTags("3. Redis Streams - Producer");

        // XADD sensors * sensor-id 1234 temp 19.8   => "1700000000000-0"
        // Worker'daki SensorTailReader bu stream'i XREAD BLOCK ile canlı takip eder.
        producer.MapPost("sensors", async (IConnectionMultiplexer redis, int sensorId = 1234, double temp = 19.8) =>
        {
            var db = redis.GetDatabase();

            NameValueEntry[] values =
            [
                new("sensor-id", sensorId),
                new("temp", temp.ToString(CultureInfo.InvariantCulture))
            ];

            // messageId verilmezse "*" kullanılır => ID'yi Redis üretir, ID'ler hep artar.
            var id = await db.StreamAddAsync(RedisNames.Streams.Sensors, values);

            return Results.Ok(new { stream = RedisNames.Streams.Sensors, id = id.ToString() });
        }).WithSummary("XADD sensors * sensor-id 1234 temp 19.8");

        // Consumer group örneği (Slayt 42): m1..m6 mesajları
        // g1 grubundaki consumer'lar işi PAYLAŞIR (her mesaj gruptan tek consumer'a gider),
        // analytics grubu ise aynı mesajların HEPSİNİ ayrıca alır.
        producer.MapPost("orders", async (IConnectionMultiplexer redis, int count = 6) =>
        {
            var db = redis.GetDatabase();
            var ids = new List<string>();

            for (var i = 1; i <= count; i++)
            {
                NameValueEntry[] values =
                [
                    new("orderId", $"m{i}"),
                    new("amount", Random.Shared.Next(50, 500)),
                    new("createdAt", DateTime.UtcNow.ToString("O"))
                ];

                ids.Add((await db.StreamAddAsync(RedisNames.Streams.Orders, values)).ToString());
            }

            return Results.Ok(new { stream = RedisNames.Streams.Orders, ids });
        }).WithSummary("XADD orders (consumer group örneği)");

        // At-least-once örneği (Slayt 44):
        // Worker'daki PaymentConsumer bazı mesajlarda "çöker" ve XACK göndermez.
        // Mesaj PEL'de kalır -> PendingRecoveryService XAUTOCLAIM ile sahiplenip yeniden işler.
        producer.MapPost("payments", async (IConnectionMultiplexer redis, int count = 10) =>
        {
            var db = redis.GetDatabase();
            var ids = new List<string>();

            for (var i = 1; i <= count; i++)
            {
                NameValueEntry[] values =
                [
                    new("paymentId", $"PAY-{i:D3}"),
                    new("amount", Random.Shared.Next(10, 1000))
                ];

                ids.Add((await db.StreamAddAsync(RedisNames.Streams.Payments, values)).ToString());
            }

            return Results.Ok(new { stream = RedisNames.Streams.Payments, ids });
        }).WithSummary("XADD payments (at-least-once + XAUTOCLAIM örneği)");

        // XADD + MAXLEN (Slayt 43): yazarken otomatik silme
        //   XADD logs MAXLEN ~ 1000 * level info message ...
        producer.MapPost("logs", async (IConnectionMultiplexer redis, int count = 2500, bool approximate = true) =>
        {
            var db = redis.GetDatabase();

            for (var i = 1; i <= count; i++)
            {
                NameValueEntry[] values =
                [
                    new("level", i % 10 == 0 ? "error" : "info"),
                    new("message", $"Log message {i}")
                ];

                // maxLength: 1000 => sadece en yeni 1000 kayıt tutulur.
                // useApproximateMaxLength: true => "MAXLEN ~ 1000"
                //   Redis tüm node'ları (radix tree düğümü) siler, tam 1000'e kesmez; çok daha ucuzdur.
                await db.StreamAddAsync(RedisNames.Streams.Logs, values,
                    maxLength: 1000, useApproximateMaxLength: approximate);
            }

            var length = await db.StreamLengthAsync(RedisNames.Streams.Logs);

            // approximate=true  => length 1000'den biraz fazla olabilir (örn. 1050)
            // approximate=false => length tam olarak 1000
            return Results.Ok(new { written = count, approximate, length });
        }).WithSummary("XADD logs MAXLEN ~ 1000 *");

        // ---------------- Okuma & inceleme ----------------
        var read = app.MapGroup("api/streams/{stream}").WithTags("4. Redis Streams - Okuma & Yönetim");

        // XRANGE stream - + COUNT n   ("-" = ilk, "+" = son)
        read.MapGet("range", async (IConnectionMultiplexer redis, string stream,
            string from = "-", string to = "+", int count = 10) =>
        {
            var entries = await redis.GetDatabase().StreamRangeAsync(stream, from, to, count);
            return Results.Ok(entries.Select(e => new { id = e.Id.ToString(), values = e.ToDictionary() }));
        }).WithSummary("XRANGE stream - + COUNT n");

        // XREVRANGE stream + - COUNT n  => en yeniden en eskiye
        read.MapGet("revrange", async (IConnectionMultiplexer redis, string stream, int count = 10) =>
        {
            var entries = await redis.GetDatabase()
                .StreamRangeAsync(stream, "-", "+", count, Order.Descending);
            return Results.Ok(entries.Select(e => new { id = e.Id.ToString(), values = e.ToDictionary() }));
        }).WithSummary("XREVRANGE stream + - COUNT n");

        // XLEN + XINFO STREAM + XINFO GROUPS
        read.MapGet("info", async (IConnectionMultiplexer redis, string stream) =>
        {
            var db = redis.GetDatabase();
            if (!await db.KeyExistsAsync(stream)) return Results.NotFound(new { stream });

            var length = await db.StreamLengthAsync(stream); // XLEN
            var info = await db.StreamInfoAsync(stream); // XINFO STREAM
            var groups = await db.StreamGroupInfoAsync(stream); // XINFO GROUPS

            return Results.Ok(new
            {
                length,
                firstEntryId = info.FirstEntry.Id.ToString(),
                lastEntryId = info.LastEntry.Id.ToString(),
                lastGeneratedId = info.LastGeneratedId.ToString(),
                consumerGroupCount = info.ConsumerGroupCount,
                groups = groups.Select(g => new
                {
                    g.Name,
                    g.ConsumerCount,
                    pending = g.PendingMessageCount, // PEL: teslim edildi ama ACK'lenmedi
                    lastDeliveredId = g.LastDeliveredId, // grubun stream'deki konumu
                    g.Lag // gruba henüz teslim edilmemiş mesaj sayısı
                })
            });
        }).WithSummary("XLEN + XINFO STREAM + XINFO GROUPS");

        // XPENDING stream group  +  XPENDING stream group - + 10
        read.MapGet("groups/{group}/pending", async (IConnectionMultiplexer redis, string stream, string group) =>
        {
            var db = redis.GetDatabase();

            var summary = await db.StreamPendingAsync(stream, group);
            var details = await db.StreamPendingMessagesAsync(stream, group, 10, RedisValue.Null);

            return Results.Ok(new
            {
                summary.PendingMessageCount,
                lowestId = summary.LowestPendingMessageId.ToString(),
                highestId = summary.HighestPendingMessageId.ToString(),
                consumers = summary.Consumers.Select(c => new { name = c.Name.ToString(), c.PendingMessageCount }),
                messages = details.Select(m => new
                {
                    id = m.MessageId.ToString(),
                    consumer = m.ConsumerName.ToString(),
                    idleMs = m.IdleTimeInMilliseconds,
                    m.DeliveryCount // kaç kez teslim edildi (retry sayısı)
                })
            });
        }).WithSummary("XPENDING (PEL - ACK bekleyen mesajlar)");

        // ---------------- Silme / Trim (Slayt 43) ----------------

        // XDEL stream id  => tek bir entry'yi ID ile sil
        read.MapDelete("entries/{id}", async (IConnectionMultiplexer redis, string stream, string id) =>
        {
            var deleted = await redis.GetDatabase().StreamDeleteAsync(stream, [id]);
            return Results.Ok(new { stream, id, deleted });
        }).WithSummary("XDEL stream id");

        // XTRIM stream MAXLEN [~] n  => istenen anda kırp (örn. zamanlanmış temizlik job'u)
        read.MapPost("trim", async (IConnectionMultiplexer redis, string stream,
            int maxLength = 1000, bool approximate = false) =>
        {
            var removed = await redis.GetDatabase().StreamTrimAsync(stream, maxLength, approximate);
            return Results.Ok(new { stream, maxLength, approximate, removed });
        }).WithSummary("XTRIM stream MAXLEN [~] n");

        // XTRIM stream MINID <id>  => sayıya göre değil ID/zamana göre kırp (Redis 6.2+)
        // Örn. olderThanMinutes=10 => 10 dakikadan eski tüm kayıtlar silinir.
        read.MapPost("trim-by-time", async (IConnectionMultiplexer redis, string stream, int olderThanMinutes = 10) =>
        {
            // Stream ID'sinin ilk kısmı unix zaman (ms) olduğu için zamandan ID üretebiliriz.
            var minId = $"{DateTimeOffset.UtcNow.AddMinutes(-olderThanMinutes).ToUnixTimeMilliseconds()}-0";
            var removed = await redis.GetDatabase().StreamTrimByMinIdAsync(stream, minId);
            return Results.Ok(new { stream, minId, removed });
        }).WithSummary("XTRIM stream MINID <id>");

        // Dead-letter stream'i temizlemek / demoyu sıfırlamak için
        read.MapDelete("", async (IConnectionMultiplexer redis, string stream) =>
        {
            var deleted = await redis.GetDatabase().KeyDeleteAsync(stream);
            return Results.Ok(new { stream, deleted });
        }).WithSummary("DEL stream (demoyu sıfırla)");
    }
}

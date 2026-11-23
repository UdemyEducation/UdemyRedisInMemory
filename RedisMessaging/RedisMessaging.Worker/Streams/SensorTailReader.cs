using RedisMessaging.Shared;
using StackExchange.Redis;

namespace RedisMessaging.Worker.Streams;

// XREAD BLOCK 5000 STREAMS sensors $   => stream'i canlı takip et (real-time tailing, Slayt 41)
//
// - Consumer group YOK: her okuyucu (her replika) tüm mesajları görür, ACK/PEL yoktur.
// - "$" => "şu andan sonra gelen" mesajlar. Sonraki okumalarda son görülen ID'den devam edilir,
//   yoksa iki okuma arasında gelen mesajlar kaçar.
//
// ÖNEMLİ: StackExchange.Redis tek bir bağlantıyı (multiplexer) tüm komutlarla paylaşır.
// BLOCK'lu bir komut o bağlantıyı kilitler ve DİĞER TÜM komutları bekletir. Bu yüzden
// kütüphanenin StreamReadAsync metodunda BLOCK parametresi yoktur.
// Çözüm: blocking okuma için SADECE bu servise ait ayrı bir ConnectionMultiplexer açmak.
public class SensorTailReader(IConfiguration configuration, ILogger<SensorTailReader> logger) : BackgroundService
{
    private const int BlockMilliseconds = 5000;

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        var options = ConfigurationOptions.Parse(configuration.GetConnectionString(RedisNames.ConnectionName)!);
        options.ClientName = "sensor-tail-reader";
        options.AsyncTimeout = BlockMilliseconds * 2; // timeout, BLOCK süresinden uzun olmalı

        await using var blockingConnection = await ConnectionMultiplexer.ConnectAsync(options);
        var db = blockingConnection.GetDatabase();

        var lastId = "$";
        logger.LogInformation("👀 '{Stream}' XREAD BLOCK ile dinleniyor...", RedisNames.Streams.Sensors);

        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                // XREAD COUNT 10 BLOCK 5000 STREAMS sensors <lastId>
                var result = await db.ExecuteAsync("XREAD",
                    "COUNT", 10, "BLOCK", BlockMilliseconds, "STREAMS", RedisNames.Streams.Sensors, lastId);

                // 5 sn içinde yeni mesaj gelmezse Redis null (nil) döner => tekrar bekle
                if (result.IsNull) continue;

                // Cevap formatı: [ [streamAdı, [ [id, [field, value, ...]], ... ] ] ]
                foreach (var stream in (RedisResult[])result!)
                {
                    var entries = (RedisResult[])((RedisResult[])stream!)[1]!;

                    foreach (var entry in entries)
                    {
                        var parts = (RedisResult[])entry!;
                        var id = parts[0].ToString();
                        var fields = (RedisValue[])parts[1]!;

                        var pairs = fields.Chunk(2).Select(p => $"{p[0]}={p[1]}");
                        logger.LogInformation("🌡️ [XREAD] {Id}  {Fields}", id, string.Join("  ", pairs));

                        lastId = id; // bir sonraki XREAD bu ID'den SONRAKİLERİ getirir
                    }
                }
            }
            catch (Exception ex) when (!stoppingToken.IsCancellationRequested)
            {
                logger.LogError(ex, "XREAD hatası, 2 sn sonra tekrar denenecek");
                await Task.Delay(2000, stoppingToken);
            }
        }
    }
}

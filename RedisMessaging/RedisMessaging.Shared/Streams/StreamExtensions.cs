using StackExchange.Redis;

namespace RedisMessaging.Shared.Streams;

public static class StreamExtensions
{
    // Bir stream entry'si küçük bir hash gibidir: field-value çiftleri (Slayt 41)
    public static string? Get(this StreamEntry entry, string field)
    {
        var value = entry[field];
        return value.IsNull ? null : value.ToString();
    }

    public static Dictionary<string, string> ToDictionary(this StreamEntry entry)
        => entry.Values.ToDictionary(v => v.Name.ToString(), v => v.Value.ToString());

    // XGROUP CREATE <stream> <group> <position> MKSTREAM
    // Grup zaten varsa Redis "BUSYGROUP" hatası döner, bu normaldir.
    public static async Task EnsureConsumerGroupAsync(this IDatabase db, string stream, string group,
        string position = "$")
    {
        try
        {
            // position "$"  => sadece grup oluşturulduktan SONRA gelen mesajlar
            // position "0-0" => stream'in başındaki eski mesajlar dahil hepsi
            // createStream: true => MKSTREAM (stream yoksa boş olarak oluştur)
            await db.StreamCreateConsumerGroupAsync(stream, group, position, createStream: true);
        }
        catch (RedisServerException ex) when (ex.Message.Contains("BUSYGROUP"))
        {
            // Grup zaten mevcut
        }
    }
}

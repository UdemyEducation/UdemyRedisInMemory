var builder = DistributedApplication.CreateBuilder(args);

// Redis container (Pub/Sub + Streams). RedisInsight ile stream'leri/kanalları görsel olarak izleyebilirsiniz.
var redis = builder.AddRedis("redis")
    .WithRedisInsight();

// Publisher / Producer: Pub/Sub yayınları ve Stream XADD/XRANGE/XTRIM endpoint'leri (Scalar UI)
builder.AddDotnetProject("api", "../RedisMessaging.Api/RedisMessaging.Api.csproj")
    .WithReference(redis)
    .WaitFor(redis)
    .WithExternalHttpEndpoints();

// Subscriber / Consumer: 3 replika
//  - Pub/Sub  => her replika mesajın KOPYASINI alır (fan-out, PUBLISH 3 döner)
//  - Streams  => g1 grubunda 3 consumer olur, mesajlar aralarında PAYLAŞTIRILIR
builder.AddDotnetProject("worker", "../RedisMessaging.Worker/RedisMessaging.Worker.csproj")
    .WithReference(redis)
    .WaitFor(redis)
    .WithReplicas(3);

builder.Build().Run();

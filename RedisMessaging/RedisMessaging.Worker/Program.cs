using RedisMessaging.Shared;
using RedisMessaging.Shared.PubSub;
using RedisMessaging.Worker.PubSub;
using RedisMessaging.Worker.Streams;

var builder = Host.CreateApplicationBuilder(args);

builder.AddServiceDefaults();

// AppHost'taki "redis" kaynağına bağlanır, IConnectionMultiplexer'ı DI'a ekler.
builder.AddRedisClient(RedisNames.ConnectionName);

builder.Services.AddMemoryCache();
builder.Services.AddSingleton<EventBus>();

// ---------------- Redis Pub/Sub (subscriber'lar) ----------------
builder.Services.AddHostedService<NotificationSubscriber>(); // SUBSCRIBE notifications
builder.Services.AddHostedService<PatternSubscriber>(); // PSUBSCRIBE user.* | order.*.created | logs.*.error
builder.Services.AddHostedService<UseCaseSubscriber>(); // cache invalidation + config change
builder.Services.AddHostedService<ShardedSubscriber>(); // SSUBSCRIBE live.scores
builder.Services.AddHostedService<OrderEventHandlers>(); // EventBus: Order servisi
builder.Services.AddHostedService<InventoryEventHandlers>(); // EventBus: Inventory servisi

// ---------------- Redis Streams (consumer'lar) ----------------
builder.Services.AddSingleton<PaymentProcessor>();
builder.Services.AddHostedService<SensorTailReader>(); // XREAD BLOCK (consumer group yok)
builder.Services.AddHostedService<OrderGroupConsumer>(); // XREADGROUP + XACK (g1 grubu)
builder.Services.AddHostedService<OrderAnalyticsConsumer>(); // XREADGROUP NOACK (analytics grubu)
builder.Services.AddHostedService<PaymentConsumer>(); // at-least-once, bazen çöker
builder.Services.AddHostedService<PendingRecoveryService>(); // XPENDING + XAUTOCLAIM + DLQ

var host = builder.Build();
host.Run();

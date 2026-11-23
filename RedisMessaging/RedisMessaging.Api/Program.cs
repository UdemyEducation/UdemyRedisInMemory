using RedisMessaging.Api.PubSub;
using RedisMessaging.Api.Streams;
using RedisMessaging.Shared;
using RedisMessaging.Shared.PubSub;
using Scalar.AspNetCore;

var builder = WebApplication.CreateBuilder(args);

// Aspire: OpenTelemetry, health check, service discovery, resilience
builder.AddServiceDefaults();

// Aspire Redis client entegrasyonu:
// AppHost'taki "redis" kaynağının connection string'ini otomatik alır ve
// IConnectionMultiplexer'ı singleton olarak DI'a ekler (+ health check + tracing).
builder.AddRedisClient(RedisNames.ConnectionName);

builder.Services.AddSingleton<EventBus>();
builder.Services.AddOpenApi();

var app = builder.Build();

app.MapDefaultEndpoints();
app.MapOpenApi();
app.MapScalarApiReference();

app.MapGet("/", () => Results.Redirect("/scalar/v1")).ExcludeFromDescription();

app.MapPubSubEndpoints();
app.MapStreamEndpoints();

app.Run();

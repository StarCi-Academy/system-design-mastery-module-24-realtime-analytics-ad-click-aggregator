using CountConsumer;

// Bootstraps the count-consumer: an HTTP server (GET /counts) plus a background
// Kafka consumer that deduplicates by clickId and counts per adId.
var builder = WebApplication.CreateBuilder(args);

builder.Services.AddSingleton(KafkaConfig.FromEnv());
builder.Services.AddSingleton<CounterService>();
builder.Services.AddHostedService<ConsumerWorker>();

var app = builder.Build();

// GET /counts returns the deduplicated totals without scraping logs.
app.MapGet("/counts", (CounterService counter) => Results.Json(counter.Snapshot()));

var port = Environment.GetEnvironmentVariable("PORT") ?? "3001";
app.Run($"http://0.0.0.0:{port}");

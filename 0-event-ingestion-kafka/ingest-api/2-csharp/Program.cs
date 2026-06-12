using IngestApi;

// Bootstraps the ingest-api HTTP producer.
//
// One route only: POST /clicks. The 202 status code makes the fire-and-forget
// contract explicit — the event is accepted into the log, not yet processed.
var builder = WebApplication.CreateBuilder(args);

builder.Services.AddSingleton(KafkaConfig.FromEnv());
builder.Services.AddSingleton<EventsService>();

var app = builder.Build();

app.MapPost("/clicks", async (PublishClickDto dto, EventsService events) =>
{
    if (string.IsNullOrWhiteSpace(dto.AdId))
    {
        return Results.BadRequest(new { error = "adId is required" });
    }
    var ack = await events.PublishAsync(dto);
    // 202 Accepted: appended into the log, not yet processed.
    return Results.Json(ack, statusCode: 202);
});

// Bind on all interfaces so the host can reach the container.
var port = Environment.GetEnvironmentVariable("PORT") ?? "3000";
app.Run($"http://0.0.0.0:{port}");

using Kotiz.Alerts;
using Microsoft.Extensions.Options;

var builder = Host.CreateApplicationBuilder(args);

builder.Services.Configure<AlertsOptions>(builder.Configuration.GetSection("Alerts"));

// Telegram only if both the bot token and the chat id are configured; otherwise, the log.
builder.Services.AddSingleton<INotifier>(sp =>
    sp.GetRequiredService<IOptions<AlertsOptions>>().Value.TelegramEnabled
        ? ActivatorUtilities.CreateInstance<TelegramNotifier>(sp, new HttpClient { Timeout = TimeSpan.FromSeconds(10) })
        : ActivatorUtilities.CreateInstance<LogNotifier>(sp));

builder.Services.AddSingleton<AlertService>();
builder.Services.AddHostedService<AlertsWorker>();

var host = builder.Build();
host.Run();

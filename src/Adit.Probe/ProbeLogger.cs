using System.Text.Json;
using System.Text.Json.Serialization;

namespace Adit.Probe;

internal sealed class ProbeLogger : IDisposable
{
    private readonly object gate = new();
    private readonly JsonSerializerOptions jsonOptions;
    private readonly Action<string>? jsonLineObserver;
    private readonly bool writeToConsole;
    private readonly StreamWriter writer;

    public ProbeLogger(
        string path,
        bool writeToConsole = true,
        Action<string>? jsonLineObserver = null)
    {
        Path = path;
        this.writeToConsole = writeToConsole;
        this.jsonLineObserver = jsonLineObserver;
        Directory.CreateDirectory(System.IO.Path.GetDirectoryName(path)!);
        writer = new StreamWriter(
            File.Open(path, FileMode.Create, FileAccess.Write, FileShare.Read))
        {
            AutoFlush = true
        };

        jsonOptions = new JsonSerializerOptions
        {
            PropertyNamingPolicy = JsonNamingPolicy.CamelCase
        };
        jsonOptions.Converters.Add(new JsonStringEnumConverter());
    }

    public string Path { get; }

    public void Log(string kind, object payload)
    {
        var entry = new ProbeLogEntry(DateTimeOffset.UtcNow, kind, payload);
        var json = JsonSerializer.Serialize(entry, jsonOptions);

        lock (gate)
        {
            if (writeToConsole)
            {
                Console.WriteLine(json);
            }
            writer.WriteLine(json);
        }

        jsonLineObserver?.Invoke(json);
    }

    public void Dispose()
    {
        writer.Dispose();
    }
}

internal sealed record ProbeLogEntry(DateTimeOffset TimestampUtc, string Kind, object Payload);

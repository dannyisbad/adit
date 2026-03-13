using Windows.Devices.Bluetooth.Advertisement;
using Windows.Storage.Streams;

namespace Adit.Core.Services;

public sealed class AppleBleAddressResolver
{
    private const ushort AppleCompanyId = 0x004C;

    public async Task<IReadOnlyList<AppleBleAddressCandidate>> ResolveCandidatesAsync(
        string? targetName,
        TimeSpan watchDuration,
        CancellationToken cancellationToken,
        int maxCandidates = 5)
    {
        var aggregates = new Dictionary<ulong, Candidate>();
        var gate = new object();

        var watcher = new BluetoothLEAdvertisementWatcher
        {
            ScanningMode = BluetoothLEScanningMode.Active
        };

        void Received(BluetoothLEAdvertisementWatcher sender, BluetoothLEAdvertisementReceivedEventArgs args)
        {
            var manufacturerPayloads = args.Advertisement.ManufacturerData
                .Where(item => item.CompanyId == AppleCompanyId)
                .Select(item => Convert.ToHexString(ReadBytes(item.Data)))
                .ToArray();
            if (manufacturerPayloads.Length == 0)
            {
                return;
            }

            lock (gate)
            {
                if (!aggregates.TryGetValue(args.BluetoothAddress, out var candidate))
                {
                    candidate = new Candidate();
                    aggregates[args.BluetoothAddress] = candidate;
                }

                candidate.Count++;
                candidate.IsConnectable |= args.IsConnectable;

                if (!string.IsNullOrWhiteSpace(args.Advertisement.LocalName))
                {
                    candidate.LocalNames.Add(args.Advertisement.LocalName);
                }

                foreach (var payload in manufacturerPayloads)
                {
                    if (payload.StartsWith("1007", StringComparison.OrdinalIgnoreCase))
                    {
                        candidate.HasApple1007 = true;
                    }
                    else if (payload.StartsWith("1005", StringComparison.OrdinalIgnoreCase))
                    {
                        candidate.HasApple1005 = true;
                    }
                }
            }
        }

        watcher.Received += Received;
        try
        {
            watcher.Start();
            await Task.Delay(watchDuration, cancellationToken);
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
        }
        finally
        {
            watcher.Stop();
            watcher.Received -= Received;
        }

        lock (gate)
        {
            var normalizedTarget = string.IsNullOrWhiteSpace(targetName)
                ? null
                : targetName.Trim();

            return aggregates
                .Select(
                    pair =>
                    {
                        var candidate = pair.Value;
                        return new AppleBleAddressCandidate(
                            pair.Key,
                            Score(candidate, normalizedTarget),
                            candidate.Count,
                            candidate.HasApple1007,
                            candidate.HasApple1005,
                            candidate.IsConnectable,
                            candidate.LocalNames
                                .OrderBy(name => name, StringComparer.OrdinalIgnoreCase)
                                .ToArray());
                    })
                .Where(candidate => candidate.Score > 0)
                .OrderByDescending(candidate => candidate.Score)
                .ThenByDescending(candidate => candidate.Count)
                .Take(Math.Max(1, maxCandidates))
                .ToArray();
        }
    }

    public async Task<ulong?> ResolveAsync(
        string? targetName,
        TimeSpan watchDuration,
        CancellationToken cancellationToken)
    {
        var candidate = (await ResolveCandidatesAsync(
            targetName,
            watchDuration,
            cancellationToken,
            1)).FirstOrDefault();
        return candidate?.Address;
    }

    private static int Score(Candidate candidate, string? normalizedTarget)
    {
        var score = 0;

        if (candidate.HasApple1007)
        {
            score += 100;
        }
        else if (candidate.HasApple1005)
        {
            score += 25;
        }

        if (candidate.IsConnectable)
        {
            score += 20;
        }

        if (!string.IsNullOrWhiteSpace(normalizedTarget))
        {
            if (candidate.LocalNames.Any(name => name.Contains(normalizedTarget, StringComparison.OrdinalIgnoreCase)))
            {
                score += 40;
            }
            else if (normalizedTarget.Contains("iphone", StringComparison.OrdinalIgnoreCase)
                     && candidate.LocalNames.Any(name => name.Contains("iphone", StringComparison.OrdinalIgnoreCase)))
            {
                score += 20;
            }
        }

        score += Math.Min(candidate.Count, 10);
        return score;
    }

    private static byte[] ReadBytes(IBuffer buffer)
    {
        var bytes = new byte[buffer.Length];
        DataReader.FromBuffer(buffer).ReadBytes(bytes);
        return bytes;
    }

    private sealed class Candidate
    {
        public int Count { get; set; }

        public bool HasApple1005 { get; set; }

        public bool HasApple1007 { get; set; }

        public bool IsConnectable { get; set; }

        public HashSet<string> LocalNames { get; } = [];
    }
}

public sealed record AppleBleAddressCandidate(
    ulong Address,
    int Score,
    int Count,
    bool HasApple1007,
    bool HasApple1005,
    bool IsConnectable,
    IReadOnlyList<string> LocalNames);

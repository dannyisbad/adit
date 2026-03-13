using Adit.Daemon.Services;
using Microsoft.Extensions.Logging.Abstractions;

namespace Adit.Daemon.Tests;

public sealed class DeviceFusionCoordinatorTests
{
    [Fact]
    public async Task RunAsync_SerializesWorkPerDevice()
    {
        await using var coordinator = new DeviceFusionCoordinator(NullLogger<DeviceFusionCoordinator>.Instance);
        var steps = new List<string>();
        var firstStarted = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

        var first = coordinator.RunAsync(
            "device-a",
            "first",
            async ct =>
            {
                steps.Add("first-start");
                firstStarted.TrySetResult();
                await Task.Delay(TimeSpan.FromMilliseconds(50), ct);
                steps.Add("first-end");
            });

        await firstStarted.Task.WaitAsync(TimeSpan.FromSeconds(1));

        var second = coordinator.RunAsync(
            "device-a",
            "second",
            ct =>
            {
                steps.Add("second");
                return Task.CompletedTask;
            });

        await Task.WhenAll(first, second);

        Assert.Equal(["first-start", "first-end", "second"], steps);
    }

    [Fact]
    public async Task RunAsync_AllowsDifferentDevicesToRunIndependently()
    {
        await using var coordinator = new DeviceFusionCoordinator(NullLogger<DeviceFusionCoordinator>.Instance);
        var firstStarted = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var releaseFirst = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var secondStarted = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

        var first = coordinator.RunAsync(
            "device-a",
            "first",
            async ct =>
            {
                firstStarted.TrySetResult();
                await releaseFirst.Task.WaitAsync(ct);
            });

        await firstStarted.Task.WaitAsync(TimeSpan.FromSeconds(1));

        var second = coordinator.RunAsync(
            "device-b",
            "second",
            ct =>
            {
                secondStarted.TrySetResult();
                return Task.CompletedTask;
            });

        await secondStarted.Task.WaitAsync(TimeSpan.FromSeconds(1));

        releaseFirst.TrySetResult();
        await Task.WhenAll(first, second);
    }
}

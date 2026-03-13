using Microsoft.Internal.Diagnostics.Context;

namespace Adit.Core.Transport;

internal static class TraceContextFactory
{
    public static ITraceContext Create()
    {
        return new TraceContext(
            Guid.NewGuid().ToString("N"),
            string.Empty,
            Guid.NewGuid().ToString("N"),
            traceFlags: 0,
            new Dictionary<string, string>());
    }
}

using System.Collections;
using System.Reflection;
using Adit.Probe.MapInterop;
using Microsoft.Extensions.Logging;
using Microsoft.Internal.Diagnostics.Context;
using Microsoft.Internal.Bluetooth.Pbap;
using Microsoft.Internal.Bluetooth.Pbap.Request;
using MixERP.Net.VCards;

namespace Adit.Probe;

internal sealed class MicrosoftPbapProbe
{
    private readonly ProbeLogger logger;
    private readonly ProbeOptions options;
    private readonly BluetoothEndpointRecord target;

    public MicrosoftPbapProbe(
        BluetoothEndpointRecord target,
        ProbeOptions options,
        ProbeLogger logger)
    {
        this.target = target;
        this.options = options;
        this.logger = logger;
    }

    public async Task<int> RunAsync(CancellationToken cancellationToken)
    {
        if (options.EvictPhoneLink)
        {
            PhoneLinkEviction.Evict(logger);
            await Task.Delay(TimeSpan.FromSeconds(1), cancellationToken);
        }

        using var loggerFactory = LoggerFactory.Create(builder => builder.SetMinimumLevel(LogLevel.Debug));
        var manager = new PbapClientManager(
            new MapSocketProvider(loggerFactory, logger),
            new MapBluetoothDeviceProvider(),
            loggerFactory);

        object? openResult = null;
        IDisposable? client = null;

        try
        {
            openResult = await manager.OpenAsync(target.Id, traceContext: null!, cancellationToken);
            logger.Log("pbap.open_result", SnapshotObject(openResult) ?? new { });

            if (!ReadBoolProperty(openResult, "IsSuccess"))
            {
                return 1;
            }

            client = GetPropertyValue<IDisposable>(openResult, "PbapClient")
                ?? GetPropertyValue<IDisposable>(openResult, "Result");
            if (client is null)
            {
                logger.Log("pbap.client_missing", new { target.Id, target.Name });
                return 1;
            }

            await NavigatePhoneBookAsync(client, cancellationToken);

            var sizeResult = await InvokePbapAsync(
                client,
                "PullPhoneBookAsync",
                new PullPhoneBookRequestParameters
                {
                    ListStartOffset = 0,
                    MaxListCount = 0,
                    Name = "telecom/pb.vcf",
                    Format = RequestFormat.VCard21,
                    Filter = AttributeMask.UID
                },
                cancellationToken);
            logger.Log("pbap.phonebook_size", SnapshotObject(sizeResult) ?? new { });

            var contactsResult = await InvokePbapAsync(
                client,
                "PullPhoneBookAsync",
                new PullPhoneBookRequestParameters
                {
                    ListStartOffset = 0,
                    MaxListCount = 200,
                    Name = "telecom/pb.vcf",
                    Format = RequestFormat.VCard21,
                    Filter = AttributeMask.FormattedName
                        | AttributeMask.StructuredName
                        | AttributeMask.PhoneNumber
                        | AttributeMask.EmailAddress
                        | AttributeMask.UID
                },
                cancellationToken);
            logger.Log("pbap.contacts_batch", SnapshotObject(contactsResult) ?? new { });

            var contacts = GetPropertyValue<IEnumerable>(contactsResult, "Body")
                ?? GetPropertyValue<IEnumerable>(contactsResult, "Result");
            if (contacts is not null)
            {
                var cards = contacts.Cast<VCard>()
                    .Select(SummarizeContact)
                    .ToArray();
                logger.Log(
                    "pbap.contacts_summary",
                    new
                    {
                        count = cards.Length,
                        preview = cards.Take(20).ToArray()
                    });

                var indexPreview = cards
                    .SelectMany(
                        card => card.Phones.Select(
                            phone => new
                            {
                                phone.Normalized,
                                contact = card.DisplayName,
                                card.UniqueIdentifier
                            }))
                    .Where(entry => !string.IsNullOrWhiteSpace(entry.Normalized))
                    .Distinct()
                    .Take(20)
                    .ToArray();
                logger.Log("pbap.phone_index_preview", new { count = indexPreview.Length, entries = indexPreview });
            }

            return 0;
        }
        catch (Exception exception)
        {
            logger.Log(
                "pbap.unhandled_exception",
                new
                {
                    error = exception.ToString(),
                    openResult = SnapshotObject(openResult)
                });
            return 1;
        }
        finally
        {
            client?.Dispose();
        }
    }

    private async Task NavigatePhoneBookAsync(object client, CancellationToken cancellationToken)
    {
        var rootResult = await InvokePbapAsync(
            client,
            "SetPhoneBookAsync",
            CreateSetPhoneBookRequest(string.Empty, "Root"),
            cancellationToken);
        logger.Log("pbap.set_phonebook_root", SnapshotObject(rootResult) ?? new { });

        var telecomResult = await InvokePbapAsync(
            client,
            "SetPhoneBookAsync",
            CreateSetPhoneBookRequest("telecom", "Down"),
            cancellationToken);
        logger.Log("pbap.set_phonebook_telecom", SnapshotObject(telecomResult) ?? new { });

        var pbResult = await InvokePbapAsync(
            client,
            "SetPhoneBookAsync",
            CreateSetPhoneBookRequest("pb", "Down"),
            cancellationToken);
        logger.Log("pbap.set_phonebook_pb", SnapshotObject(pbResult) ?? new { });
    }

    private static SetPhoneBookRequestParameters CreateSetPhoneBookRequest(string name, string flagName)
    {
        var request = new SetPhoneBookRequestParameters();
        var flagsProperty = typeof(SetPhoneBookRequestParameters).GetProperty(nameof(SetPhoneBookRequestParameters.Flags));
        if (flagsProperty is null)
        {
            throw new MissingMemberException(typeof(SetPhoneBookRequestParameters).FullName, "Flags");
        }

        flagsProperty.SetValue(request, Enum.Parse(flagsProperty.PropertyType, flagName, false));
        if (!string.IsNullOrEmpty(name))
        {
            request.Name = name;
        }

        return request;
    }

    private static async Task<object?> InvokePbapAsync(
        object target,
        string methodName,
        object request,
        CancellationToken cancellationToken)
    {
        var method = target.GetType()
            .GetMethods(BindingFlags.Instance | BindingFlags.Public)
            .FirstOrDefault(
                candidate =>
                {
                    if (!string.Equals(candidate.Name, methodName, StringComparison.Ordinal))
                    {
                        return false;
                    }

                    var parameters = candidate.GetParameters();
                    return parameters.Length == 3
                        && parameters[0].ParameterType == request.GetType()
                        && parameters[2].ParameterType == typeof(CancellationToken);
                });

        if (method is null)
        {
            throw new MissingMethodException(target.GetType().FullName, methodName);
        }

        var traceContext = CreateTraceContext();
        var task = (Task)method.Invoke(target, new object?[] { request, traceContext, cancellationToken })!;
        await task;
        return task.GetType().GetProperty("Result")?.GetValue(task);
    }

    private static ITraceContext CreateTraceContext()
    {
        return new TraceContext(
            Guid.NewGuid().ToString("N"),
            string.Empty,
            Guid.NewGuid().ToString("N"),
            traceFlags: 0,
            new Dictionary<string, string>());
    }

    private static T? GetPropertyValue<T>(object? target, string propertyName)
        where T : class
    {
        return target?.GetType()
            .GetProperty(propertyName, BindingFlags.Instance | BindingFlags.Public)
            ?.GetValue(target) as T;
    }

    private static bool ReadBoolProperty(object? target, string propertyName)
    {
        var value = target?.GetType()
            .GetProperty(propertyName, BindingFlags.Instance | BindingFlags.Public)
            ?.GetValue(target);
        return value is bool boolValue && boolValue;
    }

    private static object? SnapshotObject(object? value, int depth = 0)
    {
        if (value is null)
        {
            return null;
        }

        if (depth >= 2)
        {
            return new
            {
                type = value.GetType().FullName,
                text = value.ToString()
            };
        }

        if (value is string || value.GetType().IsPrimitive || value is Guid || value is Enum)
        {
            return value;
        }

        if (value is IEnumerable enumerable && value is not IDictionary)
        {
            return enumerable.Cast<object?>()
                .Take(10)
                .Select(item => SnapshotObject(item, depth + 1))
                .ToArray();
        }

        var properties = value.GetType()
            .GetProperties(BindingFlags.Instance | BindingFlags.Public)
            .Where(property => property.CanRead && property.GetIndexParameters().Length == 0)
            .Take(20)
            .ToDictionary(
                property => property.Name,
                property =>
                {
                    try
                    {
                        return SnapshotObject(property.GetValue(value), depth + 1);
                    }
                    catch (Exception exception)
                    {
                        return new
                        {
                            error = exception.GetType().Name,
                            exception.Message
                        };
                    }
                });

        return new
        {
            type = value.GetType().FullName,
            properties
        };
    }

    private static PbapContactSummary SummarizeContact(VCard card)
    {
        var phones = (card.Telephones ?? [])
            .Where(telephone => !string.IsNullOrWhiteSpace(telephone.Number))
            .Select(
                telephone => new PbapPhoneSummary(
                    telephone.Number,
                    PhoneNumberNormalizer.Normalize(telephone.Number),
                    telephone.Type.ToString()))
            .Distinct()
            .ToArray();
        var emails = (card.Emails ?? [])
            .Where(email => !string.IsNullOrWhiteSpace(email.EmailAddress))
            .Select(email => email.EmailAddress)
            .Distinct(StringComparer.OrdinalIgnoreCase)
            .ToArray();

        return new PbapContactSummary(
            card.UniqueIdentifier,
            ReadDisplayName(card),
            phones,
            emails);
    }

    private static string ReadDisplayName(VCard card)
    {
        if (!string.IsNullOrWhiteSpace(card.FormattedName))
        {
            return card.FormattedName;
        }

        var parts = new[]
        {
            card.Prefix,
            card.FirstName,
            card.MiddleName,
            card.LastName,
            card.Suffix
        };

        var composite = string.Join(
            " ",
            parts.Where(part => !string.IsNullOrWhiteSpace(part)).Select(part => part!.Trim()));

        return string.IsNullOrWhiteSpace(composite)
            ? "(unnamed)"
            : composite;
    }
}

internal sealed record PbapContactSummary(
    string? UniqueIdentifier,
    string DisplayName,
    IReadOnlyList<PbapPhoneSummary> Phones,
    IReadOnlyList<string> Emails);

internal sealed record PbapPhoneSummary(
    string Raw,
    string? Normalized,
    string Type);

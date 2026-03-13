using System.Collections;
using System.Reflection;
using Adit.Core.Models;
using Adit.Core.Transport;
using Adit.Core.Utilities;
using Microsoft.Extensions.Logging;
using Microsoft.Internal.Bluetooth.Pbap;
using Microsoft.Internal.Bluetooth.Pbap.Request;
using MixERP.Net.VCards;

namespace Adit.Core.Services;

public sealed class PbapContactsService
{
    private readonly ILoggerFactory loggerFactory;
    private readonly PhoneLinkProcessController processController;

    public PbapContactsService(ILoggerFactory loggerFactory, PhoneLinkProcessController processController)
    {
        this.loggerFactory = loggerFactory;
        this.processController = processController;
    }

    public async Task<IReadOnlyList<ContactRecord>> PullContactsAsync(
        BluetoothEndpointRecord target,
        bool evictPhoneLink,
        CancellationToken cancellationToken)
    {
        if (evictPhoneLink)
        {
            processController.Evict();
            await Task.Delay(TimeSpan.FromSeconds(1), cancellationToken);
        }

        var manager = new PbapClientManager(
            new MapSocketProvider(),
            new MapBluetoothDeviceProvider(),
            loggerFactory);

        IDisposable? client = null;
        object? openResult = null;

        try
        {
            openResult = await manager.OpenAsync(target.Id, traceContext: null!, cancellationToken);
            if (!ReadBoolProperty(openResult, "IsSuccess"))
            {
                throw new InvalidOperationException("PBAP open failed.");
            }

            client = GetPropertyValue<IDisposable>(openResult, "PbapClient")
                ?? GetPropertyValue<IDisposable>(openResult, "Result");
            if (client is null)
            {
                throw new InvalidOperationException("PBAP client was not returned.");
            }

            await NavigatePhoneBookAsync(client, cancellationToken);

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

            var contacts = GetPropertyValue<IEnumerable>(contactsResult, "Body")
                ?? GetPropertyValue<IEnumerable>(contactsResult, "Result");
            return contacts is null
                ? []
                : contacts.Cast<VCard>().Select(SummarizeContact).ToArray();
        }
        finally
        {
            client?.Dispose();
        }
    }

    private static async Task NavigatePhoneBookAsync(object client, CancellationToken cancellationToken)
    {
        await InvokePbapAsync(
            client,
            "SetPhoneBookAsync",
            CreateSetPhoneBookRequest(string.Empty, "Root"),
            cancellationToken);
        await InvokePbapAsync(
            client,
            "SetPhoneBookAsync",
            CreateSetPhoneBookRequest("telecom", "Down"),
            cancellationToken);
        await InvokePbapAsync(
            client,
            "SetPhoneBookAsync",
            CreateSetPhoneBookRequest("pb", "Down"),
            cancellationToken);
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

        var traceContext = TraceContextFactory.Create();
        var task = (Task)method.Invoke(target, new object?[] { request, traceContext, cancellationToken })!;
        await task;
        return task.GetType().GetProperty("Result")?.GetValue(task);
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

    private static ContactRecord SummarizeContact(VCard card)
    {
        var phones = (card.Telephones ?? [])
            .Where(telephone => !string.IsNullOrWhiteSpace(telephone.Number))
            .Select(
                telephone => new ContactPhoneRecord(
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

        return new ContactRecord(
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

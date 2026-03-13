using Adit.Core.Models;
using Windows.ApplicationModel;

namespace Adit.Core.Utilities;

public static class PackageIdentitySnapshot
{
    public static PackageIdentityRecord Capture()
    {
        try
        {
            var package = Package.Current;
            var id = package.Id;

            return new PackageIdentityRecord(
                true,
                id.Name,
                id.FullName,
                id.FamilyName,
                id.Publisher,
                null,
                null);
        }
        catch (Exception exception)
        {
            return new PackageIdentityRecord(
                false,
                null,
                null,
                null,
                null,
                exception.GetType().Name,
                exception.Message);
        }
    }
}

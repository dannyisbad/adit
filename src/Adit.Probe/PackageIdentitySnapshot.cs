using Windows.ApplicationModel;

namespace Adit.Probe;

internal static class PackageIdentitySnapshot
{
    public static object Capture()
    {
        try
        {
            var package = Package.Current;
            var id = package.Id;

            return new
            {
                hasPackageIdentity = true,
                id.Name,
                fullName = id.FullName,
                familyName = id.FamilyName,
                publisher = id.Publisher
            };
        }
        catch (Exception exception)
        {
            return new
            {
                hasPackageIdentity = false,
                error = exception.GetType().Name,
                message = exception.Message
            };
        }
    }
}

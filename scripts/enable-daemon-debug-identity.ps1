param(
    [string]$Configuration = "Debug"
)

$ErrorActionPreference = "Stop"

$repoRoot = Split-Path -Parent $PSScriptRoot
$projectRoot = Join-Path $repoRoot "src\Adit.Daemon"
$targetFramework = "net10.0-windows10.0.19041.0"
$exePath = Join-Path $projectRoot "bin\$Configuration\$targetFramework\Adit.Daemon.exe"
$manifestPath = Join-Path $projectRoot "appxmanifest.xml"

dotnet build (Join-Path $repoRoot "Adit.sln") --configuration $Configuration | Out-Host

if (-not (Test-Path $exePath)) {
    throw "Daemon executable not found at '$exePath'."
}

Push-Location $projectRoot
try {
    winapp create-debug-identity $exePath --manifest $manifestPath
}
finally {
    Pop-Location
}

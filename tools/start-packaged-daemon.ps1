param(
    [string]$Configuration = "Release",
    [string]$Url = "http://127.0.0.1:5037",
    [string]$PhoneLinkInstallDir,
    [switch]$SkipBuild,
    [switch]$SkipUiBuild
)

$ErrorActionPreference = "Stop"

$repoRoot = Split-Path -Parent $PSScriptRoot
$webDir = Join-Path $repoRoot "web"
$projectDir = Join-Path $repoRoot "src\Adit.Daemon"
$targetFramework = "net10.0-windows10.0.19041.0"
$outputDir = Join-Path $projectDir "bin\$Configuration\$targetFramework"
$exePath = Join-Path $outputDir "Adit.Daemon.exe"
$manifestPath = Join-Path $projectDir "appxmanifest.xml"
$effectivePhoneLinkInstallDir = if ($PhoneLinkInstallDir) { $PhoneLinkInstallDir } elseif ($env:PhoneLinkInstallDir) { $env:PhoneLinkInstallDir } else { $null }

if ($effectivePhoneLinkInstallDir -and -not (Test-Path $effectivePhoneLinkInstallDir)) {
    throw "Phone Link internals path not found at $effectivePhoneLinkInstallDir"
}

if (-not $SkipUiBuild) {
    Push-Location $webDir
    try {
        if (Test-Path (Join-Path $webDir "package-lock.json")) {
            npm ci | Out-Host
        }
        else {
            npm install | Out-Host
        }
        if ($LASTEXITCODE -ne 0) {
            throw "npm install failed with exit code $LASTEXITCODE"
        }

        npm run build | Out-Host
        if ($LASTEXITCODE -ne 0) {
            throw "npm run build failed with exit code $LASTEXITCODE"
        }
    }
    finally {
        Pop-Location
    }
}

if (-not $SkipBuild) {
    Push-Location $repoRoot
    try {
        $dotnetArgs = @("build", "Adit.sln", "-c", $Configuration)
        if ($effectivePhoneLinkInstallDir) {
            $dotnetArgs += "/p:PhoneLinkInstallDir=$effectivePhoneLinkInstallDir"
        }

        dotnet @dotnetArgs | Out-Host
        if ($LASTEXITCODE -ne 0) {
            throw "dotnet build failed with exit code $LASTEXITCODE"
        }
    }
    finally {
        Pop-Location
    }
}

if (-not (Test-Path $exePath)) {
    throw "Daemon executable not found at $exePath"
}

Push-Location $projectDir
try {
    winapp create-debug-identity $exePath --manifest $manifestPath --keep-identity | Out-Host
    if ($LASTEXITCODE -ne 0) {
        throw "winapp create-debug-identity failed with exit code $LASTEXITCODE"
    }
}
finally {
    Pop-Location
}

$env:ADIT_URL = $Url
$env:PhoneLinkInstallDir = $effectivePhoneLinkInstallDir
& $exePath

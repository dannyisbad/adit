# Setup And Troubleshooting

This is the practical `Adit` setup flow for v1.

The supported path is:

1. Install `Phone Link` on Windows.
2. Follow the `Link to Windows` instructions on the iPhone.
3. Accept every iPhone permission prompt that `Link to Windows` requests during first setup.
4. Wait for that first `Link to Windows` pairing to finish inside `Phone Link`.
5. Build `Adit`.
6. Run the daemon.
7. Use `doctor` and `devices` before debugging anything deeper.

Do not skip ahead and start `Adit` while the initial `Phone Link` or `Link to Windows` flow is still mid-bootstrap. The daemon expects that first pairing to exist already and then adopts the resulting runtime state.

## 1. Build

```powershell
dotnet build Adit.sln
```

`Adit` now auto-discovers the installed `Phone Link` package during build. If that fails, or `dotnet run` re-enters restore/build and cannot resolve the same internals, set the install dir manually:

```powershell
$env:PhoneLinkInstallDir = (Get-AppxPackage Microsoft.YourPhone).InstallLocation
dotnet build Adit.sln
```

## 2. Start The UI And Daemon

```powershell
cd web
npm install
npm run build
cd ..
dotnet run --project src\Adit.Daemon -- serve
```

The daemon listens on `http://127.0.0.1:5037` by default.

## 3. Use The CLI First

These are the first commands to run when something feels off:

```powershell
dotnet run --project src\Adit.Daemon -- help
dotnet run --project src\Adit.Daemon -- devices
dotnet run --project src\Adit.Daemon -- doctor
```

What they tell you:

- `devices`: what Windows currently exposes for the classic MAP/PBAP target and the LE ANCS target
- `doctor`: whether core messaging, contacts, and notifications are actually ready
- `help`: the supported daemon operator commands

## 4. HTTP Checks

```powershell
Invoke-RestMethod http://127.0.0.1:5037/v1/info
Invoke-RestMethod http://127.0.0.1:5037/v1/runtime
Invoke-RestMethod http://127.0.0.1:5037/v1/capabilities
Invoke-RestMethod http://127.0.0.1:5037/v1/doctor
Invoke-RestMethod http://127.0.0.1:5037/v1/setup/guide
Invoke-RestMethod -Method Post http://127.0.0.1:5037/v1/setup/check
Invoke-RestMethod -Method Post http://127.0.0.1:5037/v1/notifications/check
Invoke-RestMethod http://127.0.0.1:5037/v1/thread-chooser/status
```

## Common Failures

### Build fails with "Phone Link internals were not found"

Cause:
- `Phone Link` is not installed
- Windows cannot resolve the package install location

Fix:

```powershell
Get-AppxPackage Microsoft.YourPhone
$env:PhoneLinkInstallDir = (Get-AppxPackage Microsoft.YourPhone).InstallLocation
dotnet build Adit.sln
```

### Build or run fails with `Value cannot be null. (Parameter 'path1')`

Cause:
- `Phone Link` auto-discovery did not resolve a usable install path during restore/build

Fix:

```powershell
$env:PhoneLinkInstallDir = (Get-AppxPackage Microsoft.YourPhone).InstallLocation
dotnet build Adit.sln
dotnet run --project src\Adit.Daemon -- serve
```

### `doctor` says `no_device` or `waiting_for_device`

Cause:
- the iPhone is not paired
- Windows is not exposing the classic Bluetooth endpoint yet

Fix:
- Re-open `Phone Link` and confirm the LTW pairing is still live.
- Keep the iPhone nearby and unlocked.
- Run `devices` again and look for a paired classic endpoint for the iPhone.

### Messaging works but notifications are not ready

Cause:
- the classic target exists, but the LE ANCS side is missing or stale

Fix:

```powershell
Invoke-RestMethod -Method Post http://127.0.0.1:5037/v1/notifications/check
Invoke-RestMethod -Method Post http://127.0.0.1:5037/v1/notifications/enable
```

The legacy `/v1/bootstrap/notifications/check|enable|disable` routes still exist as compatibility aliases for older tooling, but `/v1/notifications/*` is the primary surface.

If it still does not come up:
- Toggle Bluetooth off and back on, on both the Windows PC and the iPhone. This resets the LE connection state and often clears stale ANCS sessions.
- Keep the phone unlocked.
- Make sure LTW pairing still exists.
- Run `devices` and confirm a paired LE endpoint shows up for the same container.

### Phone Link is holding MAP or PBAP open

Symptom:
- sends or pulls fail until `Phone Link` backs off

Fix:
- `Adit` already defaults `AutoEvictPhoneLink=true`
- if needed, close or reopen `Phone Link`, then force a sync:

```powershell
Invoke-RestMethod -Method Post http://127.0.0.1:5037/v1/sync/now
```

### The daemon is up but the UI says it cannot reach it

Cause:
- wrong URL
- daemon not actually running
- build output is stale

Fix:
- hit `http://127.0.0.1:5037/v1/info` directly
- rerun `doctor`
- rebuild the UI and restart the daemon

## Operator Commands

```powershell
dotnet run --project src\Adit.Daemon -- serve
dotnet run --project src\Adit.Daemon -- doctor
dotnet run --project src\Adit.Daemon -- devices
dotnet run --project src\Adit.Daemon -- info
dotnet run --project src\Adit.Daemon -- status
dotnet run --project src\Adit.Daemon -- runtime
dotnet run --project src\Adit.Daemon -- capabilities
dotnet run --project src\Adit.Daemon -- sync
dotnet run --project src\Adit.Daemon -- notifications-check
dotnet run --project src\Adit.Daemon -- notifications-enable
dotnet run --project src\Adit.Daemon -- notifications-disable
```

## Experimental Pairing API

The v1 product posture keeps `Link to Windows` as the supported setup path. Native daemon pairing routes are available only for lab work when you start the daemon with:

```powershell
$env:ADIT_ENABLE_EXPERIMENTAL_PAIRING_API = "true"
dotnet run --project src\Adit.Daemon -- serve
```

That enables:

- `GET /v1/pairing/candidates`
- `POST /v1/pairing/pair`
- `POST /v1/pairing/unpair`

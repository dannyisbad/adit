# Contributing to adit

Thanks for your interest. Adit is a solo project that recently went public, and
contributions are welcome, whether that's a bug report, a docs fix, a new SDK
wrapper, or a protocol-level improvement.

## Ways to contribute

**No contribution is too small.** Typo fixes, clarified docs, and better error
messages are all valued.

- **Bug reports:** open an issue. Include your Windows version, iPhone model,
  and the output of `GET /v1/doctor` if possible.
- **Feature ideas:** open an issue to discuss before writing code. This saves
  everyone time if the idea conflicts with the project's scope.
- **Pull requests:** fork, branch, fix, PR. Small focused PRs merge faster
  than large ones.
- **Docs improvements:** the setup flow especially could always use more
  clarity. If you hit a wall and figured it out, write it down.
- **SDK contributions:** new language SDKs, improved ergonomics for existing
  ones, better examples.
- **Protocol research:** if you've been poking at MAP, PBAP, or ANCS and found
  something interesting, `src/Adit.Probe` is the place for experiments.

## Setting up for development

### Prerequisites

- Windows 10/11 with Bluetooth
- iPhone (stock, no jailbreak)
- .NET 10 SDK
- Node 18+ (for the frontend and JS SDK)
- Microsoft **Phone Link** installed and paired

### Build

```powershell
dotnet build Adit.sln
cd web && npm install && npm run build && cd ..
dotnet run --project src\Adit.Daemon -- serve
```

### Run tests

```powershell
dotnet test Adit.sln
```

Some tests exercise live Bluetooth and will skip if no paired device is
available. That's expected.

## Pull request guidelines

1. **Branch from `master`**. Name your branch something descriptive
   (`fix-map-folder-listing`, `add-ruby-sdk`, `docs-pairing-steps`).

2. **Keep it focused.** One logical change per PR. If you find an unrelated
   issue while working, open a separate PR for it.

3. **Include context.** Your PR description should explain *what* changed and
   *why*. Link to an issue if one exists.

4. **Run the tests.** `dotnet test Adit.sln` should pass before you open a PR.
   If your change touches the frontend, make sure `npm run build` succeeds in
   `web/`.

5. **Match the existing style.** No need to memorize a style guide, just look
   at the surrounding code and follow the same patterns. The codebase uses:
   - C#: standard .NET conventions
   - TypeScript/React: functional components, Tailwind for styling
   - Docs: direct, concise, no filler

6. **Don't worry about being perfect.** If you're unsure about something, open
   the PR anyway and mention what you're uncertain about. Review is a
   conversation, not an exam.

## Project structure at a glance

| Path | What it is |
|---|---|
| `src/Adit.Core` | Bluetooth/MAP/PBAP/ANCS interop layer |
| `src/Adit.Daemon` | ASP.NET Core HTTP API, sync engine, SQLite cache |
| `src/Adit.Probe` | Protocol reverse-engineering lab (messy on purpose) |
| `sdk/` | JS, Python, MCP server, agent skill |
| `web/` | React 19 + TypeScript + Tailwind frontend (WIP) |
| `training/` | Thread chooser ML pipeline |
| `tests/` | Core, daemon, and probe tests |
| `docs/` | API reference, architecture, troubleshooting |

See [docs/architecture.md](docs/architecture.md) for more detail on layers and
design rules.

## Where help is especially welcome

- **Setup experience:** the pairing flow is the hardest part for new users.
  Better docs, better error messages, better recovery paths.
- **Frontend:** the React UI is early-stage. If you know React + Tailwind,
  there's a lot of room to improve things.
- **New SDKs:** Ruby, Go, Rust, Swift. If you want to wrap the REST API in
  your language of choice, go for it.
- **ANCS improvements:** notification streaming is beta. More testing across
  different iPhone models and iOS versions would help a lot.
- **Tests:** more coverage is always good, especially for edge cases in MAP
  message parsing and contact number normalization.

## Code of conduct

Be respectful. See [CODE_OF_CONDUCT.md](CODE_OF_CONDUCT.md).

## Questions?

If you're not sure about something, open an issue and ask. There are no stupid
questions. This project touches obscure Bluetooth protocols that most people
have never heard of, and the setup has real rough edges. Asking questions helps
improve the docs for everyone.

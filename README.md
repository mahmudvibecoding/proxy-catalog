# Proxy catalog on macOS

This private project discovers public proxy sources with Codex CLI, validates feeds, refreshes the local PostgreSQL catalog, and publishes verified backups to GitHub Releases.

## Daily behavior

The macOS LaunchAgent checks once a minute. When the signed-in user is at an unlocked, awake display and the internet is available, it starts the day's run. The timezone is Asia/Tashkent. Later wakes do not repeat a completed day's work. An unfinished run is resumed, and a process lock prevents overlap. Several missed days produce one current refresh.

A second LaunchAgent starts the existing PostgreSQL cluster after login. Configure its exact path in `postgres_service.data_directory`. It observes an already running instance without restarting it, and starts PostgreSQL when that instance is absent. It never initializes or replaces a cluster. The researcher checks both a real database query and internet access before attempting a run. Unavailable dependencies leave the saved stage and attempt count untouched, and the next minute's check tries again. `.local/readiness.json` records what it is waiting for.

Discovery uses **gpt-6.1-sol**, **max** reasoning effort, and live web search through ChatGPT authentication. There is **no discovery deadline, runtime cutoff, query limit, candidate limit, or token budget**. The [research prompt](prompts/discover.md) requires exhaustive discovery: pursue all actionable leads now, expand indexes and publishers beyond samples, search across languages and hosting ecosystems, and audit a persistent work list and coverage record before finishing. A candidate count or the prospect of another scheduled run is not a stopping condition. These are instructions to the researcher; the runner validates candidate data but cannot certify that the internet has been exhausted. Existing account usage limits still apply. The desktop app and a Terminal window do not need to remain open. The app's bundled CLI binary must remain installed.

Codex works inside an isolated research directory. It saves candidate URLs and evidence incrementally. Deterministic Python code downloads and validates those sources, imports valid sources, refreshes all enabled feeds, and publishes the result. Proxy connection testing is not part of this workflow.

While a run is active, `caffeinate -i` prevents idle system sleep. Closing the lid or explicitly sleeping the Mac can interrupt work; the next invocation uses saved checkpoints. The initial seed backs up existing data and does not consume the day's discovery run.

## Setup on another Mac

1. Clone this private repository, install Python 3.11 or later, PostgreSQL 18 tools, GitHub CLI, and Codex CLI.
2. Sign in to GitHub CLI and to Codex using ChatGPT.
3. Create the environment with `python3 -m venv .venv` and `.venv/bin/pip install -r requirements.txt`.
4. Copy `config.example.json` to `config.local.json`. Set the database connection, installed Codex path, optional cache storage location, and `postgres_service.data_directory` to an existing PostgreSQL cluster owned by your macOS account. Run `.venv/bin/python catalog.py install-postgres` to start it automatically at login. Omit `postgres_service` only when another service already manages your database.
5. Run `git pull --ff-only`, then `.venv/bin/python catalog.py sync` to retrieve a verified snapshot.
6. Restore into a new database using the command below. Point the local configuration at that database. Source payload/parser caches are optional and are downloaded again when needed; restored source records retain their historical observations.
7. Run a seed publication and restore audit, then install the LaunchAgent.

Local configuration, credentials, runtime state, downloads, and caches are excluded from Git. The scheduled command explicitly uses ChatGPT auth and removes API-key environment overrides. GitHub CLI uses the existing local sign-in. The CLI's `--ignore-user-config` gives this job a reproducible configuration; workspace-write permissions, network access, model, effort, web search, and authentication are specified explicitly. It does not alter the desktop's configuration.

## Commands

Run commands from the repository directory:

```sh
.venv/bin/python catalog.py status
.venv/bin/python catalog.py seed
.venv/bin/python catalog.py run
.venv/bin/python catalog.py resume
.venv/bin/python catalog.py install
.venv/bin/python catalog.py disable
.venv/bin/python catalog.py install-postgres
.venv/bin/python catalog.py disable-postgres
```

`seed` creates a full backup of the existing database, uploads and downloads it, and restores it into a temporary database for verification. `run` performs discovery and collection before backup. `resume` continues the active run; if none exists it starts a run. `install` requires a completed restore audit and enables automatic runs, including the PostgreSQL service when configured. `disable` stops research automation and preserves checkpoints. A manually started foreground run can be interrupted with Ctrl-C; its checkpoint remains.

`disable-postgres` separately disables database startup and stops PostgreSQL if this service started it. The current Mac's cluster also serves the `media` database, so research shutdown deliberately leaves the PostgreSQL service running. To deliberately keep the database stopped, disable its service before using `pg_ctl stop`. A PostgreSQL process that was already running before the service started remains owned by its original launcher.

Download and restore:

```sh
git pull --ff-only
.venv/bin/python catalog.py sync
.venv/bin/python catalog.py sync --tag catalog-RUN_ID --destination .local/downloads/chosen
.venv/bin/python catalog.py restore --destination .local/downloads/chosen --database proxy_catalog_restore_copy
```

The restore command only accepts a fresh database whose name begins `proxy_catalog_restore_`. It never replaces the collector's live database. Download verification checks the manifest, each part, and the reassembled backup. Restore verification checks every table's count and PostgreSQL content fingerprint, plus the identity sequence.

## What is saved

| Location | Contents |
|---|---|
| `sources/registry.json` | Current source registry, including disabled sources |
| `sources/discovery-history.json` | Discovery summaries, source provenance and validation outcomes |
| `reports/` | Per-run counts, failures, discovery usage and collection summary |
| `latest.json` | Latest verified release and manifest checksum |
| GitHub Releases | Compressed PostgreSQL backup parts, source records before/after collection, candidate evidence and manifest |
| `.local/runs/` | Detailed checkpoints, research transcript, logs and audit receipts |

Catalog backups include `proxies`, `proxy_lists`, existing `proxy_stats`, schema and identity state. Deduplication preserves existing proxy IDs and statistics. Configurations absent from current feeds remain in the catalog. A source that parses successfully is not evidence that its proxies work.

The collector is derived from the existing media project. Source downloads start at 24 concurrent requests with the existing host limits and parser cache. Each source's import and status commit together. The runner saves source records before a new collection replaces their latest status. New run IDs are allocated before starting the collector, so an interrupted invocation can find its own database checkpoint.

## Publication and recovery

Backups use a consistent PostgreSQL snapshot. Assets are split into at most 512 MiB parts. Draft releases recover by authenticated release listing, because a draft's Git tag may not exist yet. Uploaded sizes and SHA-256 digests must match. GitHub servers without asset digests get an explicit download check.

The first seed and one run per ISO week are fully downloaded and restored into a temporary database. After all required checks pass, the release is published and generated metadata is committed and pushed to `main`. Code commits must already be synchronized with `origin/main`; the automatic publisher only commits generated sources, reports and `latest.json`, and never force-pushes.

An upload failure keeps the same immutable snapshot and run ID for retry. Completed discovery/collection stages are not repeated. A source download failure is reported and does not prevent other sources being saved. A Codex error or incomplete response leaves discovery unfinished: collection cannot start until Codex reports a completed turn and writes a fresh `research_summary.json` with `completion_status: "complete"`. The next attempt resumes the saved session and candidates. Shutdown signals stop the research child and preserve the current stage. Runtime failures are eligible for retry after 60 seconds; database or network startup delays do not consume attempts or create a long backoff.

After a reboot, log in and unlock the Mac. PostgreSQL starts, the runner waits for connectivity and database readiness, and the same unfinished research resumes at the next eligible check. Research checkpoints are local until the later publication stages run. A full Mac reboot is a separate verification step from the controlled recovery pilot below.

Retain the initial seed, the seven latest published daily snapshots, and one snapshot from each of the four latest weeks. Cleanup only touches this runner's own verified publications, after a newer snapshot is published. Detailed reports remain in Git. Source payload/parser caches remain local. Low free space stops snapshot creation with a recoverable error.

## Verification

```sh
PYTHONPATH=vendor .venv/bin/python -m unittest discover -s tests -v
```

The tests cover local-day scheduling, duplicate locks, readiness delays, interrupted-stage recovery, incomplete/stale research responses, meaningful URL query parameters, malformed candidates, snapshot corruption, protected restore targets, model settings and existing parser/pagination behavior.

Run `.venv/bin/python tests/pilot_recovery.py` for the explicit recovery pilot. It sends SIGTERM to an isolated runner with a fake research CLI, verifies session/candidate preservation and duplicate exclusion, and resumes it to the validation boundary. It also creates a temporary PostgreSQL cluster and temporary LaunchAgent, verifies adoption of an already running server, startup after absence, clean shutdown, and restart, then removes its test service and cluster. It never stops the live database or invokes the real research model. Receipts are saved under `.local/recovery-pilot/`. A real reboot or sleep/wake cycle is verified through the installed LaunchAgents' subsequent run records; fixture checks alone do not prove that event.

Official references: [Codex unattended execution](https://learn.chatgpt.com/docs/non-interactive-mode), [ChatGPT authentication](https://learn.chatgpt.com/docs/auth), [GitHub release limits](https://docs.github.com/en/repositories/releasing-projects-on-github/about-releases), [macOS launchd](https://developer.apple.com/library/archive/documentation/MacOSX/Conceptual/BPSystemStartup/Chapters/ScheduledJobs.html).

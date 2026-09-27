# Publisher Lakehouse runbook

How to operate the v1.0 single-publisher platform: start and stop it, follow a
day's data through it, and recover when a step fails. Commands are for Windows
PowerShell 5.1, run from the repository root, unless stated otherwise. Never
print or paste `.env`; every command below reads it for you.

## What runs where

| Where | Component | Notes |
| --- | --- | --- |
| Windows host | PostgreSQL service, port 5432 | The ingestion manifest (`DATABASE_URL`). Not in Docker. |
| Windows host | Task Scheduler task `PublisherLakehouse-Producer` | Runs `scripts\producer.ps1` daily at 02:00 local time while you are logged on. |
| Windows host | Chrome + `.venv` | The scraper (`ingest run`), `lake land`, `ops push-scrape-health`, `scripts\check_grafana.py`. |
| Windows host | `data\` | Local bronze JSONL and raw HTML: the source that `lake land` mirrors. |
| Docker | `objectstore` (RustFS) | The lake bucket. Volume `lake-data`. S3 API on 127.0.0.1:9000, console on 127.0.0.1:9001. |
| Docker | `warehouse` (Postgres 17) | Serving warehouse on 127.0.0.1:5433. Volume `warehouse-data`. |
| Docker | `grafana` | Dashboards on http://127.0.0.1:3000. Volume `grafana-data`. |
| Docker | `airflow-db`, `airflow-init`, `airflow-apiserver`, `airflow-scheduler`, `airflow-dag-processor` | Orchestration; UI on http://127.0.0.1:8080. Volumes `airflow-db-data`, `airflow-auth`, `airflow-logs`. |
| Docker | `publisher-lakehouse-job:local` image | Immutable Spark job image that the DAGs start with DockerOperator. |
| Docker | `spark` service | Development Spark with the code bind-mounted; for manual jobs and tests only. |

## Start and stop

Start Docker Desktop and wait until it reports that the engine is running.
Then start the services in groups, storage first:

```powershell
docker compose up -d objectstore warehouse
docker compose up -d grafana
docker compose up -d airflow-db airflow-init
docker compose up -d airflow-apiserver airflow-scheduler airflow-dag-processor
docker compose ps
```

Every long-running service should show `healthy` (the dag processor has no
health check) and `airflow-init` should have exited with code 0. The
producer's preflight needs only `objectstore` and `warehouse`.

To stop, first make sure no DAG run is `running` or `queued` and no producer
run holds `logs\producer\.lock`:

```powershell
docker compose exec airflow-scheduler airflow dags list-runs lakehouse_sciencedirect
docker compose exec airflow-scheduler airflow dags list-runs lakehouse_maintenance
```

Then stop in reverse order:

```powershell
docker compose stop airflow-scheduler airflow-dag-processor airflow-apiserver
docker compose stop grafana
docker compose stop warehouse objectstore airflow-db
```

`docker compose stop` and `docker compose down` keep every named volume.

> **Warning:** `docker compose down -v` deletes **all** named volumes:
> `lake-data` (the whole lake), `warehouse-data`, `airflow-db-data`,
> `grafana-data`, `airflow-auth` and `airflow-logs`. Take a backup first (see
> [Backup and restore](#backup-and-restore)) and never use `-v` otherwise.

A Delta commit is atomic, so stopping Docker during a job never leaves a
half-written table version, but the task fails and must be cleared (see
[A failed DAG task](#a-failed-dag-task)).

## The daily flow and its evidence

```text
02:00 local  Task Scheduler -> producer.ps1
                ingest run -> lake land -> marker -> ops push-scrape-health
:00 UTC      lakehouse_sciencedirect (hourly) -> detect_pending
                bronze -> silver -> gold -> publish -> acknowledge
             Grafana reads the warehouse
Sun 03:00    lakehouse_maintenance (weekly, Asia/Colombo) -> OPTIMIZE
```

| Step | Evidence |
| --- | --- |
| Task started | `Get-ScheduledTaskInfo -TaskName PublisherLakehouse-Producer` (last run time and result) |
| Producer run | `logs\producer\<UTC yyyyMMddTHHmmssZ>-<publisher>\producer.log`; its last line is the summary, e.g. `Producer sciencedirect: ingest=0 land=ok marker=written push=ok exit=0` |
| Scrape | `ingest.err.log`: the `ingestion_run_complete` event carries the run counters |
| Landing | `land.log`: `uploaded`, `unchanged`, `replaced` and `empty skipped` counts, then `marker: landing/_markers/...` or `no marker: nothing new landed` |
| Scraper health | `push.log`; Grafana **Pipeline health → Scraper health** row |
| Pending work | Airflow UI, `lakehouse_sciencedirect` → `detect_pending` log: `Pending markers for sciencedirect: N; force=False`. With no marker the other five tasks are **skipped**; that is normal. |
| Lake jobs | Airflow task logs for `bronze`, `silver`, `gold` and `publish` (each job prints its summary) |
| Acknowledgement | A `<marker>.json.processed` object next to each processed marker |
| Published data | Grafana **Pipeline health**: *Last successful publish*, *Last publish status*, *Lake tables* (versions and commit ages), *Journal freshness* |
| Dashboards correct | `.venv\Scripts\python.exe scripts\check_grafana.py` ends with `OK summary: 0 failed checks` |

A run that scrapes nothing new writes a 0-byte JSONL part locally. `lake land`
skips 0-byte files and writes no marker, so the hourly DAG stays idle.

## Producer exit codes

When several steps fail the code is the first match in the order 5, 3, 1, 2, 4.

| Code | Meaning | What to do |
| --- | --- | --- |
| 0 | Success | Nothing. `marker=none` is normal when nothing new was scraped. |
| 1 | Ingest failed, or the script itself failed | Read `ingest.err.log` (structlog JSON, traceback at the end) and `ingest.out.log`. Land and push still ran, so what was scraped is landed and the run is recorded as `incomplete`. Fix the cause and rerun the producer; the manifest skips articles already collected. A script fault is logged in `producer.log`. |
| 2 | Land failed | Read `land.log`. Usually the object store or its credentials. The files stay in `data\`; after fixing, run `.venv\Scripts\python.exe -m publisher_lakehouse.cli lake land --publisher sciencedirect` or wait for the next producer run. |
| 3 | Docker unavailable; land and push skipped | See [Docker down during a producer run](#docker-down-during-a-producer-run). |
| 4 | Push failed | Read `push.log`: the warehouse or the manifest database was unreachable. Rerun the push for that run (command below). The lake is unaffected. |
| 5 | Another producer run is active | Wait. `logs\producer\.lock` names the holder's PID. A lock whose process is gone is replaced automatically by the next run. |

To record a run whose push was skipped or failed, point the push at that run's
log:

```powershell
$run = 'logs\producer\20260927T125233Z-sciencedirect'
.venv\Scripts\python.exe -m publisher_lakehouse.cli ops push-scrape-health --publisher sciencedirect --run-log "$run\ingest.err.log" --ingest-exit-code 0
```

Use the ingest exit code from that run's `producer.log`. The push is
idempotent: rerunning it updates the same `ops.scrape_runs` row.

## Recovery recipes

### A failed DAG task

Markers are acknowledged only after `publish` succeeds, so a failed run leaves
its markers pending and the next hourly run tries again by itself. To rerun
now, open the run in the Airflow UI, select the failed task and choose
**Clear** with **Downstream** and **Only failed**. The jobs are safe to repeat:
bronze, silver and gold skip MERGEs with nothing to change.

From the command line, for a scheduled run (manual runs have no logical date;
clear those in the UI):

```powershell
docker compose exec airflow-scheduler airflow tasks clear lakehouse_sciencedirect -s 2026-09-27T13:00:00+00:00 -e 2026-09-27T13:00:00+00:00 --only-failed --downstream --yes
```

To run all four jobs without a pending marker, trigger the DAG with the
configuration `{"force": true}`.

### Publish failed

A failed publish rolls back the live tables in its transaction and records a
`failed` row. Read the error:

```powershell
"SELECT publish_id, started_at, error FROM ops.publish_log WHERE status = 'failed' ORDER BY started_at DESC LIMIT 5;" | docker compose exec -T warehouse sh -c 'psql -U "$POSTGRES_USER" -d "$POSTGRES_DB"'
```

Pipe SQL into `psql` like this. Windows PowerShell 5.1 drops the inner double
quotes of a `psql -c "..."` argument, so that form runs the wrong statement.

Fix the cause, then publish again. A plain publish runs whenever any table
version differs from the last successful publish; `--force` republishes the
same versions:

```powershell
docker compose run --rm spark python -m publisher_lakehouse.transform.publish --dry-run
docker compose run --rm spark python -m publisher_lakehouse.transform.publish --force
```

Pause both DAGs while you run jobs by hand: the `lake_writer` pool serializes
only Airflow tasks.

### A stuck or unwanted marker

List the markers and their acknowledgements (a marker without a
`.processed` sibling is pending):

```powershell
.venv\Scripts\python.exe -c "from publisher_lakehouse.settings import load_lake_settings; from publisher_lakehouse.landing.sync import make_s3_client; s = load_lake_settings(); c = make_s3_client(s); [print(o['Key']) for p in c.get_paginator('list_objects_v2').paginate(Bucket=s.lake_bucket, Prefix='landing/_markers/') for o in p.get('Contents', [])]"
```

- **Stuck** (pending, and runs keep failing): fix the failing task; the marker
  clears when a run publishes successfully.
- **Unwanted** (you do not want a run for it): acknowledge it by hand, which
  keeps the audit trail:

  ```powershell
  $key = 'landing/_markers/sciencedirect/20260927T114954Z.json'
  .venv\Scripts\python.exe -c "import json, sys; from datetime import datetime, timezone; from publisher_lakehouse.settings import load_lake_settings; from publisher_lakehouse.landing.sync import make_s3_client; s = load_lake_settings(); make_s3_client(s).put_object(Bucket=s.lake_bucket, Key=sys.argv[1] + '.processed', Body=json.dumps({'run_id': 'manual', 'processed_at': datetime.now(timezone.utc).isoformat()}).encode())" $key
  ```

- **Reprocess** an acknowledged batch: delete its `.processed` object in the
  RustFS console (http://127.0.0.1:9001/rustfs/console/index.html), or trigger
  the DAG with `{"force": true}`.

### Docker down during a producer run

The producer exits 3. The scrape itself ran and its files are on disk, but
land and push were skipped. Start Docker Desktop and the storage services,
wait for `healthy`, then land and record the run:

```powershell
docker compose up -d objectstore warehouse
.venv\Scripts\python.exe -m publisher_lakehouse.cli lake land --publisher sciencedirect
```

Then run the push for that run's log, as under
[Producer exit codes](#producer-exit-codes). If you skip this, the next
producer run lands the files anyway, but the missed run gets no
`ops.scrape_runs` row.

### Rebuilding the job image after code changes

The DAGs run the code baked into `publisher-lakehouse-job:local`, not the
working tree. After committing a change under `publisher_lakehouse/`:

```powershell
powershell -ExecutionPolicy Bypass -File scripts\build_job_image.ps1
```

It prints `org.opencontainers.image.revision=<sha>`; a `-dirty` suffix means
the image contains uncommitted code. The next DAG task uses the new image;
`detect_pending` logs the revision whenever it starts the jobs. DAG files under `dags\`
are read live by the DAG processor and need no rebuild.

### Rotating a password

Several services store a secret only when their volume is first created, so
changing `.env` alone does nothing for them. Change the service first, then
`.env`, then recreate whatever reads the value from the environment.

For the Postgres roles, open `psql` interactively and use `\password <role>`,
which prompts twice and keeps the new password out of your shell history:

```powershell
docker compose exec warehouse sh -c 'psql -U "$POSTGRES_USER" -d "$POSTGRES_DB"'
docker compose exec airflow-db psql -U airflow -d airflow
```

| Secret | Kept from first start? | How to change it |
| --- | --- | --- |
| `S3_ACCESS_KEY`, `S3_SECRET_KEY` | No: RustFS reads its root credentials from the environment at start | Update `.env`, then `docker compose up -d --force-recreate objectstore airflow-scheduler airflow-dag-processor`. Log in to the RustFS console with the new keys to confirm. Host commands read `.env` on each run. |
| `WAREHOUSE_PASSWORD` | Yes (`warehouse-data` init) | In the warehouse `psql`, `\password <WAREHOUSE_USER>`. Update `.env`, then recreate `airflow-scheduler` and `airflow-dag-processor`, which pass it to jobs. |
| `GRAFANA_DB_PASSWORD` (`grafana_ro`) | Yes (`warehouse-data` init) | In the warehouse `psql`, `\password grafana_ro`. Update `.env`, then `docker compose up -d --force-recreate grafana airflow-scheduler airflow-dag-processor`. The datasource reads the value when Grafana starts. |
| `GRAFANA_ADMIN_PASSWORD` | Yes (`grafana-data`) | `docker compose exec grafana grafana cli admin reset-admin-password <new>`, then update `.env`, which `check_grafana.py` uses. |
| `AIRFLOW_ADMIN_PASSWORD` | No: `airflow-init` rewrites the password file on every run | Update `.env`, `docker compose up -d --force-recreate airflow-init`, then `docker compose restart airflow-apiserver`. |
| `AIRFLOW_DB_PASSWORD` | Yes (`airflow-db-data`) | In the Airflow `psql`, `\password airflow`. Update `.env`, then recreate `airflow-init`, `airflow-apiserver`, `airflow-scheduler` and `airflow-dag-processor`. |
| `AIRFLOW_JWT_SECRET` | No | Update `.env`, then recreate the API server, scheduler and DAG processor together. |
| `AIRFLOW_FERNET_KEY` | Encrypts connections and variables in `airflow-db-data` | The project stores none, but rotate safely anyway: set `new,old` in `.env`, recreate the Airflow services, run `airflow rotate-fernet-key` in the scheduler, then set `new` alone and recreate again. |
| `DATABASE_URL` (manifest) | Windows PostgreSQL | `ALTER ROLE` in the host PostgreSQL, then update the URL in `.env`. |

## Backup and restore

The Docker state lives in five named volumes: `lake-data`, `warehouse-data`,
`airflow-db-data`, `grafana-data` and `airflow-auth`. Compose prefixes them
with the project name, for example `publisher-lakehouse_lake-data`.
`airflow-logs` is not needed for a restore. The host keeps two more things to
back up yourself: the `data\` directory and the manifest database (`pg_dump`
on the host).

The warehouse can be rebuilt from the lake with `publish --force`, but
`lake-data` cannot be rebuilt without landing and replaying everything.

**Back up.** Stop the stack so every archive is consistent, and avoid the
02:00 producer window. The backup folder must be on a drive Docker Desktop
can share; with the WSL 2 backend any local fixed drive works.

```powershell
docker compose stop
$backup = "D:\backups\publisher-lakehouse\$(Get-Date -Format yyyyMMdd-HHmm)"
New-Item -ItemType Directory -Force -Path $backup | Out-Null
Push-Location $backup
try {
    foreach ($volume in 'lake-data', 'warehouse-data', 'airflow-db-data', 'grafana-data', 'airflow-auth') {
        docker run --rm -v "publisher-lakehouse_${volume}:/v:ro" -v "${PWD}:/b" alpine:3.22 tar czf "/b/$volume.tar.gz" -C /v .
        if ($LASTEXITCODE -ne 0) { throw "backup of $volume failed" }
    }
} finally {
    Pop-Location
}
```

Then start the stack again as in [Start and stop](#start-and-stop).

**Restore.** Restoring replaces a volume's contents. Remove the containers
first (`down`, **never** `down -v`), then recreate each volume with Compose's
labels so Compose adopts it, and unpack the archive into it:

```powershell
docker compose down
Push-Location 'D:\backups\publisher-lakehouse\20260927-1400'
try {
    foreach ($volume in 'lake-data', 'warehouse-data', 'airflow-db-data', 'grafana-data', 'airflow-auth') {
        docker volume rm "publisher-lakehouse_$volume"
        docker volume create --label com.docker.compose.project=publisher-lakehouse --label "com.docker.compose.volume=$volume" "publisher-lakehouse_$volume" | Out-Null
        docker run --rm -v "publisher-lakehouse_${volume}:/v" -v "${PWD}:/b" alpine:3.22 sh -c "cd /v && tar xzf /b/$volume.tar.gz"
        if ($LASTEXITCODE -ne 0) { throw "restore of $volume failed" }
    }
} finally {
    Pop-Location
}
```

Start the stack as usual. The Postgres volumes come back initialised, so their
init scripts do not run again and the passwords are the ones from backup time.
Restore a single volume by listing only that name. The backup and restore
commands above were checked round-trip on `grafana-data` into a scratch
volume, with identical file checksums.

## Maintenance

`lakehouse_maintenance` runs every Sunday at 03:00 Asia/Colombo (Saturday
21:30 UTC). It is one DockerOperator task that runs
`python -m publisher_lakehouse.transform.maintenance` under the one-slot
`lake_writer` pool, so it never overlaps a lake job. For each of the 11
tracked tables it reads `DESCRIBE DETAIL` and runs `OPTIMIZE` when the table
has more than one file and they average under 32 MB. It checks that the new
history entry is `OPTIMIZE` and that the row count and a SHA-256 over every
row are unchanged, and it prints `files before -> after`, rows and versions
for every table. It never publishes: the next publish sees the new versions
and republishes identical data. The task log holds the per-table report.

Trigger it by hand with `{"dry_run": true}` to see the plan without writing
anything. Like every new DAG it is paused when first created; unpausing it
runs the most recent missed Sunday slot straight away.

**VACUUM.** Maintenance never vacuums by default. To remove data files that
old versions no longer reference, trigger it with
`{"vacuum_hours": 168}` or more. The job refuses anything below 168 hours
(7 days) and never disables Delta's retention check. The cost: time travel
(`versionAsOf`) to any version older than the retention window fails
afterwards, because its files are gone. Publish always reads the latest
versions, so it is unaffected. Only VACUUM when nothing needs to roll back
further than the window, and never while a job that started before the
window could still read old files.

## Known limitations

- **One machine.** Every service, the scraper and the lake share one Windows
  PC and one Docker Desktop VM. There is no high availability; the scraper
  runs only while you are logged on and the PC is awake.
- **One writer.** Delta on S3 has no multi-writer log store here. Safety comes
  from the `lake_writer` pool (one slot) and `max_active_runs=1`. Jobs you run
  by hand bypass the pool, so pause the DAGs first.
- **Name-based author identity.** Authors are keyed by normalized name; there
  is no ORCID. Different people with one name merge, and spelling variants of
  one person split.
- **The manifest has no journal rows.** The pipeline stages only article rows,
  so `ops.scrape_journals` is empty. *Journal freshness* is therefore built
  from the published gold data, not from the scraper manifest.
- **PowerShell 7 is untested.** The producer and scripts are verified in
  Windows PowerShell 5.1 only; `pwsh` is not installed on this machine.
- **Freshness is by day.** Gold stores an article's first-seen date, not its
  timestamp, so *Journal freshness* counts whole days (UTC).
- **Known display defect.** *CC-licensed share by journal (scholarly)* on
  Corpus overview shows one bar (the last journal's share) instead of one bar
  per journal; its data query and `check_grafana.py` are correct.
- **0-byte landed objects.** One 0-byte JSONL part landed before empty files
  were skipped. It stays in place; bronze reads it as empty.

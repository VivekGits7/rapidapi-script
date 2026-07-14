# Dumper Command Reference (`cmd.md`)

Every command for the RapidAPI Auto-Parts dumper — what it means and how to use it.

- **`guvrun`** is the global-venv Python (`/Users/vivek/.global-venv/.venv/bin/python`).
  Always run the CLI as `guvrun -m dumper.cli <command>` — never plain `python`.
- The CLI lives in `dumper/cli.py`. Run any command with `--help` for its options.

---

## The one big idea: one job **per vehicle type**

The catalog has several TecDoc **vehicle types**. We dump three:

| `--vehicle-type` | TecDoc id | Meaning |
|---|---|---|
| `pc`  | 1 | Passenger Car |
| `cv`  | 2 | Commercial Vehicle |
| `bus` | 8 | Bus |

*(You can also pass the raw id, e.g. `--vehicle-type 2`. Full map lives in `dumper/state.py`.)*

Each vehicle type is a **separate, independently-resumable job**. A PC job and a CV job
never touch each other — you can pause one and resume the other any time. The active type
is chosen per run with `--vehicle-type` (default `pc`). It scopes:

- which rows get claimed from `rapid_api_dump_targets` (only that type's rows),
- which `rapid_api_dump_jobs` row is resumed/created,
- the TecDoc `type-id` sent to every API call.

---

## Typical workflow

```bash
# 1) ONE-TIME: load the make/model shortlist into the DB (source of truth)
guvrun -m dumper.cli import-targets --csv archive/new/cv_models.csv  --vehicle-type cv
guvrun -m dumper.cli import-targets --csv archive/new/bus_models.csv --vehicle-type bus

# 2) Crawl a type (creates its job if new, else resumes it)
guvrun -m dumper.cli run --vehicle-type cv

# 3) Check progress / stop / resume anytime
guvrun -m dumper.cli status  --vehicle-type cv
guvrun -m dumper.cli stop    --vehicle-type cv     # graceful pause
guvrun -m dumper.cli resume  --vehicle-type cv     # continue where it stopped
```

PC keeps working exactly as before — omit `--vehicle-type` (defaults to `pc`).

---

## Commands

### `import-targets` — load a make/model CSV into the targets table
ONE-TIME (idempotent) import of a CSV into `rapid_api_dump_targets`, the source of truth.
After importing, the crawl reads the **table**, never the CSV again. Re-running refreshes
names and ADDS new rows; existing statuses stay (resume-safe).

Accepts **both** CSV column layouts automatically:
- PC targets: `tec_manufacturer_id, tec_manufacturer_name, tec_model_id, tec_model_name`
- CV/bus lists: `make, manufacturer_id, model_id, model_name`

| Option | Meaning |
|---|---|
| `--csv <path>` | CSV to import. Default: `DUMP_TARGETS_CSV` from `.env` (the PC list). |
| `--vehicle-type <pc\|cv\|bus\|id>` | Tag every imported row with this type. Default: `pc`. |

```bash
guvrun -m dumper.cli import-targets                                            # PC (default CSV)
guvrun -m dumper.cli import-targets --csv archive/new/cv_models.csv  --vehicle-type cv
guvrun -m dumper.cli import-targets --csv archive/new/bus_models.csv --vehicle-type bus
```

### `run` — crawl the dump
Creates a new job for the chosen vehicle type if none is active, otherwise **resumes** that
type's job. Runs N worker lanes sharing one global rate limit. Resumable and quota-aware —
safe to stop and re-run.

| Option | Meaning |
|---|---|
| `--vehicle-type <pc\|cv\|bus\|id>` | Which type to crawl. Default: `pc`. |
| `--limit <N>` | Process at most N targets this run (`0` = all). `--limit 1` = smoke test. |
| `--workers <N>` | Concurrent make/model workers. Default: `DUMP_WORKERS` (.env). |
| `--rate <req/s>` | Global requests/sec across all workers. Default: `DUMP_RATE_PER_SEC` (capped at the plan's 100/s). |
| `--makes <ids>` | Only these `tec_manufacturer_id`s (comma-separated), e.g. `5,16,21`. |
| `--models <ids>` | Only these `tec_model_id`s (validated against the table; combine with `--makes` for a safety check). |
| `--until <phase>` | **breadth_first only**: stop cleanly AFTER `manufacturers\|models\|vehicles\|categories\|articles\|details`. Resume continues. |
| `--only details` | Skip the listing crawl; only fetch full details for already-stored articles (top `MAX_ARTICLES_PER_CATEGORY` per category; set it to `0` for every article). |

```bash
guvrun -m dumper.cli run --vehicle-type cv                     # full CV crawl
guvrun -m dumper.cli run --vehicle-type cv --limit 1           # smoke test (1 target)
guvrun -m dumper.cli run --vehicle-type cv --makes 2241        # only ASHOK LEYLAND
guvrun -m dumper.cli run --workers 8 --rate 18                 # PC, tuned throughput
```

### `resume` — explicit resume
Same as `run` but **errors** if there's no paused/failed job for that vehicle type (instead
of creating a new one). Takes all the same options as `run`.

```bash
guvrun -m dumper.cli resume --vehicle-type cv
```

### `status` — print job state
Prints the latest job's status, phase, counts, key usage, and errors. Auto-corrects a
job stuck on `running` with a stale heartbeat (crashed worker) to `paused`.

| Option | Meaning |
|---|---|
| `--vehicle-type <...>` | Show the latest job for this type. Omit = latest job of any type. |

```bash
guvrun -m dumper.cli status --vehicle-type cv
```

### `counts` — API + target progress
Prints RapidAPI call totals, this month's usage vs the monthly ceiling, and target
progress (pending / resumable / complete).

| Option | Meaning |
|---|---|
| `--vehicle-type <...>` | Scope target counts to this type. Omit = all types combined. |

```bash
guvrun -m dumper.cli counts --vehicle-type cv
```

### `stop` — graceful pause
Signals the running dump to stop after each worker finishes its current small unit and
commits (writes `stop_requested = TRUE`). Nothing is corrupted; `resume`/`run` continues.

| Option | Meaning |
|---|---|
| `--vehicle-type <...>` | Stop the running job for this type. Omit = latest running job of any type. |

```bash
guvrun -m dumper.cli stop --vehicle-type cv
```

### `reset` — DANGER: wipe everything
Truncates every `rapid_api_*` table and resets sequences. Requires the confirmation flag.
This wipes **all** vehicle types (PC + CV + bus), not just one.

```bash
guvrun -m dumper.cli reset --yes-i-am-sure
```

---

## Running a DB migration

Migrations live in `database/migrations/`. Run one with `psql`, loading `.env` without
printing it:

```bash
set -a && source .env && set +a
PW=$(python3 -c "import os,urllib.parse as u; print(u.quote(os.environ['POSTGRES_DB_PASSWORD']))")
psql "postgresql://$POSTGRES_DB_USER:$PW@$POSTGRES_DB_HOST:$POSTGRES_DB_PORT/$POSTGRES_DB_NAME" \
  -v ON_ERROR_STOP=1 -f database/migrations/<migration_file>.sql
```

`database/catalog.sql` is the source-of-truth schema; every migration is also mirrored into it.

---

## Good to know

- **Store-all, crawl top-N:** every vehicle + article the API returns is STORED; the
  `MAX_VEHICLES_PER_MODEL` / `MAX_ARTICLES_PER_CATEGORY` caps (`.env`) only decide how much
  gets the expensive deep crawl. Widen coverage later by raising a cap — no re-crawl.
- **Diesel filter:** `VEHICLE_FUEL_EXCLUDE_PREFIXES` (default `diesel`) stores diesel
  variants but skips them from the deep crawl. This applies to CV/bus too — most commercial
  vehicles are diesel, so they'll be listed but not parts-crawled until you flip the flag.
- **Resume-safe:** every target moves `pending → resumable → complete`; cursors are written
  only after data commits. Crash / quota-pause / `stop` all continue exactly where they left off.
- **Quota-aware:** at the per-second or monthly limit the job **pauses** (not fails); the
  next `run`/`resume` picks up from the stopping point.

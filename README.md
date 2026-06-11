# RapidAPI Auto-Parts Catalog Dumper

Pulls the **TecDoc parts catalog** (via RapidAPI) into Postgres for the Kalaax
marketplace — bilingual (English + Egyptian Arabic), resumable, and budget-aware.

It powers: seller bulk-upload matching, buyer browse by brand / vehicle /
category, and product pages.

---

## What it dumps — store-all, complete top-N

The **only filter is make + model** (`dump_targets.csv`, 647 rows · 91 makes ·
508 models), processed **A→Z by make then model** (one make finishes before the
next). Everything under a targeted model follows **store-all, complete the top-N**:

| Layer | Rule |
|---|---|
| **Vehicles** | store **ALL** engine variants (ranked latest-first by construction date); fully crawl only the top **`MAX_VEHICLES_PER_MODEL` (=5)** |
| **Categories** | **ALL** leaf categories of each crawled vehicle (every car part) |
| **Articles** | store **ALL** articles per category (with `rank`); fetch full details only for the top **`MAX_ARTICLES_PER_CATEGORY` (=5)** |
| **Details** | `article-complete-details` (one call) → specs + OEM + EAN + image + **all compatible vehicles** |

**Progress is tracked by two enums on vehicles + articles:**
- `dump_state`: `incomplete` | `complete` (complete = dumped through article details)
- `dump_stage`: `listed` → `categories` → `articles` → `details`

**Future extensibility:** want more vehicles per model or more articles per
category later? **Raise the cap and re-run** — rows still `incomplete` get
processed, zero re-fetch (all discovery is already stored).

**Bilingual:** every text endpoint is fetched in EN (lang 4) **and** Arabic
(lang 42) in parallel and merged into `*_en` / `*_ar` columns.

API endpoints + verified request/response JSON: see **`API_DUMP_REFERENCE.md`**.

---

## Setup

1. Python deps: `uv sync` (uses the global venv; aliases `guv`/`guvrun`/`guvtest`).
2. Configure `.env` (copy from `.env.example`):
   - `RAPIDAPI_KEY` — your key (single-key mode = no rotation)
   - `RAPIDAPI_RATE_LIMIT_PER_SEC=20`, `MONTHLY_REQUEST_HARD_LIMIT=100000`
   - `POSTGRES_DB_*` — database connection
   - `DEFAULT_LANG_ID=4`, `ARABIC_LANG_ID=42`, `DEFAULT_COUNTRY_FILTER_ID=80`, `BILINGUAL=true`
   - `MAX_VEHICLES_PER_MODEL=5`, `MAX_ARTICLES_PER_CATEGORY=5`
   - S3 (for the image mirror, optional): `S3_ENABLED`, `AWS_*`, `S3_ARTICLE_MEDIA_FOLDER`
3. Create the schema (**destructive — DROP + rebuild of all `rapid_api_*` tables**):
   ```bash
   ./backup_rapid_api.sh                       # back up current tables first
   set -a && source .env && set +a
   PW=$(python3 -c "import os,urllib.parse as u; print(u.quote(os.environ['POSTGRES_DB_PASSWORD']))")
   psql "postgresql://$POSTGRES_DB_USER:$PW@$POSTGRES_DB_HOST:$POSTGRES_DB_PORT/$POSTGRES_DB_NAME" -f database/catalog.sql
   ```
   Revert anytime with `./restore_rapid_api.sh backups/<file>.sql`.

---

## Run it — CLI

```bash
guvrun -m dumper.cli run --limit 1   # SMOKE TEST: dump only the first target (1 make/model)
guvrun -m dumper.cli run             # dump everything (resumes; pauses at 99,500 calls/month)
guvrun -m dumper.cli resume          # explicitly resume a paused/failed job
guvrun -m dumper.cli status          # latest job state + counts
guvrun -m dumper.cli counts          # API usage, monthly ceiling, target progress
guvrun -m dumper.cli stop            # graceful stop (pauses at next checkpoint)
guvrun -m dumper.cli reset --yes-i-am-sure   # DANGER: truncate all dump tables
```

> **`--limit` = number of `dump_targets.csv` rows (make/model targets)** processed
> this run — **not** vehicles/articles within a target. Omit (or `0`) = process
> all, resuming where it left off. `--limit 1` = one target = a smoke test.

### Run as a server (control surface)

```bash
guv main:app --reload --host 0.0.0.0 --port 8000   # note: --reload (not --relaod)
```
Swagger at `http://localhost:8000/docs`.

---

## API — `/api/dump/*`

| Method | Path | Body / params | Does |
|---|---|---|---|
| POST | `/api/dump/start` | `{ "mode": "run", "limit": 0 }` | start (or resume) in the background |
| POST | `/api/dump/resume` | — | resume the latest paused/failed job |
| GET  | `/api/dump/status` | — | latest job state + counts + key cooldowns |
| GET  | `/api/dump/api-counts` | — | call totals, this month's usage vs ceiling, target progress |
| POST | `/api/dump/stop` | — | graceful stop signal |

**`POST /api/dump/start` body:**
```json
{ "mode": "resume", "limit": 0 }
```
- `mode`: `run` (default — new job, or pick up an in-flight one) or `resume`.
- `limit` (optional): cap targets this run. Omit / `0` = no cap → **resume from
  where it left off** and process everything. `1` = smoke test.

Returns `202` immediately; poll `GET /api/dump/status`.

---

## Images → S3 (separate, run individually)

The dump captures the RapidAPI image URL inline (`api_image_url`) — **no extra
calls**. Mirroring those images to our S3 is a **standalone** step (zero RapidAPI
quota), run on its own:

```bash
guvrun media_rapid_to_s3.py            # mirror all pending images → s3_image_url
guvrun media_rapid_to_s3.py --limit 500
```
Requires `S3_ENABLED=true` + `AWS_*` in `.env` and `boto3` installed.

---

## How resume works

Nothing is marked done until its data commits, so a crash/stop/quota-pause
re-does only the incomplete step:
- **Target:** `rapid_api_dump_targets.status` (pending → resumable → complete)
- **Cursors:** `models.vehicles_fetched_at`, `vehicles.categories_fetched_at`, `vehicle_categories.articles_fetched_at`
- **State flags:** `vehicles.dump_state`, `articles.dump_state` (gate the deep work)
- **Monthly guard:** `rapid_api_monthly_usage` (pauses at the 100k ceiling)

Re-running is free for already-done work.

---

## Layout

```
scripts/
├── main.py                  # FastAPI control surface (/api/dump/*)
├── config.py                # settings (.env)
├── dumper/
│   ├── cli.py               # CLI entry (run/resume/status/counts/stop/reset)
│   ├── runner.py            # orchestrator (line approach + completed model)
│   ├── targets.py           # dump_targets.csv → table, A→Z iterator
│   ├── http_client.py       # rate-limited GET/POST + key cooldowns
│   ├── key_manager.py       # key rotation + monthly-usage guard
│   └── phases/
│       ├── reference.py     # languages / countries / vehicle types
│       ├── manufacturers.py # seed make/model/mvt from targets (no API)
│       ├── deep_crawl.py    # vehicles (EN+AR, ranked) + categories (EN+AR)
│       ├── articles.py      # store-all article lists + rank
│       └── details.py       # article-complete-details (EN+AR) → specs/OEM/compat
├── database/catalog.sql     # schema (UUID, dump_state/stage, bilingual)
├── dump_targets.csv         # the make/model filter (what we dump)
├── media_rapid_to_s3.py     # standalone image → S3 mirror
├── backup_rapid_api.sh      # pg_dump rapid_api_* tables
├── restore_rapid_api.sh     # restore from a backup (revert)
└── API_DUMP_REFERENCE.md    # endpoint-by-endpoint reference
```

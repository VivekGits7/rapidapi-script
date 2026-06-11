# SETUP — quick reference

Everything you need to run, stop, resume, and monitor the dumper. Minimal words.

---

## 1. One-time setup

```bash
cp .env.example .env          # fill RAPIDAPI_KEY + POSTGRES_DB_*
psql ... -f database/catalog.sql                          # fresh DB only (DESTRUCTIVE)
psql ... -f database/migrations/add_product_names.sql     # existing DB (already run ✅)
```

---

## 2. Normal run (recommended — one terminal)

```bash
guvrun -m dumper.cli run --workers 8 --rate 18    # full speed, all 508 targets
guvrun -m dumper.cli run --limit 1                # smoke test (1 target)
guvrun -m dumper.cli run --makes 74,183           # only these manufacturer IDs
```

Defaults live in `.env` (`DUMP_WORKERS=8`, `DUMP_RATE_PER_SEC=18`) — plain `run` is enough.

| Flag | Meaning |
|---|---|
| `--workers 8` | models crawled at the same time |
| `--rate 18` | total req/s, ALL workers combined (plan max 20) |
| `--makes 5,16` | only these `tec_manufacturer_id`s |
| `--limit 10` | stop after 10 targets, pause cleanly |

---

## 3. Three-terminal setup (split by manufacturer)

```bash
# Terminal 1
guvrun -m dumper.cli run --makes 74,183,121 --rate 6 --workers 3
# Terminal 2
guvrun -m dumper.cli run --makes 184,88,5   --rate 6 --workers 3
# Terminal 3
guvrun -m dumper.cli run --makes 16,21,36   --rate 6 --workers 3
```

**Two hard rules:**
1. Make lists must NOT overlap.
2. Rates must sum ≤ 18 (6+6+6 ✅).

| Action | How |
|---|---|
| Stop ONE terminal | `Ctrl+C` in that terminal — or from anywhere: `pkill -INT -f "makes 74,183,121"` (match that terminal's --makes string) |
| Stop ALL terminals | `guvrun -m dumper.cli stop` (global flag — every process pauses) |
| Resume ONE group | re-run the SAME command, same `--makes` |
| Resume everything | one terminal: `guvrun -m dumper.cli run` (no flags = all targets) |

**Know this:** all terminals share one job row. When one group finishes first,
`status` may say `paused` while others still run — ignore it; the targets table
(§5) is the truth. Quota counting stays correct across all processes.

---

## 4. Stop / resume (single terminal)

```bash
guvrun -m dumper.cli stop      # graceful: pauses within ~5s, fully resumable
Ctrl+C                         # also fine — nothing is lost
guvrun -m dumper.cli run       # resume (picks up exactly where it stopped)
guvrun -m dumper.cli resume    # same, but errors if there is nothing to resume
```

Auto-pause (no action needed): monthly quota hit (99,500 calls) · all keys cooling
· `--limit` reached. Resume next month / later with `run`.

---

## 5. Monitor

```bash
guvrun -m dumper.cli status    # job state, phase, counts, key cooldowns
guvrun -m dumper.cli counts    # API calls used vs monthly ceiling, target progress
tail -f logs/dumper.log        # live requests
```

Per-make progress (psql / MCP):

```sql
SELECT tec_manufacturer_name, status, count(*)
FROM rapid_api_dump_targets
GROUP BY 1,2 ORDER BY 1,2;
```

Live throughput check (≈ your --rate when busy):

```bash
grep -E "→ GET" logs/dumper.log | tail -100 | cut -d',' -f1 | uniq -c
```

---

## 6. Data transfer (DB → DB copy)

```bash
guvrun datatransfer.py                 # all rapid_api_* tables
guvrun datatransfer.py --tables '*'    # EVERY table in source (full copy)
guvrun datatransfer.py --tables rapid_api_articles,rapid_api_models
```

- Credentials: `SOURCE_DB_*` / `DEST_DB_*` in `.env`, or `--src-*` / `--dst-*` flags.
- Idempotent: re-run replaces dest tables. Enum types + extensions sync automatically.
- Ends with a row-count parity check per table.

---

## 7. Reset (DANGER)

```bash
guvrun -m dumper.cli reset --yes-i-am-sure   # truncates ALL dump tables
```

---

## 8. Key numbers

| Fact | Value |
|---|---|
| Plan limits | 20 req/s · 100k calls/month (pauses at 99.5k) |
| Full dump cost | ~1.67M calls ≈ 17 months on 1 key |
| Month's quota at 18 req/s | gone in ~92 min, then auto-pause |
| One model | ~3,300 calls ≈ 3 min |
| Caps | `MAX_VEHICLES_PER_MODEL=5` · `MAX_ARTICLES_PER_CATEGORY=2` (raise later + re-run = fills the rest, no re-fetch) |

Deep details: `README.md` · design rationale: `archive/OPTIMIZATION_PLAN.md`.

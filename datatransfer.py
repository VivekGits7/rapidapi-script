"""datatransfer — copy specific Postgres tables (schema + data + indexes +
constraints) from a SOURCE database to a DESTINATION database.

Wraps `pg_dump | psql`, so the transfer is faithful (not just rows — indexes,
PKs, FKs, defaults all come across). Idempotent: the dump uses `--clean
--if-exists`, so re-running cleanly replaces the destination tables. It also
creates the `pg_trgm` + `pgcrypto` extensions on the destination first (trigram
GIN indexes need pg_trgm).

WHICH TABLES — you pick them. Set the `TABLES` list below, or pass `--tables`.
If left empty, it transfers every `rapid_api_*` table found in the source.
Put "*" in the list (or `--tables '*'`) to transfer EVERY table in the source's
public schema — a full, faithful copy of all tables + data.

Give credentials any of three ways (first found wins, per field):
  1. CLI flags:  --src-host / --dst-host / --tables …  (see --help)
  2. The SOURCE / DEST dicts below (edit them directly)
  3. Dedicated env vars: SOURCE_DB_HOST/PORT/NAME/USER/PASSWORD and
     DEST_DB_HOST/PORT/NAME/USER/PASSWORD  (independent of the app's POSTGRES_DB_*)

Examples:
    guvrun datatransfer.py                                   # tables from the list / all rapid_api_*
    guvrun datatransfer.py --tables rapid_api_articles,rapid_api_models
    guvrun datatransfer.py --src-host 2.24.8.69 --src-port 6001 \
        --src-db kalaaxdb_sandbox --src-user kalaaxuser --src-password '***' \
        --dst-host localhost --dst-port 5433 --dst-db autoparts \
        --dst-user admin --dst-password secret --tables rapid_api_articles

Requires `pg_dump` + `psql` (PostgreSQL client / libpq) on PATH.
"""

import argparse
import os
import shutil
import subprocess
import sys
import tempfile

try:
    from dotenv import load_dotenv
    load_dotenv()  # lets you keep SOURCE_DB_* / DEST_DB_* in this folder's .env
except Exception:
    pass

# ==================== EDIT HERE (or use SOURCE_DB_* / DEST_DB_* env, or CLI) ====================
# SOURCE — the database you're copying FROM.
SOURCE = {
    "host":     os.getenv("SOURCE_DB_HOST", "localhost"),
    "port":     os.getenv("SOURCE_DB_PORT", "5432"),
    "dbname":   os.getenv("SOURCE_DB_NAME", ""),
    "user":     os.getenv("SOURCE_DB_USER", ""),
    "password": os.getenv("SOURCE_DB_PASSWORD", ""),
}

# DESTINATION — the database you're copying INTO.
DEST = {
    "host":     os.getenv("DEST_DB_HOST", "localhost"),
    "port":     os.getenv("DEST_DB_PORT", "5433"),
    "dbname":   os.getenv("DEST_DB_NAME", "autoparts"),
    "user":     os.getenv("DEST_DB_USER", "admin"),
    "password": os.getenv("DEST_DB_PASSWORD", "secret"),
}

# TABLES to transfer. List the exact table names you want.
# Leave EMPTY ([]) to transfer every rapid_api_* table found in the source.
# Put "*" to transfer EVERY table in the source's public schema (full copy).
TABLES: list[str] = [
    "*" 
    # "*",
    # "rapid_api_articles",
    # "rapid_api_models",
]

FALLBACK_PATTERN = "rapid_api_*"            # used only when TABLES is empty
# Always created on dest; the source's installed extensions are ALSO synced
# dynamically before restore (e.g. uuid-ossp for uuid_generate_v4 defaults).
DEST_EXTENSIONS = ["pg_trgm", "pgcrypto"]


# ==================== helpers ====================
def _bin(name: str) -> str:
    """Locate a libpq binary (pg_dump/psql), falling back to Homebrew's libpq."""
    found = shutil.which(name)
    if found:
        return found
    for cand in (f"/opt/homebrew/opt/libpq/bin/{name}", f"/usr/local/opt/libpq/bin/{name}"):
        if os.path.exists(cand):
            return cand
    sys.exit(f"ERROR: '{name}' not found. Install the PostgreSQL client (e.g. `brew install libpq`).")


def _env(creds: dict) -> dict:
    e = os.environ.copy()
    e["PGPASSWORD"] = creds["password"]
    e["PGCONNECT_TIMEOUT"] = "10"
    return e


def _psql_scalar(psql: str, creds: dict, sql: str) -> str:
    out = subprocess.run(
        [psql, "-h", creds["host"], "-p", str(creds["port"]), "-U", creds["user"],
         "-d", creds["dbname"], "-tAc", sql],
        env=_env(creds), capture_output=True, text=True,
    )
    if out.returncode != 0:
        raise RuntimeError(out.stderr.strip())
    return out.stdout.strip()


def _discover_rapid_api(psql: str, creds: dict) -> list:
    names = _psql_scalar(
        psql, creds,
        "SELECT string_agg(table_name, ',') FROM information_schema.tables "
        "WHERE table_schema='public' AND table_name LIKE 'rapid_api\\_%';",
    )
    return [n for n in (names or "").split(",") if n]


def _discover_all_tables(psql: str, creds: dict) -> list:
    """Every base table in the source's public schema that the user can READ.

    The privilege filter matters: information_schema lists tables you can't
    SELECT (e.g. owned by another role) — pg_dump would die on those.
    """
    names = _psql_scalar(
        psql, creds,
        "SELECT string_agg(table_name, ',') FROM information_schema.tables "
        "WHERE table_schema='public' AND table_type='BASE TABLE' "
        "AND has_table_privilege(format('%I.%I', table_schema, table_name), 'SELECT');",
    )
    return [n for n in (names or "").split(",") if n]


def _count(psql: str, creds: dict, table: str) -> int:
    try:
        return int(_psql_scalar(psql, creds, f'SELECT count(*) FROM public."{table}";'))
    except Exception:
        return -1  # table missing


def _sync_enum_types(psql: str, src: dict, dst: dict) -> None:
    """Create the source's custom enum types on dest (pg_dump --table omits them).

    Without this, restoring into a FRESH database fails with
    'type "public.rapid_api_dump_state" does not exist'. Existing types on dest
    are left untouched (duplicate_object is swallowed).
    """
    rows = _psql_scalar(
        psql, src,
        "SELECT string_agg(t.typname || '|' || vals, ';') FROM ("
        "  SELECT t.typname, string_agg(quote_literal(e.enumlabel), ', ' ORDER BY e.enumsortorder) AS vals"
        "  FROM pg_type t"
        "  JOIN pg_enum e ON e.enumtypid = t.oid"
        "  JOIN pg_namespace n ON n.oid = t.typnamespace"
        "  WHERE n.nspname = 'public'"
        "  GROUP BY t.typname) t;",
    )
    if not rows:
        return
    for entry in rows.split(";"):
        if "|" not in entry:
            continue
        typname, vals = entry.split("|", 1)
        try:
            _psql_scalar(
                psql, dst,
                f"DO $$ BEGIN CREATE TYPE public.\"{typname}\" AS ENUM ({vals}); "
                f"EXCEPTION WHEN duplicate_object THEN NULL; END $$;",
            )
            print(f"  enum type synced: {typname}")
        except Exception as e:
            print(f"  warn: could not sync enum type {typname}: {e}")


# ==================== main ====================
def _sync_trigger_functions(psql: str, dst: dict) -> None:
    """Create the browse trigger functions on dest. pg_dump --table carries a table's triggers but not the
    functions they call, so a fresh dest would otherwise fail on the CREATE TRIGGER lines of the restore."""
    from dumper.schema import TRIGGER_FUNCTIONS_SQL

    fd, path = tempfile.mkstemp(prefix="datatransfer_fn_", suffix=".sql")
    os.close(fd)
    with open(path, "w", encoding="utf-8") as f:
        f.write(TRIGGER_FUNCTIONS_SQL)
    try:
        rc = subprocess.run(
            [psql, "-h", dst["host"], "-p", str(dst["port"]), "-U", dst["user"], "-d", dst["dbname"],
             "-v", "ON_ERROR_STOP=1", "-q", "-f", path],
            env=_env(dst),
        ).returncode
        if rc != 0:
            print("  warn: could not create the browse trigger functions on dest; the restore may fail on its triggers")
    finally:
        os.remove(path)


def main() -> None:
    ap = argparse.ArgumentParser(description="Transfer specific tables between two Postgres DBs.")
    for side, d in (("src", SOURCE), ("dst", DEST)):
        ap.add_argument(f"--{side}-host", default=d["host"])
        ap.add_argument(f"--{side}-port", default=d["port"])
        ap.add_argument(f"--{side}-db", default=d["dbname"])
        ap.add_argument(f"--{side}-user", default=d["user"])
        ap.add_argument(f"--{side}-password", default=d["password"])
    ap.add_argument("--tables", default="", help="Comma-separated table names. Empty = all rapid_api_* tables. '*' = ALL tables in the source's public schema.")
    ap.add_argument("--keep-file", action="store_true", help="Keep the intermediate .sql dump file")
    args = ap.parse_args()

    src = {"host": args.src_host, "port": args.src_port, "dbname": args.src_db,
           "user": args.src_user, "password": args.src_password}
    dst = {"host": args.dst_host, "port": args.dst_port, "dbname": args.dst_db,
           "user": args.dst_user, "password": args.dst_password}

    if not src["dbname"] or not src["user"]:
        sys.exit("ERROR: source db/user is empty. Set the SOURCE dict, SOURCE_DB_* env, or --src-* flags.")
    if not dst["dbname"] or not dst["user"]:
        sys.exit("ERROR: destination db/user is empty. Set the DEST dict, DEST_DB_* env, or --dst-* flags.")

    pg_dump, psql = _bin("pg_dump"), _bin("psql")

    # Resolve the table list: --tables > TABLES dict > all rapid_api_*
    tables = [t.strip() for t in (args.tables.split(",") if args.tables else TABLES) if t.strip()]

    print(f"SOURCE : {src['user']}@{src['host']}:{src['port']}/{src['dbname']}")
    print(f"DEST   : {dst['user']}@{dst['host']}:{dst['port']}/{dst['dbname']}")

    # connectivity check on both ends
    try:
        _psql_scalar(psql, src, "SELECT 1;")
    except Exception as e:
        sys.exit(f"ERROR: cannot reach SOURCE — {e}")
    try:
        _psql_scalar(psql, dst, "SELECT 1;")
    except Exception as e:
        sys.exit(f"ERROR: cannot reach DEST — {e}")

    star_mode = "*" in tables
    if star_mode:
        # '*' = full copy: every READABLE base table in the source's public schema.
        tables = _discover_all_tables(psql, src)
        if not tables:
            sys.exit("ERROR: SOURCE has no readable tables in the public schema. Nothing to transfer.")
        print(f"TABLES : (* — ALL public tables) {len(tables)} tables")
    elif not tables:
        tables = _discover_rapid_api(psql, src)
        if not tables:
            sys.exit("ERROR: no rapid_api_* tables in SOURCE and no --tables given. Nothing to transfer.")
        print(f"TABLES : (all rapid_api_*) {len(tables)} tables")
    else:
        print(f"TABLES : {', '.join(tables)}")

    src_counts = {t: _count(psql, src, t) for t in tables}
    missing = [t for t, c in src_counts.items() if c < 0]
    if missing and star_mode:
        # '*' mode: best-effort — skip tables we can't read (permissions/RLS) and continue.
        print(f"  warn: skipping {len(missing)} unreadable table(s): {', '.join(missing)}")
        tables = [t for t in tables if src_counts[t] >= 0]
        src_counts = {t: src_counts[t] for t in tables}
        if not tables:
            sys.exit("ERROR: no readable tables left to transfer.")
    elif missing:
        sys.exit(f"ERROR: these tables don't exist in SOURCE (or aren't readable): {', '.join(missing)}")
    print(f"Source rows: {sum(src_counts.values())} across {len(tables)} tables.\n")

    # ensure required extensions on DEST: the static list + everything installed
    # on SOURCE (table defaults like uuid_generate_v4() need uuid-ossp, etc.)
    src_exts = []
    try:
        names = _psql_scalar(psql, src, "SELECT string_agg(extname, ',') FROM pg_extension WHERE extname <> 'plpgsql';")
        src_exts = [n for n in (names or "").split(",") if n]
    except Exception as e:
        print(f"  warn: could not list source extensions: {e}")
    for ext in dict.fromkeys(DEST_EXTENSIONS + src_exts):  # dedup, keep order
        try:
            _psql_scalar(psql, dst, f'CREATE EXTENSION IF NOT EXISTS "{ext}";')
        except Exception as e:
            print(f"  warn: could not create extension {ext} on dest: {e}")

    # ensure the source's custom enum types exist on DEST (pg_dump --table omits
    # them; a fresh dest DB would otherwise fail on 'type ... does not exist')
    _sync_enum_types(psql, src, dst)
    _sync_trigger_functions(psql, dst)

    # dump SOURCE → temp .sql (clean+if-exists makes the restore idempotent)
    fd, dump_path = tempfile.mkstemp(prefix="datatransfer_", suffix=".sql")
    os.close(fd)
    table_args = []
    for t in tables:
        table_args += ["--table", t]
    print(f"Dumping {len(tables)} table(s) → {dump_path} ...")
    rc = subprocess.run(
        [pg_dump, "-h", src["host"], "-p", str(src["port"]), "-U", src["user"], "-d", src["dbname"],
         "--no-owner", "--no-privileges", "--clean", "--if-exists", *table_args, "-f", dump_path],
        env=_env(src),
    ).returncode
    if rc != 0:
        sys.exit("ERROR: pg_dump failed.")

    # Clear competing connections to the dest DB first — otherwise the restore's
    # DROP TABLE (from --clean) blocks forever on a lock held by some other session
    # (e.g. an IDE DB explorer connected to the dest). This is the usual "stuck
    # after dumping" cause.
    try:
        killed = _psql_scalar(
            psql, dst,
            "SELECT count(*) FROM ("
            "SELECT pg_terminate_backend(pid) FROM pg_stat_activity "
            "WHERE datname = current_database() AND pid <> pg_backend_pid()) x;",
        )
        if killed and int(killed) > 0:
            print(f"  cleared {killed} other connection(s) to dest before restore")
    except Exception as e:
        print(f"  warn: could not clear dest connections: {e}")

    # restore into DEST. lock_timeout makes a still-blocked DROP fail fast (clear
    # error) instead of hanging silently.
    print("Restoring into destination ...")
    restore_env = _env(dst)
    restore_env["PGOPTIONS"] = "-c lock_timeout=15000"
    rc = subprocess.run(
        [psql, "-h", dst["host"], "-p", str(dst["port"]), "-U", dst["user"], "-d", dst["dbname"],
         "-v", "ON_ERROR_STOP=1", "-q", "-f", dump_path],
        env=restore_env,
    ).returncode
    if rc != 0:
        sys.exit(f"ERROR: restore failed. Dump kept at {dump_path}")

    # verify per-table row parity
    print("\nVerifying row counts ...")
    ok = True
    for t in tables:
        s, d = src_counts[t], _count(psql, dst, t)
        if s != d:
            ok = False
        print(f"  [{'OK ' if s == d else 'MISMATCH'}] {t}: src={s} dst={d}")

    if not args.keep_file:
        os.remove(dump_path)
    print("\n" + ("✅ DONE — faithful copy, all tables match." if ok
                  else "⚠️  DONE with MISMATCHES — check the rows above."))
    sys.exit(0 if ok else 1)


if __name__ == "__main__":
    main()

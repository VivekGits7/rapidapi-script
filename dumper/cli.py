"""CLI entry point for the dumper.

Usage:
    guvrun -m dumper.cli import-targets                                           # ONE-TIME: load PC dump_targets.csv (source of truth)
    guvrun -m dumper.cli import-targets --csv archive/new/cv_models.csv --vehicle-type cv    # load CV models
    guvrun -m dumper.cli import-targets --csv archive/new/bus_models.csv --vehicle-type bus  # load bus models
    guvrun -m dumper.cli run                        # one-shot PC dump (resumes the PC job if in flight)
    guvrun -m dumper.cli run --vehicle-type cv      # crawl the CV job (independent of PC — its own resumable job)
    guvrun -m dumper.cli run --workers 8 --rate 18  # parallel crawl at ~18 req/s (defaults from .env)
    guvrun -m dumper.cli run --makes 5,16,21        # only these tec_manufacturer_ids
    guvrun -m dumper.cli resume --vehicle-type cv   # explicit resume of the CV job (errors if none resumable)
    guvrun -m dumper.cli status --vehicle-type cv   # print the CV job state
    guvrun -m dumper.cli stop --vehicle-type cv     # stop the running CV dump gracefully
    guvrun -m dumper.cli reset --yes-i-am-sure      # DANGEROUS: wipe all dumped data + sequences

Each vehicle type (pc | cv | bus | ...) is a SEPARATE, independently-resumable job:
resume PC and CV whenever you want without one touching the other.
"""

import asyncio
import json
import sys
from typing import Optional

import click

from logger import get_logger, setup_logging

setup_logging()
logger = get_logger("dumper.cli")


def _print_json(data: dict | list) -> None:
    click.echo(json.dumps(data, indent=2, default=str))


def _parse_ids(value: Optional[str], flag: str) -> Optional[list[int]]:
    """'5,16,21' → [5, 16, 21]; None/empty → None (no filter)."""
    if not value:
        return None
    try:
        ids = [int(tok.strip()) for tok in value.split(",") if tok.strip()]
    except ValueError:
        raise click.BadParameter(f"{flag} must be comma-separated integers, got: {value!r}")
    return ids or None


def _resolve_vehicle_type(value: Optional[str]) -> Optional[int]:
    """'pc'/'cv'/'bus'/... (any case) or '1'/'2'/'8' → TecDoc vehicle type id.
    None/empty → None (caller defaults to PC / settings.DEFAULT_TYPE_ID)."""
    from dumper.state import CODE_TO_TYPE_ID, TYPE_ID_TO_CODE

    if not value:
        return None
    v = value.strip().upper()
    if v in CODE_TO_TYPE_ID:
        return CODE_TO_TYPE_ID[v]
    try:
        tid = int(v)
    except ValueError:
        raise click.BadParameter(
            f"--vehicle-type must be a code ({', '.join(CODE_TO_TYPE_ID)}) or a numeric id, got: {value!r}"
        )
    if tid not in TYPE_ID_TO_CODE:
        raise click.BadParameter(f"--vehicle-type id {tid} unknown; valid ids: {sorted(TYPE_ID_TO_CODE)}")
    return tid


_run_options = [
    click.option("--vehicle-type", "vehicle_type", type=str, default=None, help="Which vehicle type to crawl: pc | cv | bus | ... or the numeric TecDoc id (default: pc). Scopes the job + targets — PC and CV are independent, each with its own resumable job."),
    click.option("--limit", type=int, default=0, help="Process at most N targets this run (0 = all). Use --limit 1 for a smoke test."),
    click.option("--workers", type=int, default=None, help="Concurrent target workers (default: DUMP_WORKERS from .env)."),
    click.option("--rate", type=float, default=None, help="Global req/s across all workers (default: DUMP_RATE_PER_SEC; clamped to the plan's 100/s)."),
    click.option("--makes", type=str, default=None, help="Comma-separated tec_manufacturer_ids to crawl (e.g. 5,16,21). Omit = all."),
    click.option("--models", type=str, default=None, help="Comma-separated tec_model_ids to crawl (e.g. 4635,10615). Validated against the CSV; combine with --makes for a safety check."),
    click.option("--until", type=click.Choice(["manufacturers", "models", "vehicles", "categories", "articles", "details"]),
                 default=None, help="breadth_first only: stop cleanly AFTER this phase (e.g. categories). Resume later to continue. Omit = all phases."),
    click.option("--only", type=click.Choice(["details"]), default=None,
                 help="Run ONLY this phase, skipping the listing crawl. `--only details` fetches full details for ALL already-stored articles (top MAX_ARTICLES_PER_CATEGORY per category; set it to 0 for every article) and includes already-'complete' targets. Omit = normal full crawl."),
]


def _with_run_options(f):
    for opt in reversed(_run_options):
        f = opt(f)
    return f


@click.group()
def cli():
    """RapidAPI Auto Parts Catalog dumper."""


@cli.command()
@_with_run_options
def run(vehicle_type: Optional[str], limit: int, workers: Optional[int], rate: Optional[float], makes: Optional[str], models: Optional[str], until: Optional[str], only: Optional[str]):
    """Run the dump — creates a new job (per vehicle type) if none active; otherwise resumes that type's job."""
    from dumper.runner import dump_main

    try:
        result = asyncio.run(
            dump_main(mode="run", limit=limit, workers=workers, rate=rate,
                      makes=_parse_ids(makes, "--makes"), models=_parse_ids(models, "--models"), until=until, only=only,
                      vehicle_type_id=_resolve_vehicle_type(vehicle_type))
        )
        _print_json(result)
    except KeyboardInterrupt:
        click.echo("\nForce-quit — job left as-is; `run` resumes it", err=True)
        sys.exit(130)
    except Exception as e:
        click.echo(f"ERROR: {e}", err=True)
        logger.error("CLI run failed", exc_info=True)
        sys.exit(1)


@cli.command()
@_with_run_options
def resume(vehicle_type: Optional[str], limit: int, workers: Optional[int], rate: Optional[float], makes: Optional[str], models: Optional[str], until: Optional[str], only: Optional[str]):
    """Resume the latest paused/failed job FOR THIS vehicle type. Errors if there's nothing to resume."""
    from dumper.runner import dump_main

    try:
        result = asyncio.run(
            dump_main(mode="resume", limit=limit, workers=workers, rate=rate,
                      makes=_parse_ids(makes, "--makes"), models=_parse_ids(models, "--models"), until=until, only=only,
                      vehicle_type_id=_resolve_vehicle_type(vehicle_type))
        )
        _print_json(result)
    except KeyboardInterrupt:
        click.echo("\nForce-quit — job left as-is; `run` resumes it", err=True)
        sys.exit(130)
    except Exception as e:
        click.echo(f"ERROR: {e}", err=True)
        logger.error("CLI resume failed", exc_info=True)
        sys.exit(1)


@cli.command(name="import-targets")
@click.option("--csv", "csv_path", type=str, default=None, help="Path to the targets CSV (default: DUMP_TARGETS_CSV from .env). Accepts PC (tec_*) or CV/bus (make/manufacturer_id/model_*) columns.")
@click.option("--vehicle-type", "vehicle_type", type=str, default=None, help="Tag imported rows with this vehicle type: pc | cv | bus | ... or numeric id (default: pc).")
def import_targets(csv_path: Optional[str], vehicle_type: Optional[str]):
    """ONE-TIME import of a make/model CSV into rapid_api_dump_targets.

    The table is the source of truth after this — the dump no longer reads the CSV.
    Idempotent: re-running refreshes names and ADDS new rows; existing statuses stay.
    Every imported row is tagged with --vehicle-type so PC/CV/bus coexist and each
    is crawled by its own independently-resumable job.
    """
    from dumper import targets
    from services.db import close_db_pool, create_db_pool

    vt = _resolve_vehicle_type(vehicle_type)

    async def _run() -> dict:
        await create_db_pool()
        try:
            return await targets.import_targets_from_csv(csv_path, vehicle_type_id=vt)
        finally:
            await close_db_pool()

    try:
        _print_json(asyncio.run(_run()))
    except Exception as e:
        click.echo(f"ERROR: {e}", err=True)
        logger.error("CLI import-targets failed", exc_info=True)
        sys.exit(1)


@cli.command()
@click.option("--vehicle-type", "vehicle_type", type=str, default=None, help="Show the latest job for this vehicle type (pc | cv | bus | ... or id). Omit = latest job of any type.")
def status(vehicle_type: Optional[str]):
    """Print the latest dump-job state from the DB. Safe to run anytime."""
    from dumper.runner import get_status

    try:
        state = asyncio.run(get_status(vehicle_type_id=_resolve_vehicle_type(vehicle_type)))
        _print_json(state)
    except Exception as e:
        click.echo(f"ERROR: {e}", err=True)
        sys.exit(1)


@cli.command()
@click.option("--vehicle-type", "vehicle_type", type=str, default=None, help="Scope target progress to this vehicle type (pc | cv | bus | ... or id). Omit = all types.")
def counts(vehicle_type: Optional[str]):
    """Print RapidAPI call totals, this month's usage vs ceiling, and target progress."""
    from dumper.runner import get_api_counts

    try:
        _print_json(asyncio.run(get_api_counts(vehicle_type_id=_resolve_vehicle_type(vehicle_type))))
    except Exception as e:
        click.echo(f"ERROR: {e}", err=True)
        sys.exit(1)


@cli.command()
@click.option("--vehicle-type", "vehicle_type", type=str, default=None, help="Stop the running job for this vehicle type (pc | cv | bus | ... or id). Omit = latest running job of any type.")
def stop(vehicle_type: Optional[str]):
    """Signal the running dump to stop gracefully (writes stop_requested = TRUE)."""
    from dumper.runner import request_stop

    try:
        result = asyncio.run(request_stop(vehicle_type_id=_resolve_vehicle_type(vehicle_type)))
        _print_json(result)
    except Exception as e:
        click.echo(f"ERROR: {e}", err=True)
        sys.exit(1)


@cli.command()
@click.option(
    "--yes-i-am-sure",
    "yes_i_am_sure",
    is_flag=True,
    default=False,
    help="Required confirmation — without this flag, reset refuses to run.",
)
def reset(yes_i_am_sure: bool):
    """DANGEROUS: truncate every rapid_api_* table and reset all sequences."""
    if not yes_i_am_sure:
        click.echo("Refusing to reset without --yes-i-am-sure", err=True)
        sys.exit(1)
    from dumper.runner import reset_dump

    try:
        result = asyncio.run(reset_dump())
        _print_json(result)
    except Exception as e:
        click.echo(f"ERROR: {e}", err=True)
        sys.exit(1)


@cli.command(name="ensure-schema")
@click.option("--backfill", is_flag=True, default=False, help="Also backfill the link table from the existing link rows (hours on a slow disk; backfill_category_links.py shows progress).")
@click.option("--reconcile", is_flag=True, default=False, help="Force a full per category backfill pass even if one already completed (walks the whole link index).")
def ensure_schema(backfill: bool, reconcile: bool):
    """Create or repair the browse and search objects the Kalaax backend reads. Safe anytime; run does this too."""
    from dumper.schema import ensure_browse_schema
    from dumper.search_sync import ensure_search_schema
    from services.db import close_db_pool, create_db_pool

    async def _run() -> dict:
        await create_db_pool(max_size=2)
        try:
            browse = await ensure_browse_schema(backfill=backfill, reconcile=reconcile)
            search = await ensure_search_schema()
            return {"browse": browse, "search": search}
        finally:
            await close_db_pool()

    try:
        _print_json(asyncio.run(_run()))
    except Exception as e:
        click.echo(f"ERROR: {e}", err=True)
        logger.error("CLI ensure-schema failed", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    cli()

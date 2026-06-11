"""CLI entry point for the dumper.

Usage:
    guvrun -m dumper.cli run                       # one-shot full dump (resumes if a job is in flight)
    guvrun -m dumper.cli resume                    # explicit resume (errors if no resumable job)
    guvrun -m dumper.cli status                    # print latest job state
    guvrun -m dumper.cli stop                      # signal running dump to stop gracefully
    guvrun -m dumper.cli reset --yes-i-am-sure     # DANGEROUS: wipe all dumped data + sequences
"""

import asyncio
import json
import sys

import click

from logger import get_logger, setup_logging

setup_logging()
logger = get_logger("dumper.cli")


def _print_json(data: dict | list) -> None:
    click.echo(json.dumps(data, indent=2, default=str))


@click.group()
def cli():
    """RapidAPI Auto Parts Catalog dumper."""


@cli.command()
@click.option("--limit", type=int, default=0, help="Process at most N targets this run (0 = all). Use --limit 1 for a smoke test.")
def run(limit: int):
    """Run the dump — creates a new job if none active; otherwise resumes."""
    from dumper.runner import dump_main

    try:
        result = asyncio.run(dump_main(mode="run", limit=limit))
        _print_json(result)
    except KeyboardInterrupt:
        click.echo("\nInterrupted — use `cli stop` for a graceful shutdown next time", err=True)
        sys.exit(130)
    except Exception as e:
        click.echo(f"ERROR: {e}", err=True)
        logger.error("CLI run failed", exc_info=True)
        sys.exit(1)


@cli.command()
def resume():
    """Resume the latest paused/failed job. Errors if there's nothing to resume."""
    from dumper.runner import dump_main

    try:
        result = asyncio.run(dump_main(mode="resume"))
        _print_json(result)
    except KeyboardInterrupt:
        click.echo("\nInterrupted — use `cli stop` for a graceful shutdown next time", err=True)
        sys.exit(130)
    except Exception as e:
        click.echo(f"ERROR: {e}", err=True)
        logger.error("CLI resume failed", exc_info=True)
        sys.exit(1)


@cli.command()
def status():
    """Print the latest dump-job state from the DB. Safe to run anytime."""
    from dumper.runner import get_status

    try:
        state = asyncio.run(get_status())
        _print_json(state)
    except Exception as e:
        click.echo(f"ERROR: {e}", err=True)
        sys.exit(1)


@cli.command()
def counts():
    """Print RapidAPI call totals, this month's usage vs ceiling, and target progress."""
    from dumper.runner import get_api_counts

    try:
        _print_json(asyncio.run(get_api_counts()))
    except Exception as e:
        click.echo(f"ERROR: {e}", err=True)
        sys.exit(1)


@cli.command()
def stop():
    """Signal the running dump to stop gracefully (writes stop_requested = TRUE)."""
    from dumper.runner import request_stop

    try:
        result = asyncio.run(request_stop())
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


if __name__ == "__main__":
    cli()

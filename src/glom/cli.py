"""rich-click CLI for glom: index & search agent context."""

from __future__ import annotations

import json
from datetime import datetime

import rich_click as click
from rich import box
from rich.console import Console
from rich.panel import Panel
from rich.progress import (
    BarColumn,
    MofNCompleteColumn,
    Progress,
    SpinnerColumn,
    TextColumn,
    TimeElapsedColumn,
)
from rich.table import Table

from glom.db import Database
from glom.indexer import index_all

console = Console(stderr=True)

_KIND_STYLE: dict[str, str] = {
    "memory": "green",
    "plan": "blue",
    "task": "yellow",
    "instructions": "magenta",
    "settings": "cyan",
    "skill": "red",
    "session": "dim white",
    "history": "dim yellow",
}


# ── helpers ──────────────────────────────────────────────────────────────────

def _human(n: int | float) -> str:
    for unit in ("B", "KB", "MB", "GB"):
        if abs(n) < 1024:
            return f"{n:.0f} {unit}" if unit == "B" else f"{n:.1f} {unit}"
        n /= 1024
    return f"{n:.1f} TB"


def _fts_error(exc: Exception) -> bool:
    msg = str(exc).lower()
    return "fts5" in msg or "syntax" in msg or "parse" in msg


def _highlight_snippet(text: str) -> str:
    return text.replace("»", "[bold yellow]").replace("«", "[/bold yellow]")


# ── CLI ──────────────────────────────────────────────────────────────────────

@click.group()
@click.version_option(package_name="glom")
def main() -> None:
    """Index and search agent context from ~/.claude and ~/.codex."""


# ── index ────────────────────────────────────────────────────────────────────

@main.command()
@click.option("--full", is_flag=True, help="Force full re-index (ignore mtimes).")
@click.option("--json", "as_json", is_flag=True, help="Emit JSON to stdout.")
def index(full: bool, as_json: bool) -> None:
    """Walk ~/.claude and ~/.codex, index every discoverable file."""
    db = Database()

    with Progress(
        SpinnerColumn(),
        TextColumn("[progress.description]{task.description}"),
        BarColumn(),
        MofNCompleteColumn(),
        TimeElapsedColumn(),
        console=console,
        disable=as_json,
    ) as progress:
        task = progress.add_task("Scanning", total=None)

        def on_progress(done: int, total: int) -> None:
            progress.update(task, completed=done, total=total)

        def on_phase(phase: str) -> None:
            labels = {
                "scanning": "Scanning",
                "indexing": "Indexing",
                "rebuilding": "Rebuilding FTS",
            }
            label = labels.get(phase, phase)
            if phase == "rebuilding":
                # indeterminate spinner while FTS rebuilds
                progress.update(task, description=label, completed=0, total=None)
            else:
                progress.update(task, description=label)

        stats = index_all(
            db, full=full,
            on_progress=on_progress, on_phase=on_phase,
        )

    db.close()

    if as_json:
        click.echo(json.dumps({
            "new": stats.new, "updated": stats.updated,
            "unchanged": stats.unchanged, "deleted": stats.deleted,
            "errors": stats.errors, "total": stats.total_processed,
            "tool_calls": stats.tool_calls_extracted,
        }))
        return

    parts: list[str] = []
    if stats.new:
        parts.append(f"[green]+{stats.new} new[/]")
    if stats.updated:
        parts.append(f"[yellow]~{stats.updated} updated[/]")
    if stats.unchanged:
        parts.append(f"[dim]{stats.unchanged} unchanged[/]")
    if stats.deleted:
        parts.append(f"[red]-{stats.deleted} deleted[/]")
    if stats.errors:
        parts.append(f"[bold red]{stats.errors} errors[/]")
    summary = f"Indexed {stats.total_processed} documents: {', '.join(parts)}"
    if stats.tool_calls_extracted:
        summary += f"  [dim]({stats.tool_calls_extracted:,} tool calls)[/]"
    console.print(summary)
    for ep in stats.error_paths[:5]:
        console.print(f"  [red]![/] {ep}")


# ── search ───────────────────────────────────────────────────────────────────

@main.command()
@click.argument("query")
@click.option("-k", "--kind", help="Filter by document kind.")
@click.option("-p", "--project", help="Filter by project slug (substring).")
@click.option("-s", "--source", type=click.Choice(["claude", "codex"]),
              help="Filter by source.")
@click.option("-n", "--limit", default=10, show_default=True,
              help="Maximum results.")
@click.option("--json", "as_json", is_flag=True, help="Emit JSON to stdout.")
def search(query: str, kind: str | None, project: str | None,
           source: str | None, limit: int, as_json: bool) -> None:
    """Full-text search over the index (FTS5, BM25-ranked)."""
    db = Database()
    try:
        results = db.search(
            query, kind=kind, project=project, source=source, limit=limit,
        )
    except Exception as exc:
        if _fts_error(exc):
            console.print(f"[red]Bad query:[/] {exc}")
            raise SystemExit(1) from None
        raise
    finally:
        db.close()

    if as_json:
        click.echo(json.dumps([
            {"path": r.path, "kind": r.kind, "source": r.source,
             "project": r.project, "title": r.title,
             "snippet": r.snippet, "rank": r.rank, "size": r.size}
            for r in results
        ], indent=2))
        return

    if not results:
        console.print("[dim]No results.[/]")
        return

    for i, r in enumerate(results, 1):
        style = _KIND_STYLE.get(r.kind, "white")
        header = (
            f"[bold]{i:>3}[/]  [{style}]{r.kind:<14}[/]"
            f"  [bold cyan]{r.title or '—'}[/]"
        )
        if r.project:
            header += f"  [dim]({r.project})[/]"
        console.print(header)
        console.print(f"     [dim]{r.path}[/]")
        if r.snippet:
            console.print(f"     {_highlight_snippet(r.snippet)}")
        console.print()


# ── tools ────────────────────────────────────────────────────────────────────

@main.command()
@click.argument("query", required=False)
@click.option("--names", is_flag=True, help="List all tool names with counts.")
@click.option("-t", "--tool", "tool_name", help="Filter by tool name.")
@click.option("-p", "--project", help="Filter by project slug (substring).")
@click.option("-s", "--source", type=click.Choice(["claude", "codex"]),
              help="Filter by source.")
@click.option("-n", "--limit", default=10, show_default=True,
              help="Maximum results.")
@click.option("--json", "as_json", is_flag=True, help="Emit JSON to stdout.")
def tools(query: str | None, names: bool, tool_name: str | None,
          project: str | None, source: str | None,
          limit: int, as_json: bool) -> None:
    """Search tool calls, or list tool names with --names."""
    db = Database()

    if names:
        counts = db.tool_name_counts()
        db.close()
        if as_json:
            click.echo(json.dumps(counts, indent=2))
            return
        if not counts:
            console.print("[dim]No tool calls indexed.  Run [bold]glom index --full[/bold].[/]")
            return
        tbl = Table(box=box.SIMPLE)
        tbl.add_column("Tool", style="bold cyan")
        tbl.add_column("Count", justify="right")
        for name, count in counts.items():
            tbl.add_row(name, f"{count:,}")
        console.print(tbl)
        return

    if not query:
        console.print("[red]Provide a QUERY, or use --names to list tools.[/]")
        raise SystemExit(1)

    try:
        results = db.search_tool_calls(
            query, tool_name=tool_name, project=project,
            source=source, limit=limit,
        )
    except Exception as exc:
        if _fts_error(exc):
            console.print(f"[red]Bad query:[/] {exc}")
            raise SystemExit(1) from None
        raise
    finally:
        db.close()

    if as_json:
        click.echo(json.dumps([
            {"tool_name": r.tool_name, "session_path": r.session_path,
             "source": r.source, "project": r.project,
             "call_id": r.call_id, "line_number": r.line_number,
             "is_error": r.is_error,
             "input_snippet": r.input_snippet,
             "output_snippet": r.output_snippet,
             "rank": r.rank}
            for r in results
        ], indent=2))
        return

    if not results:
        console.print("[dim]No results.[/]")
        return

    for i, r in enumerate(results, 1):
        header = f"[bold]{i:>3}[/]  [bold cyan]{r.tool_name}[/]"
        header += f"  [dim]line {r.line_number}[/]"
        if r.is_error:
            header += "  [bold red]ERROR[/]"
        if r.project:
            header += f"  [dim]({r.project})[/]"
        console.print(header)
        console.print(f"     [dim]{r.session_path}[/]")
        if r.input_snippet:
            console.print(f"     [green]in:[/]  {_highlight_snippet(r.input_snippet)}")
        if r.output_snippet:
            console.print(f"     [yellow]out:[/] {_highlight_snippet(r.output_snippet)}")
        console.print()


# ── stats ────────────────────────────────────────────────────────────────────

@main.command()
@click.option("--json", "as_json", is_flag=True, help="Emit JSON to stdout.")
def stats(as_json: bool) -> None:
    """Show index statistics."""
    db = Database()
    s = db.stats()
    db.close()

    if as_json:
        click.echo(json.dumps(s, indent=2, default=str))
        return

    if not s["total"]:
        console.print("[dim]Index is empty.  Run [bold]glom index[/bold] first.[/]")
        return

    tbl = Table(title="glom index", box=box.SIMPLE)
    tbl.add_column("", style="bold")
    tbl.add_column("", justify="right")

    tbl.add_row("Documents", str(s["total"]))
    for kind, count in s["by_kind"].items():
        style = _KIND_STYLE.get(kind, "white")
        tbl.add_row(f"  [{style}]{kind}[/]", str(count))
    tbl.add_row("", "")
    for src, count in s["by_source"].items():
        tbl.add_row(f"  {src}", str(count))
    tbl.add_row("", "")
    tbl.add_row("Tool calls", f"{s['tool_calls']:,}")
    tbl.add_row("Content", _human(s["total_content_bytes"]))
    tbl.add_row("Database", _human(s["db_bytes"]))
    if s["last_indexed"]:
        ts = datetime.fromtimestamp(s["last_indexed"]).strftime("%Y-%m-%d %H:%M:%S")
        tbl.add_row("Last indexed", ts)

    console.print(tbl)


# ── show ─────────────────────────────────────────────────────────────────────

@main.command()
@click.argument("path")
@click.option("--full", is_flag=True, help="Show full content (no truncation).")
@click.option("--json", "as_json", is_flag=True, help="Emit JSON to stdout.")
def show(path: str, full: bool, as_json: bool) -> None:
    """Display a specific indexed document (exact or suffix match on PATH)."""
    db = Database()
    doc = db.find_document(path)
    db.close()

    if not doc:
        console.print(f"[red]Not found:[/] {path}")
        raise SystemExit(1)

    if as_json:
        click.echo(json.dumps(dict(doc), indent=2, default=str))
        return

    content = doc["content"]
    truncated = False
    if not full and len(content) > 4000:
        content = content[:4000]
        truncated = True

    style = _KIND_STYLE.get(doc["kind"], "white")
    console.print(Panel(
        content + ("\n[dim]… truncated (use --full)[/]" if truncated else ""),
        title=f"[{style}]{doc['kind']}[/] · {doc['title'] or doc['path']}",
        subtitle=f"[dim]{_human(doc['size'])} · {doc['source']}[/]",
        expand=True,
    ))

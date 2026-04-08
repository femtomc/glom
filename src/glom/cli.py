"""CLI for glom: index & search agent context."""

from __future__ import annotations

import json
from collections.abc import Callable
from datetime import datetime

import rich_click as click
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

from glom._compact import Column, apply_16kb_cap, compact_table
from glom.db import Database
from glom.indexer import index_all

console = Console(stderr=True)


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


def _truncate(s: str, max_len: int) -> str:
    return s if len(s) <= max_len else s[:max_len - 1] + "\u2026"


def _json_envelope(
    rows: list[dict],
    total: int,
    limit: int,
) -> dict:
    return {
        "rows": rows,
        "total": total,
        "displayed": len(rows),
        "truncated": len(rows) < total,
        "limit": limit,
    }


def _make_index_progress_callbacks(
    progress: Progress,
) -> tuple[int, int, Callable[[int, int], None], Callable[[str], None]]:
    index_task = progress.add_task("Scanning", total=None)
    rebuild_task = progress.add_task("Rebuilding FTS", total=None, visible=False)

    def on_progress(done: int, total: int) -> None:
        progress.update(index_task, completed=done, total=total)

    def on_phase(phase: str) -> None:
        labels = {
            "scanning": "Scanning",
            "indexing": "Indexing",
            "rebuilding": "Rebuilding FTS",
        }
        label = labels.get(phase, phase)

        if phase == "rebuilding":
            progress.update(index_task, visible=False)
            progress.update(rebuild_task, description=label, visible=True)
            return

        progress.update(rebuild_task, visible=False)
        progress.update(index_task, description=label, visible=True)

    return index_task, rebuild_task, on_progress, on_phase


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
        _, _, on_progress, on_phase = _make_index_progress_callbacks(progress)

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

_SEARCH_COLUMNS: list[Column] = [
    ("rank", "rank", {"align": "right", "max_width": 8}),
    ("kind", "kind", {"max_width": 14}),
    ("name", "name", {"max_width": 40}),
    ("location", "location", {"max_width": 40}),
    ("snippet", "snippet", {"max_width": 40}),
]


def _search_row(r, i: int) -> dict:
    return {
        "rank": i,
        "kind": r.kind,
        "name": r.title or r.path.rsplit("/", 1)[-1],
        "location": r.path,
        "snippet": r.snippet.replace("\u00bb", "").replace("\u00ab", ""),
    }


@main.command()
@click.argument("query")
@click.option("-k", "--kind", help="Filter by document kind.")
@click.option("-p", "--project", help="Filter by project slug (substring).")
@click.option("-s", "--source", type=click.Choice(["claude", "codex"]),
              help="Filter by source.")
@click.option("-n", "--limit", default=10, show_default=True,
              help="Maximum results.  0 = unlimited.")
@click.option("--full", is_flag=True, help="Show multi-line detail per result.")
@click.option("--json", "as_json", is_flag=True, help="Emit JSON to stdout.")
@click.option("--json-legacy", "json_legacy", is_flag=True, hidden=True)
def search(query: str, kind: str | None, project: str | None,
           source: str | None, limit: int, full: bool,
           as_json: bool, json_legacy: bool) -> None:
    """Full-text search over the index (FTS5, BM25-ranked)."""
    db = Database()
    try:
        results, total = db.search(
            query, kind=kind, project=project, source=source, limit=limit,
        )
    except Exception as exc:
        if _fts_error(exc):
            click.echo(f"Bad query: {exc}", err=True)
            raise SystemExit(1) from None
        raise
    finally:
        db.close()

    if json_legacy:
        click.echo(json.dumps([
            {"path": r.path, "kind": r.kind, "source": r.source,
             "project": r.project, "title": r.title,
             "snippet": r.snippet, "rank": r.rank, "size": r.size}
            for r in results
        ], indent=2))
        return

    if as_json:
        rows = [_search_row(r, i) for i, r in enumerate(results, 1)]
        click.echo(json.dumps(_json_envelope(rows, total, limit), indent=2))
        return

    if not results:
        click.echo("No results.")
        return

    if full:
        lines: list[str] = []
        for i, r in enumerate(results, 1):
            lines.append(f"{i:>3}  {r.kind:<14}  {r.title or '-'}")
            lines.append(f"     {r.path}")
            if r.snippet:
                plain = r.snippet.replace("\u00bb", "").replace("\u00ab", "")
                lines.append(f"     {plain}")
            lines.append("")
        output = "\n".join(lines) + "\n"
        click.echo(apply_16kb_cap(output), nl=False)
        return

    row_dicts = [_search_row(r, i) for i, r in enumerate(results, 1)]
    click.echo(compact_table(row_dicts, _SEARCH_COLUMNS, total=total), nl=False)


# ── tools ────────────────────────────────────────────────────────────────────

_TOOLS_NAMES_COLUMNS: list[Column] = [
    ("tool", "tool", {"max_width": 40}),
    ("count", "count", {"align": "right", "max_width": 10}),
]

_TOOLS_QUERY_COLUMNS: list[Column] = [
    ("rank", "rank", {"align": "right", "max_width": 8}),
    ("kind", "kind", {"max_width": 14}),
    ("name", "name", {"max_width": 40}),
    ("location", "location", {"max_width": 40}),
    ("snippet", "snippet", {"max_width": 40}),
]


def _tool_call_row(r, i: int) -> dict:
    snippet = r.input_snippet or r.output_snippet
    snippet = snippet.replace("\u00bb", "").replace("\u00ab", "")
    return {
        "rank": i,
        "kind": "tool",
        "name": r.tool_name,
        "location": r.session_path,
        "snippet": _truncate(snippet, 40),
    }


@main.command()
@click.argument("query", required=False)
@click.option("--names", is_flag=True, help="List all tool names with counts.")
@click.option("-t", "--tool", "tool_name", help="Filter by tool name.")
@click.option("-p", "--project", help="Filter by project slug (substring).")
@click.option("-s", "--source", type=click.Choice(["claude", "codex"]),
              help="Filter by source.")
@click.option("-n", "--limit", default=20, show_default=True,
              help="Maximum results.  0 = unlimited.")
@click.option("--full", is_flag=True,
              help="Show all rows (--names) or multi-line detail (query).")
@click.option("--json", "as_json", is_flag=True, help="Emit JSON to stdout.")
@click.option("--json-legacy", "json_legacy", is_flag=True, hidden=True)
def tools(query: str | None, names: bool, tool_name: str | None,
          project: str | None, source: str | None,
          limit: int, full: bool, as_json: bool,
          json_legacy: bool) -> None:
    """Search tool calls, or list tool names with --names."""
    db = Database()

    if names:
        counts = db.tool_name_counts()
        db.close()
        items = list(counts.items())
        total_items = len(items)
        cap = 0 if full else limit
        shown = items if cap <= 0 else items[:cap]

        if json_legacy:
            click.echo(json.dumps(counts, indent=2))
            return

        if as_json:
            rows = [{"tool": name, "count": count} for name, count in shown]
            click.echo(json.dumps(
                _json_envelope(rows, total_items, cap), indent=2,
            ))
            return

        if not counts:
            click.echo("No tool calls indexed.  Run glom index --full.")
            return

        row_dicts = [{"tool": name, "count": count} for name, count in shown]
        click.echo(compact_table(
            row_dicts, _TOOLS_NAMES_COLUMNS, total=total_items,
        ), nl=False)
        return

    if not query:
        click.echo("Provide a QUERY, or use --names to list tools.", err=True)
        raise SystemExit(1)

    # search uses --limit 10 default per spec, but tools query keeps 20
    try:
        results, total = db.search_tool_calls(
            query, tool_name=tool_name, project=project,
            source=source, limit=limit,
        )
    except Exception as exc:
        if _fts_error(exc):
            click.echo(f"Bad query: {exc}", err=True)
            raise SystemExit(1) from None
        raise
    finally:
        db.close()

    if json_legacy:
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

    if as_json:
        rows = [_tool_call_row(r, i) for i, r in enumerate(results, 1)]
        click.echo(json.dumps(_json_envelope(rows, total, limit), indent=2))
        return

    if not results:
        click.echo("No results.")
        return

    if full:
        lines: list[str] = []
        for i, r in enumerate(results, 1):
            err = " ERROR" if r.is_error else ""
            lines.append(f"{i:>3}  {r.tool_name}  line {r.line_number}{err}")
            lines.append(f"     {r.session_path}")
            if r.input_snippet:
                plain = r.input_snippet.replace("\u00bb", "").replace("\u00ab", "")
                lines.append(f"     in:  {plain}")
            if r.output_snippet:
                plain = r.output_snippet.replace("\u00bb", "").replace("\u00ab", "")
                lines.append(f"     out: {plain}")
            lines.append("")
        output = "\n".join(lines) + "\n"
        click.echo(apply_16kb_cap(output), nl=False)
        return

    row_dicts = [_tool_call_row(r, i) for i, r in enumerate(results, 1)]
    click.echo(compact_table(
        row_dicts, _TOOLS_QUERY_COLUMNS, total=total,
    ), nl=False)


# ── stats ────────────────────────────────────────────────────────────────────

_STATS_COLUMNS: list[Column] = [
    ("metric", "metric", {"max_width": 20}),
    ("value", "value", {"align": "right", "max_width": 20}),
]


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
        click.echo("Index is empty.  Run glom index first.")
        return

    rows: list[dict] = []
    rows.append({"metric": "Documents", "value": str(s["total"])})
    for kind_name, count in s["by_kind"].items():
        rows.append({"metric": f"  {kind_name}", "value": str(count)})
    for src, count in s["by_source"].items():
        rows.append({"metric": f"  {src}", "value": str(count)})
    rows.append({"metric": "Tool calls", "value": f"{s['tool_calls']:,}"})
    rows.append({"metric": "Content", "value": _human(s["total_content_bytes"])})
    rows.append({"metric": "Database", "value": _human(s["db_bytes"])})
    if s["last_indexed"]:
        ts = datetime.fromtimestamp(s["last_indexed"]).strftime("%Y-%m-%d %H:%M:%S")
        rows.append({"metric": "Last indexed", "value": ts})

    click.echo(compact_table(rows, _STATS_COLUMNS), nl=False)


# ── show ─────────────────────────────────────────────────────────────────────

@main.command()
@click.argument("path")
@click.option("--full", is_flag=True,
              help="Show full content without truncation (default truncates to 4000 chars).")
@click.option("--json", "as_json", is_flag=True, help="Emit JSON to stdout.")
def show(path: str, full: bool, as_json: bool) -> None:
    """Display a specific indexed document (exact or suffix match on PATH)."""
    db = Database()
    doc = db.find_document(path)
    db.close()

    if not doc:
        click.echo(f"Not found: {path}", err=True)
        raise SystemExit(1)

    if as_json:
        d = dict(doc)
        if not full and isinstance(d.get("content"), str) and len(d["content"]) > 4000:
            d["content"] = d["content"][:4000]
            d["_truncated"] = True
        click.echo(json.dumps(d, indent=2, default=str))
        return

    content = doc["content"]
    truncated = False
    if not full and len(content) > 4000:
        content = content[:4000]
        truncated = True

    header = f"{doc['kind']} | {doc['title'] or doc['path']}"
    footer = f"{_human(doc['size'])} | {doc['source']}"
    lines = [header, "-" * len(header), content]
    if truncated:
        lines.append("... truncated (use --full)")
    lines.append(footer)
    output = "\n".join(lines) + "\n"
    click.echo(apply_16kb_cap(output) if full else output, nl=False)

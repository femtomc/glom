"""File discovery, parsing, and indexing for ~/.claude and ~/.codex."""

from __future__ import annotations

import json
import re
from collections.abc import Callable
from dataclasses import dataclass, field
from pathlib import Path

import yaml

from glom.db import Database, Document

_UUID_RE = re.compile(
    r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$", re.I
)
_HEX_RE = re.compile(r"^[0-9a-f]{16,}$", re.I)

# If more than this many files need writing, drop FTS triggers and rebuild
# at the end instead of maintaining the index per-row.
_BULK_THRESHOLD = 100


# ---------------------------------------------------------------------------
# Stats
# ---------------------------------------------------------------------------

@dataclass
class IndexStats:
    new: int = 0
    updated: int = 0
    unchanged: int = 0
    deleted: int = 0
    errors: int = 0
    tool_calls_extracted: int = 0
    error_paths: list[str] = field(default_factory=list)

    @property
    def total_processed(self) -> int:
        return self.new + self.updated + self.unchanged


# ---------------------------------------------------------------------------
# Discovery
# ---------------------------------------------------------------------------

@dataclass(slots=True)
class FileEntry:
    path: Path
    source: str
    kind: str
    project: str | None


def _add(entries: list[FileEntry], path: Path, source: str, kind: str,
         project: str | None = None) -> None:
    if path.is_file():
        entries.append(FileEntry(path, source, kind, project))


def discover(
    claude_root: Path | None = None,
    codex_root: Path | None = None,
) -> list[FileEntry]:
    """Walk ~/.claude and ~/.codex and return every indexable file."""
    claude = claude_root or Path.home() / ".claude"
    codex = codex_root or Path.home() / ".codex"
    entries: list[FileEntry] = []

    # ── Claude ────────────────────────────────────────────────────────────
    if claude.is_dir():
        _add(entries, claude / "CLAUDE.md", "claude", "instructions")
        _add(entries, claude / "settings.json", "claude", "settings")
        _add(entries, claude / "settings.local.json", "claude", "settings")
        _add(entries, claude / "history.jsonl", "claude", "history")

        # Skills (often symlinks into ~/.config/skills)
        skills = claude / "skills"
        if skills.is_dir():
            for d in sorted(skills.iterdir()):
                _add(entries, d / "SKILL.md", "claude", "skill")

        # Plans
        plans = claude / "plans"
        if plans.is_dir():
            for p in sorted(plans.glob("*.md")):
                entries.append(FileEntry(p, "claude", "plan", None))

        # Tasks
        tasks = claude / "tasks"
        if tasks.is_dir():
            for td in sorted(tasks.iterdir()):
                if not td.is_dir():
                    continue
                for tf in sorted(td.glob("*.json")):
                    entries.append(FileEntry(tf, "claude", "task", None))

        # Projects — memories, project instructions, session transcripts
        projects = claude / "projects"
        if projects.is_dir():
            for pd in sorted(projects.iterdir()):
                if not pd.is_dir():
                    continue
                proj = pd.name
                _add(entries, pd / "CLAUDE.md", "claude", "instructions", proj)

                mem = pd / "memory"
                if mem.is_dir():
                    for mf in sorted(mem.glob("*.md")):
                        entries.append(FileEntry(mf, "claude", "memory", proj))

                for sf in sorted(pd.glob("*.jsonl")):
                    entries.append(FileEntry(sf, "claude", "session", proj))

    # ── Codex ─────────────────────────────────────────────────────────────
    if codex.is_dir():
        _add(entries, codex / "AGENTS.md", "codex", "instructions")
        _add(entries, codex / "config.toml", "codex", "settings")
        _add(entries, codex / "history.jsonl", "codex", "history")

        skills = codex / "skills"
        if skills.is_dir():
            for sf in sorted(skills.rglob("*.md")):
                entries.append(FileEntry(sf, "codex", "skill", None))

        sessions = codex / "sessions"
        if sessions.is_dir():
            for sf in sorted(sessions.rglob("*.jsonl")):
                entries.append(FileEntry(sf, "codex", "session", None))

    return entries


# ---------------------------------------------------------------------------
# Parsing — text extraction
# ---------------------------------------------------------------------------

def _split_frontmatter(text: str) -> tuple[dict | None, str]:
    m = re.match(r"^---\s*\n(.*?)\n---\s*\n", text, re.DOTALL)
    if not m:
        return None, text
    try:
        meta = yaml.safe_load(m.group(1))
        return (meta if isinstance(meta, dict) else None), text[m.end():]
    except yaml.YAMLError:
        return None, text


def _parse_markdown(text: str, fallback_title: str) -> tuple[str, str]:
    meta, body = _split_frontmatter(text)
    title = fallback_title
    if meta:
        title = meta.get("name") or meta.get("title") or fallback_title
    return title, body.strip() or text


def _parse_json_task(text: str, filename: str) -> tuple[str, str]:
    try:
        data = json.loads(text)
    except json.JSONDecodeError:
        return filename, text
    title = data.get("subject") or data.get("title") or filename
    parts = [title]
    if desc := data.get("description"):
        parts.append(desc)
    if status := data.get("status"):
        parts.append(f"[{status}]")
    return title, "\n".join(parts)


def _collect_texts(obj: object, acc: list[str], depth: int = 0) -> None:
    """Recursively extract meaningful strings from a JSON value."""
    if depth > 15:
        return
    if isinstance(obj, str):
        text = obj.strip()
        if len(text) > 3 and not _UUID_RE.match(text) and not _HEX_RE.match(text):
            acc.append(text)
    elif isinstance(obj, dict):
        for v in obj.values():
            _collect_texts(v, acc, depth + 1)
    elif isinstance(obj, list):
        for item in obj:
            _collect_texts(item, acc, depth + 1)


def _collect_visible_text(obj: object, acc: list[str]) -> None:
    """Extract human-facing transcript text from structured message blocks."""
    if isinstance(obj, str):
        _collect_texts(obj, acc)
        return
    if isinstance(obj, list):
        for item in obj:
            _collect_visible_text(item, acc)
        return
    if not isinstance(obj, dict):
        return

    block_type = obj.get("type")
    if block_type in {
        "thinking",
        "reasoning",
        "tool_use",
        "tool_result",
        "function_call",
        "function_call_output",
        "custom_tool_call",
        "custom_tool_call_output",
        "image",
        "input_image",
    }:
        return

    if block_type in {"text", "input_text", "output_text"}:
        _collect_visible_text(obj.get("text"), acc)
        return

    for key in ("text", "message", "content"):
        if key in obj:
            _collect_visible_text(obj[key], acc)


def _collect_claude_session_text(obj: dict, acc: list[str]) -> None:
    if obj.get("type") not in {"user", "assistant"}:
        return
    message = obj.get("message")
    if not isinstance(message, dict):
        return
    _collect_visible_text(message.get("content"), acc)


def _collect_codex_session_text(obj: dict, acc: list[str]) -> None:
    kind = obj.get("type")

    if kind == "event_msg":
        payload = obj.get("payload")
        if not isinstance(payload, dict):
            return
        payload_type = payload.get("type")
        if payload_type in {"user_message", "agent_message"}:
            _collect_visible_text(payload.get("message"), acc)
        return

    if kind == "response_item":
        payload = obj.get("payload")
        if not isinstance(payload, dict):
            return
        if payload.get("type") == "message" and payload.get("role") in {"user", "assistant"}:
            _collect_visible_text(payload.get("content"), acc)


# ---------------------------------------------------------------------------
# Parsing — tool-call extraction
# ---------------------------------------------------------------------------

# Each extracted call is a tuple:
#   (call_id, tool_name, input_json, output_text, is_error, line_number, timestamp)
_ToolTuple = tuple[str, str, str, str, int, int, str | None]


def _extract_claude_calls(
    obj: dict, pending: dict[str, list], line_num: int
) -> list[_ToolTuple]:
    """Extract tool_use and tool_result from a Claude JSONL line.

    Returns completed calls (where we have both invocation and result).
    Invocations without a result yet are stashed in *pending*.
    """
    msg = obj.get("message")
    if not isinstance(msg, dict):
        return []
    content = msg.get("content")
    if not isinstance(content, list):
        return []

    ts = obj.get("timestamp")
    completed: list[_ToolTuple] = []

    for block in content:
        if not isinstance(block, dict):
            continue

        if block.get("type") == "tool_use":
            cid = block.get("id", "")
            pending[cid] = [
                cid,
                block.get("name", "unknown"),
                json.dumps(block.get("input", {}), ensure_ascii=False),
                "",   # output — filled when result arrives
                0,    # is_error
                line_num,
                ts,
            ]

        elif block.get("type") == "tool_result":
            cid = block.get("tool_use_id", "")
            if cid not in pending:
                continue
            result = block.get("content", "")
            if isinstance(result, list):
                parts = []
                for sub in result:
                    if isinstance(sub, dict):
                        parts.append(sub.get("text", json.dumps(sub)))
                    else:
                        parts.append(str(sub))
                result = "\n".join(parts)
            entry = pending.pop(cid)
            entry[3] = str(result)
            entry[4] = int(bool(block.get("is_error", False)))
            completed.append(tuple(entry))  # type: ignore[arg-type]

    return completed


def _extract_codex_calls(
    obj: dict, pending: dict[str, list], line_num: int
) -> list[_ToolTuple]:
    """Extract function_call / function_call_output from a Codex JSONL line."""
    ts = obj.get("timestamp")
    payload = obj.get("payload")
    if not isinstance(payload, dict):
        return []

    completed: list[_ToolTuple] = []
    ptype = payload.get("type")

    if ptype == "function_call":
        cid = payload.get("call_id", "")
        args = payload.get("arguments", "{}")
        if not isinstance(args, str):
            args = json.dumps(args, ensure_ascii=False)
        pending[cid] = [
            cid,
            payload.get("name", "unknown"),
            args,
            "",
            0,
            line_num,
            ts,
        ]

    elif ptype == "function_call_output":
        cid = payload.get("call_id", "")
        if cid in pending:
            entry = pending.pop(cid)
            entry[3] = str(payload.get("output", ""))
            completed.append(tuple(entry))  # type: ignore[arg-type]

    return completed


def _parse_session(
    path: Path, source: str
) -> tuple[str, str, list[_ToolTuple]]:
    """Single-pass JSONL reader: extracts both document text and tool calls."""
    text_parts: list[str] = []
    pending: dict[str, list] = {}
    finished: list[_ToolTuple] = []
    extractor = _extract_claude_calls if source == "claude" else _extract_codex_calls
    text_extractor = (
        _collect_claude_session_text if source == "claude" else _collect_codex_session_text
    )

    with path.open(errors="replace") as f:
        for line_num, raw in enumerate(f, 1):
            raw = raw.strip()
            if not raw:
                continue
            try:
                obj = json.loads(raw)
            except (json.JSONDecodeError, RecursionError):
                continue
            text_extractor(obj, text_parts)
            finished.extend(extractor(obj, pending, line_num))

    # Flush invocations whose results never appeared (truncated sessions, etc.)
    for entry in pending.values():
        finished.append(tuple(entry))  # type: ignore[arg-type]

    return path.stem, "\n".join(text_parts), finished


# ---------------------------------------------------------------------------
# Top-level parse dispatch
# ---------------------------------------------------------------------------

def parse_file(entry: FileEntry) -> tuple[Document, list[_ToolTuple]]:
    """Turn a discovered file into a Document (and tool calls for sessions)."""
    path = entry.path
    stat = path.stat()
    metadata: str | None = None
    tool_calls: list[_ToolTuple] = []

    if entry.kind in ("session", "history"):
        title, content, tool_calls = _parse_session(path, entry.source)
    elif entry.kind == "task":
        text = path.read_text(errors="replace")
        title, content = _parse_json_task(text, path.name)
    elif entry.kind == "settings":
        text = path.read_text(errors="replace")
        title, content = path.name, text
    else:
        # markdown: memory, plan, instructions, skill
        text = path.read_text(errors="replace")
        title, content = _parse_markdown(text, path.stem)
        if entry.kind == "memory":
            fm, _ = _split_frontmatter(text)
            if fm:
                metadata = json.dumps(fm)

    doc = Document(
        source=entry.source,
        path=str(path),
        kind=entry.kind,
        project=entry.project,
        title=title,
        content=content,
        metadata=metadata,
        mtime=stat.st_mtime,
        size=stat.st_size,
    )
    return doc, tool_calls


# ---------------------------------------------------------------------------
# Indexing
# ---------------------------------------------------------------------------

@dataclass(slots=True)
class _WorkItem:
    entry: FileEntry
    is_new: bool


def index_all(
    db: Database,
    *,
    claude_root: Path | None = None,
    codex_root: Path | None = None,
    full: bool = False,
    on_progress: Callable[[int, int], None] | None = None,
    on_phase: Callable[[str], None] | None = None,
) -> IndexStats:
    """Index every discoverable file.  Incremental unless *full* is set.

    Automatically uses bulk mode (deferred FTS rebuild) when the number of
    files that need writing exceeds *_BULK_THRESHOLD*.
    """
    stats = IndexStats()
    entries = discover(claude_root, codex_root)
    seen: set[str] = set()
    existing_mtimes = db.get_all_mtimes()

    # ── phase 1: classify ────────────────────────────────────────────────
    if on_phase:
        on_phase("scanning")

    work: list[_WorkItem] = []
    for entry in entries:
        str_path = str(entry.path)
        seen.add(str_path)

        try:
            mtime = entry.path.stat().st_mtime
        except OSError:
            stats.errors += 1
            stats.error_paths.append(str_path)
            continue

        existing = existing_mtimes.get(str_path)
        if not full and existing is not None and existing >= mtime:
            stats.unchanged += 1
            continue

        is_new = existing is None
        work.append(_WorkItem(entry, is_new))

    # ── phase 2: parse + insert ──────────────────────────────────────────
    use_bulk = len(work) > _BULK_THRESHOLD
    if use_bulk:
        db.begin_bulk()

    if on_phase:
        on_phase("indexing")

    try:
        for i, item in enumerate(work):
            try:
                doc, tool_calls = parse_file(item.entry)
                db.upsert(doc)

                if tool_calls:
                    db.replace_tool_calls(
                        str(item.entry.path), item.entry.source,
                        item.entry.project, tool_calls,
                    )
                    stats.tool_calls_extracted += len(tool_calls)

                if item.is_new:
                    stats.new += 1
                else:
                    stats.updated += 1
            except Exception as exc:
                stats.errors += 1
                stats.error_paths.append(f"{item.entry.path}: {exc}")

            _tick(on_progress, i + 1, len(work))
    finally:
        if use_bulk:
            if on_phase:
                on_phase("rebuilding")
            db.end_bulk()

    # ── phase 3: prune stale entries ─────────────────────────────────────
    for stale in existing_mtimes.keys() - seen:
        db.delete_path(stale)
        stats.deleted += 1

    db.commit()
    return stats


def _tick(
    cb: Callable[[int, int], None] | None, done: int, total: int
) -> None:
    if cb is not None:
        cb(done, total)

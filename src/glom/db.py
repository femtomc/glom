"""SQLite + FTS5 index for agent context documents and tool calls."""

from __future__ import annotations

import json
import os
import sqlite3
import time
from dataclasses import dataclass
from pathlib import Path


def _default_db_path() -> Path:
    env = os.environ.get("GLOM_DB")
    if env:
        return Path(env).expanduser()
    return Path.home() / ".local" / "share" / "glom" / "index.db"


# ---------------------------------------------------------------------------
# Data types
# ---------------------------------------------------------------------------

@dataclass(slots=True)
class Document:
    source: str
    path: str
    kind: str
    project: str | None
    title: str | None
    content: str
    metadata: str | None  # JSON string
    mtime: float
    size: int


@dataclass(slots=True)
class SearchResult:
    path: str
    kind: str
    source: str
    project: str | None
    title: str | None
    snippet: str
    rank: float
    size: int


@dataclass(slots=True)
class ToolCallRow:
    session_path: str
    source: str
    project: str | None
    tool_name: str
    call_id: str
    input_snippet: str
    output_snippet: str
    is_error: bool
    line_number: int
    rank: float


# ---------------------------------------------------------------------------
# Schema — split so triggers can be toggled for bulk mode
# ---------------------------------------------------------------------------

_TABLES = """\
CREATE TABLE IF NOT EXISTS documents (
    id INTEGER PRIMARY KEY,
    source TEXT NOT NULL,
    path TEXT NOT NULL UNIQUE,
    kind TEXT NOT NULL,
    project TEXT,
    title TEXT,
    content TEXT NOT NULL,
    metadata TEXT,
    mtime REAL NOT NULL,
    size INTEGER NOT NULL,
    indexed_at REAL NOT NULL
);

CREATE TABLE IF NOT EXISTS tool_calls (
    id INTEGER PRIMARY KEY,
    session_path TEXT NOT NULL,
    source TEXT NOT NULL,
    project TEXT,
    call_id TEXT,
    tool_name TEXT NOT NULL,
    input TEXT NOT NULL DEFAULT '',
    output TEXT NOT NULL DEFAULT '',
    is_error INTEGER NOT NULL DEFAULT 0,
    line_number INTEGER,
    timestamp TEXT
);

CREATE INDEX IF NOT EXISTS idx_tc_session ON tool_calls(session_path);
CREATE INDEX IF NOT EXISTS idx_tc_name    ON tool_calls(tool_name);
"""

_FTS = """\
CREATE VIRTUAL TABLE IF NOT EXISTS documents_fts USING fts5(
    title, content, path, kind, project,
    content=documents, content_rowid=id,
    tokenize='porter unicode61'
);

CREATE VIRTUAL TABLE IF NOT EXISTS tool_calls_fts USING fts5(
    tool_name, input, output,
    content=tool_calls, content_rowid=id,
    tokenize='porter unicode61'
);
"""

_TRIGGERS = """\
CREATE TRIGGER IF NOT EXISTS documents_ai AFTER INSERT ON documents BEGIN
    INSERT INTO documents_fts(rowid, title, content, path, kind, project)
    VALUES (new.id, new.title, new.content, new.path, new.kind, new.project);
END;

CREATE TRIGGER IF NOT EXISTS documents_ad AFTER DELETE ON documents BEGIN
    INSERT INTO documents_fts(documents_fts, rowid, title, content, path, kind, project)
    VALUES ('delete', old.id, old.title, old.content, old.path, old.kind, old.project);
END;

CREATE TRIGGER IF NOT EXISTS documents_au AFTER UPDATE ON documents BEGIN
    INSERT INTO documents_fts(documents_fts, rowid, title, content, path, kind, project)
    VALUES ('delete', old.id, old.title, old.content, old.path, old.kind, old.project);
    INSERT INTO documents_fts(rowid, title, content, path, kind, project)
    VALUES (new.id, new.title, new.content, new.path, new.kind, new.project);
END;

CREATE TRIGGER IF NOT EXISTS tc_ai AFTER INSERT ON tool_calls BEGIN
    INSERT INTO tool_calls_fts(rowid, tool_name, input, output)
    VALUES (new.id, new.tool_name, new.input, new.output);
END;

CREATE TRIGGER IF NOT EXISTS tc_ad AFTER DELETE ON tool_calls BEGIN
    INSERT INTO tool_calls_fts(tool_calls_fts, rowid, tool_name, input, output)
    VALUES ('delete', old.id, old.tool_name, old.input, old.output);
END;

CREATE TRIGGER IF NOT EXISTS tc_au AFTER UPDATE ON tool_calls BEGIN
    INSERT INTO tool_calls_fts(tool_calls_fts, rowid, tool_name, input, output)
    VALUES ('delete', old.id, old.tool_name, old.input, old.output);
    INSERT INTO tool_calls_fts(rowid, tool_name, input, output)
    VALUES (new.id, new.tool_name, new.input, new.output);
END;
"""

_DROP_TRIGGERS = """\
DROP TRIGGER IF EXISTS documents_ai;
DROP TRIGGER IF EXISTS documents_ad;
DROP TRIGGER IF EXISTS documents_au;
DROP TRIGGER IF EXISTS tc_ai;
DROP TRIGGER IF EXISTS tc_ad;
DROP TRIGGER IF EXISTS tc_au;
"""


class Database:
    """Thin wrapper over a SQLite connection with FTS5 search."""

    def __init__(self, path: Path | None = None):
        self.path = path or _default_db_path()
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._conn = sqlite3.connect(str(self.path))
        self._conn.row_factory = sqlite3.Row
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.executescript(_TABLES + _FTS + _TRIGGERS)

    def close(self) -> None:
        self._conn.close()

    def commit(self) -> None:
        self._conn.commit()

    # -- bulk mode ------------------------------------------------------------

    def begin_bulk(self) -> None:
        """Drop FTS triggers and tune pragmas for high-throughput inserts."""
        self._conn.executescript(_DROP_TRIGGERS)
        self._conn.execute("PRAGMA cache_size = -65536")    # 64 MB
        self._conn.execute("PRAGMA synchronous = NORMAL")

    def end_bulk(self) -> None:
        """Rebuild both FTS indices from content tables, restore triggers."""
        # Widen cache + enable mmap for the FTS rebuild scan
        self._conn.execute("PRAGMA cache_size = -262144")   # 256 MB
        self._conn.execute("PRAGMA mmap_size = 2147483648")  # 2 GB
        self._conn.execute(
            "INSERT INTO documents_fts(documents_fts) VALUES('rebuild')"
        )
        self._conn.execute(
            "INSERT INTO tool_calls_fts(tool_calls_fts) VALUES('rebuild')"
        )
        # Restore defaults
        self._conn.executescript(_TRIGGERS)
        self._conn.execute("PRAGMA cache_size = -2000")
        self._conn.execute("PRAGMA mmap_size = 0")
        self._conn.commit()

    # -- incremental helpers --------------------------------------------------

    def get_mtime(self, path: str) -> float | None:
        row = self._conn.execute(
            "SELECT mtime FROM documents WHERE path = ?", (path,)
        ).fetchone()
        return row["mtime"] if row else None

    def get_all_paths(self) -> set[str]:
        return {
            r["path"]
            for r in self._conn.execute("SELECT path FROM documents").fetchall()
        }

    # -- document mutations ---------------------------------------------------

    def upsert(self, doc: Document) -> None:
        self._conn.execute(
            """\
            INSERT INTO documents
                (source, path, kind, project, title, content, metadata, mtime, size, indexed_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(path) DO UPDATE SET
                source=excluded.source, kind=excluded.kind,
                project=excluded.project, title=excluded.title,
                content=excluded.content, metadata=excluded.metadata,
                mtime=excluded.mtime, size=excluded.size,
                indexed_at=excluded.indexed_at""",
            (
                doc.source, doc.path, doc.kind, doc.project, doc.title,
                doc.content, doc.metadata, doc.mtime, doc.size, time.time(),
            ),
        )

    def delete_path(self, path: str) -> None:
        self._conn.execute("DELETE FROM tool_calls WHERE session_path = ?", (path,))
        self._conn.execute("DELETE FROM documents WHERE path = ?", (path,))

    def rebuild_fts(self) -> None:
        self._conn.execute(
            "INSERT INTO documents_fts(documents_fts) VALUES('rebuild')"
        )
        self._conn.execute(
            "INSERT INTO tool_calls_fts(tool_calls_fts) VALUES('rebuild')"
        )
        self._conn.commit()

    # -- tool-call mutations --------------------------------------------------

    def replace_tool_calls(
        self,
        session_path: str,
        source: str,
        project: str | None,
        calls: list[tuple],
    ) -> int:
        """Delete existing tool calls for *session_path*, insert *calls*.

        Each element of *calls* is a tuple:
            (call_id, tool_name, input, output, is_error, line_number, timestamp)
        """
        self._conn.execute(
            "DELETE FROM tool_calls WHERE session_path = ?", (session_path,)
        )
        self._conn.executemany(
            """\
            INSERT INTO tool_calls
                (session_path, source, project, call_id, tool_name,
                 input, output, is_error, line_number, timestamp)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            [
                (session_path, source, project, cid, name, inp, out, err, ln, ts)
                for cid, name, inp, out, err, ln, ts in calls
            ],
        )
        return len(calls)

    # -- document queries -----------------------------------------------------

    def search(
        self,
        query: str,
        *,
        kind: str | None = None,
        project: str | None = None,
        source: str | None = None,
        limit: int = 10,
    ) -> list[SearchResult]:
        sql = """\
            SELECT d.path, d.kind, d.source, d.project, d.title, d.size,
                   snippet(documents_fts, 1, '»', '«', ' … ', 48) AS snippet,
                   rank
            FROM documents_fts
            JOIN documents d ON d.id = documents_fts.rowid
            WHERE documents_fts MATCH ?"""
        params: list[str | int] = [query]

        if kind:
            sql += " AND d.kind = ?"
            params.append(kind)
        if project:
            sql += " AND d.project LIKE ?"
            params.append(f"%{project}%")
        if source:
            sql += " AND d.source = ?"
            params.append(source)

        sql += " ORDER BY rank LIMIT ?"
        params.append(limit)

        return [
            SearchResult(
                path=r["path"], kind=r["kind"], source=r["source"],
                project=r["project"], title=r["title"],
                snippet=r["snippet"] or "", rank=r["rank"], size=r["size"],
            )
            for r in self._conn.execute(sql, params).fetchall()
        ]

    def get_document(self, path: str) -> sqlite3.Row | None:
        return self._conn.execute(
            "SELECT * FROM documents WHERE path = ?", (path,)
        ).fetchone()

    def find_document(self, fragment: str) -> sqlite3.Row | None:
        """Lookup by exact path first, then fall back to suffix match."""
        row = self.get_document(fragment)
        if row:
            return row
        return self._conn.execute(
            "SELECT * FROM documents WHERE path LIKE ? LIMIT 1",
            (f"%{fragment}",),
        ).fetchone()

    # -- tool-call queries ----------------------------------------------------

    def search_tool_calls(
        self,
        query: str,
        *,
        tool_name: str | None = None,
        project: str | None = None,
        source: str | None = None,
        limit: int = 10,
    ) -> list[ToolCallRow]:
        sql = """\
            SELECT tc.session_path, tc.source, tc.project, tc.tool_name,
                   tc.call_id, tc.is_error, tc.line_number,
                   snippet(tool_calls_fts, 1, '»', '«', ' … ', 48) AS input_snippet,
                   snippet(tool_calls_fts, 2, '»', '«', ' … ', 48) AS output_snippet,
                   rank
            FROM tool_calls_fts
            JOIN tool_calls tc ON tc.id = tool_calls_fts.rowid
            WHERE tool_calls_fts MATCH ?"""
        params: list[str | int] = [query]

        if tool_name:
            sql += " AND tc.tool_name = ?"
            params.append(tool_name)
        if project:
            sql += " AND tc.project LIKE ?"
            params.append(f"%{project}%")
        if source:
            sql += " AND tc.source = ?"
            params.append(source)

        sql += " ORDER BY rank LIMIT ?"
        params.append(limit)

        return [
            ToolCallRow(
                session_path=r["session_path"], source=r["source"],
                project=r["project"], tool_name=r["tool_name"],
                call_id=r["call_id"],
                input_snippet=r["input_snippet"] or "",
                output_snippet=r["output_snippet"] or "",
                is_error=bool(r["is_error"]),
                line_number=r["line_number"], rank=r["rank"],
            )
            for r in self._conn.execute(sql, params).fetchall()
        ]

    def tool_name_counts(self) -> dict[str, int]:
        return {
            r["tool_name"]: r["n"]
            for r in self._conn.execute(
                "SELECT tool_name, COUNT(*) AS n FROM tool_calls "
                "GROUP BY tool_name ORDER BY n DESC"
            ).fetchall()
        }

    # -- stats ----------------------------------------------------------------

    def stats(self) -> dict:
        total = self._conn.execute(
            "SELECT COUNT(*) AS n FROM documents"
        ).fetchone()["n"]
        by_kind = {
            r["kind"]: r["n"]
            for r in self._conn.execute(
                "SELECT kind, COUNT(*) AS n FROM documents GROUP BY kind ORDER BY n DESC"
            ).fetchall()
        }
        by_source = {
            r["source"]: r["n"]
            for r in self._conn.execute(
                "SELECT source, COUNT(*) AS n FROM documents GROUP BY source"
            ).fetchall()
        }
        total_size = self._conn.execute(
            "SELECT COALESCE(SUM(size), 0) AS s FROM documents"
        ).fetchone()["s"]
        last = self._conn.execute(
            "SELECT MAX(indexed_at) AS t FROM documents"
        ).fetchone()["t"]
        db_size = self.path.stat().st_size if self.path.exists() else 0
        tool_call_count = self._conn.execute(
            "SELECT COUNT(*) AS n FROM tool_calls"
        ).fetchone()["n"]
        return {
            "total": total,
            "by_kind": by_kind,
            "by_source": by_source,
            "total_content_bytes": total_size,
            "db_bytes": db_size,
            "last_indexed": last,
            "tool_calls": tool_call_count,
        }

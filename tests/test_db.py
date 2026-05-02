from __future__ import annotations

import tempfile
import unittest
import sqlite3
from pathlib import Path

from glom.db import Database, Document


class DatabaseBulkModeTests(unittest.TestCase):
    def test_bulk_mode_switches_journal_mode_and_restores_it(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            db = Database(Path(tmp) / "index.db")
            self.assertEqual(
                db._conn.execute("PRAGMA journal_mode").fetchone()[0].lower(),
                "wal",
            )

            db.begin_bulk()
            self.assertEqual(
                db._conn.execute("PRAGMA journal_mode").fetchone()[0].lower(),
                "memory",
            )
            self.assertEqual(
                db._conn.execute("PRAGMA synchronous").fetchone()[0],
                0,
            )

            db.end_bulk()
            self.assertEqual(
                db._conn.execute("PRAGMA journal_mode").fetchone()[0].lower(),
                "wal",
            )
            self.assertEqual(
                db._conn.execute("PRAGMA synchronous").fetchone()[0],
                1,
            )
            db.close()

    def test_bulk_mode_falls_back_to_wal_when_reader_holds_lock(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "index.db"
            db = Database(path)
            db.upsert(
                Document(
                    source="codex",
                    path="/tmp/a.jsonl",
                    kind="session",
                    project=None,
                    title="a",
                    content="hello",
                    metadata=None,
                    mtime=1.0,
                    size=5,
                )
            )
            db.commit()

            reader = sqlite3.connect(path)
            reader.execute("BEGIN")
            reader.execute("SELECT COUNT(*) FROM documents").fetchone()

            db.begin_bulk()
            self.assertEqual(
                db._conn.execute("PRAGMA journal_mode").fetchone()[0].lower(),
                "wal",
            )

            db.upsert(
                Document(
                    source="codex",
                    path="/tmp/b.jsonl",
                    kind="session",
                    project=None,
                    title="b",
                    content="world",
                    metadata=None,
                    mtime=2.0,
                    size=5,
                )
            )
            db.end_bulk()
            db.close()
            reader.close()

            check = Database(path)
            try:
                self.assertIsNotNone(check.get_document("/tmp/b.jsonl"))
            finally:
                check.close()


class DatabaseFilterTests(unittest.TestCase):
    def test_document_search_filters_by_repo_path_and_mtime(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            db = Database(Path(tmp) / "index.db")
            try:
                db.upsert(
                    Document(
                        source="codex",
                        path="/tmp/synth/new.md",
                        kind="memory",
                        project=None,
                        title="new",
                        content="needle in the new file",
                        metadata=None,
                        mtime=200.0,
                        size=22,
                    )
                )
                db.upsert(
                    Document(
                        source="codex",
                        path="/tmp/other/old.md",
                        kind="memory",
                        project=None,
                        title="old",
                        content="needle in the old file",
                        metadata=None,
                        mtime=100.0,
                        size=22,
                    )
                )
                db.commit()

                results, total = db.search("needle", repo="synth")
                self.assertEqual(total, 1)
                self.assertEqual(results[0].path, "/tmp/synth/new.md")

                results, total = db.search("needle", path_fragment="old")
                self.assertEqual(total, 1)
                self.assertEqual(results[0].path, "/tmp/other/old.md")

                results, total = db.search("needle", since=150.0)
                self.assertEqual(total, 1)
                self.assertEqual(results[0].path, "/tmp/synth/new.md")

                results, total = db.search("needle", until=150.0)
                self.assertEqual(total, 1)
                self.assertEqual(results[0].path, "/tmp/other/old.md")
            finally:
                db.close()

    def test_tool_search_filters_by_repo_path_and_session_mtime(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            db = Database(Path(tmp) / "index.db")
            try:
                for path, mtime in [
                    ("/tmp/synth/session.jsonl", 200.0),
                    ("/tmp/other/session.jsonl", 100.0),
                ]:
                    db.upsert(
                        Document(
                            source="codex",
                            path=path,
                            kind="session",
                            project=None,
                            title=Path(path).stem,
                            content="session",
                            metadata=None,
                            mtime=mtime,
                            size=7,
                        )
                    )
                    db.replace_tool_calls(
                        path,
                        "codex",
                        None,
                        [("1", "shell", '{"cmd":"git status"}', "clean", 0, 3, None)],
                    )
                db.commit()

                results, total = db.search_tool_calls("git", repo="synth")
                self.assertEqual(total, 1)
                self.assertEqual(results[0].session_path, "/tmp/synth/session.jsonl")

                results, total = db.search_tool_calls("git", path_fragment="other")
                self.assertEqual(total, 1)
                self.assertEqual(results[0].session_path, "/tmp/other/session.jsonl")

                results, total = db.search_tool_calls("git", since=150.0)
                self.assertEqual(total, 1)
                self.assertEqual(results[0].session_path, "/tmp/synth/session.jsonl")

                results, total = db.search_tool_calls("git", until=150.0)
                self.assertEqual(total, 1)
                self.assertEqual(results[0].session_path, "/tmp/other/session.jsonl")
            finally:
                db.close()


class DatabaseSearchRefTests(unittest.TestCase):
    def test_search_refs_replace_previous_results(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            db = Database(Path(tmp) / "index.db")
            try:
                db.save_search_refs("documents", ["/tmp/a.md", "/tmp/b.md"])
                self.assertEqual(db.resolve_search_ref("documents", "@1"), "/tmp/a.md")
                self.assertEqual(db.resolve_search_ref("documents", "@2"), "/tmp/b.md")

                db.save_search_refs("documents", ["/tmp/c.md"])
                self.assertEqual(db.resolve_search_ref("documents", "@1"), "/tmp/c.md")
                self.assertIsNone(db.resolve_search_ref("documents", "@2"))
                self.assertIsNone(db.resolve_search_ref("documents", "not-a-ref"))
            finally:
                db.close()


class DatabaseOptimizeTests(unittest.TestCase):
    def test_optimize_preserves_search_results(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            db = Database(Path(tmp) / "index.db")
            try:
                db.upsert(
                    Document(
                        source="codex",
                        path="/tmp/a.md",
                        kind="memory",
                        project=None,
                        title="a",
                        content="needle",
                        metadata=None,
                        mtime=1.0,
                        size=6,
                    )
                )
                db.commit()

                report = db.optimize()
                results, total = db.search("needle")

                self.assertEqual(total, 1)
                self.assertEqual(results[0].path, "/tmp/a.md")
                self.assertFalse(report["rebuild_fts"])
                self.assertIn("wal_checkpoint", report)
            finally:
                db.close()


if __name__ == "__main__":
    unittest.main()

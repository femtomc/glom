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


if __name__ == "__main__":
    unittest.main()

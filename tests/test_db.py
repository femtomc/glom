from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from glom.db import Database


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


if __name__ == "__main__":
    unittest.main()

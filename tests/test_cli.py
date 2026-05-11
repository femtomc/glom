from __future__ import annotations

import json
import os
import tempfile
import unittest
from pathlib import Path
from unittest.mock import MagicMock, patch

from click.testing import CliRunner
from rich.progress import Progress

from glom._compact import compact_table
from glom.cli import _doctor_report, _make_index_progress_callbacks, main


def _task(progress: Progress, task_id: int):
    return next(task for task in progress.tasks if task.id == task_id)


class IndexProgressTests(unittest.TestCase):
    def test_rebuild_uses_separate_indeterminate_task(self) -> None:
        with Progress(disable=True) as progress:
            index_task_id, rebuild_task_id, on_progress, on_phase = (
                _make_index_progress_callbacks(progress)
            )

            on_phase("indexing")
            on_progress(42, 100)

            on_phase("rebuilding")

            index_task = _task(progress, index_task_id)
            rebuild_task = _task(progress, rebuild_task_id)

            self.assertFalse(index_task.visible)
            self.assertEqual(index_task.completed, 42)
            self.assertEqual(index_task.total, 100)
            self.assertTrue(rebuild_task.visible)
            self.assertIsNone(rebuild_task.total)
            self.assertEqual(rebuild_task.description, "Rebuilding FTS")


class DoctorTests(unittest.TestCase):
    def test_doctor_reports_missing_index_rows(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            claude_root = root / ".claude"
            codex_root = root / ".codex"
            memories = codex_root / "memories"
            claude_root.mkdir()
            memories.mkdir(parents=True)
            (claude_root / "CLAUDE.md").write_text("# Claude\n")
            (memories / "MEMORY.md").write_text("# Memory\n")

            old_db = os.environ.get("GLOM_DB")
            os.environ["GLOM_DB"] = str(root / "index.db")
            try:
                report = _doctor_report(claude_root, codex_root)
            finally:
                if old_db is None:
                    os.environ.pop("GLOM_DB", None)
                else:
                    os.environ["GLOM_DB"] = old_db

            self.assertTrue(report["fts5"])
            self.assertEqual(report["discoverable"]["total"], 2)
            self.assertEqual(report["indexed"]["total"], 0)
            self.assertEqual(report["missing_from_index"]["count"], 2)
            self.assertTrue(report["needs_index"])


class ShowJsonTests(unittest.TestCase):
    """glom show --json truncates content by default; --full disables."""

    _DOC = {
        "path": "/tmp/test.jsonl",
        "kind": "session",
        "source": "claude",
        "project": "test",
        "title": "test-session",
        "size": 50000,
        "content": "x" * 10000,
        "indexed_at": 1700000000,
    }

    def _run_show(self, *args: str) -> str:
        runner = CliRunner()
        with patch("glom.cli.Database") as MockDB:
            instance = MockDB.return_value
            instance.find_document.return_value = self._DOC
            result = runner.invoke(main, ["show", "/tmp/test.jsonl", *args])
        assert result.exit_code == 0, result.output
        return result.output

    def test_json_truncates_by_default(self) -> None:
        out = self._run_show("--json")
        data = json.loads(out)
        self.assertEqual(len(data["content"]), 4000)
        self.assertTrue(data["_truncated"])

    def test_json_full_is_untruncated(self) -> None:
        out = self._run_show("--json", "--full")
        data = json.loads(out)
        self.assertEqual(len(data["content"]), 10000)
        self.assertNotIn("_truncated", data)

    def test_json_is_single_line(self) -> None:
        out = self._run_show("--json")
        json.loads(out)
        self.assertEqual(out.count("\n"), 1)

    def test_show_resolves_last_search_ref(self) -> None:
        runner = CliRunner()
        with patch("glom.cli.Database") as MockDB:
            instance = MockDB.return_value
            instance.resolve_search_ref.return_value = "/tmp/test.jsonl"
            instance.find_document.return_value = self._DOC
            result = runner.invoke(main, ["show", "@1", "--json"])

        self.assertEqual(result.exit_code, 0, result.output)
        instance.resolve_search_ref.assert_called_once_with("documents", "@1")
        instance.find_document.assert_called_once_with("/tmp/test.jsonl")


class ToolsNamesLimitTests(unittest.TestCase):
    """glom tools --names respects --limit and --full."""

    _COUNTS = {f"tool_{i}": 100 - i for i in range(30)}

    def _run_tools(self, *args: str) -> str:
        runner = CliRunner()
        with patch("glom.cli.Database") as MockDB:
            instance = MockDB.return_value
            instance.tool_name_counts.return_value = self._COUNTS
            result = runner.invoke(main, ["tools", "--names", *args])
        assert result.exit_code == 0, result.output
        return result.output

    def test_default_caps_at_20(self) -> None:
        out = self._run_tools()
        self.assertIn("showing 20 of 30", out)
        data_lines = [
            line for line in out.splitlines() if line.strip() and "tool_" in line
        ]
        self.assertEqual(len(data_lines), 20)

    def test_limit_5(self) -> None:
        out = self._run_tools("--limit", "5")
        self.assertIn("showing 5 of 30", out)
        data_lines = [
            line for line in out.splitlines() if line.strip() and "tool_" in line
        ]
        self.assertEqual(len(data_lines), 5)

    def test_full_shows_all(self) -> None:
        out = self._run_tools("--full")
        self.assertNotIn("showing", out)
        data_lines = [
            line for line in out.splitlines() if line.strip() and "tool_" in line
        ]
        self.assertEqual(len(data_lines), 30)

    def test_no_box_drawing(self) -> None:
        out = self._run_tools()
        import re

        self.assertIsNone(
            re.search(r"[┏━┃┗┓┛│─┬┼]", out),
            "compact output must not contain box-drawing characters",
        )

    def test_no_trailing_whitespace(self) -> None:
        for line in self._run_tools().splitlines():
            self.assertEqual(line, line.rstrip(), f"trailing whitespace: {line!r}")


class SearchCompactTests(unittest.TestCase):
    """glom search compact output and JSON envelope."""

    def _make_result(self, i: int) -> MagicMock:
        r = MagicMock()
        r.path = f"/tmp/session_{i}.jsonl"
        r.kind = "session"
        r.source = "claude"
        r.project = "proj"
        r.title = f"Session {i}"
        r.snippet = f"found match in line {i}"
        r.rank = 10.0 - i
        r.size = 5000
        return r

    def _run_search(self, *args: str, total: int = 50) -> str:
        runner = CliRunner()
        results = [self._make_result(i) for i in range(5)]
        with patch("glom.cli.Database") as MockDB:
            instance = MockDB.return_value
            instance.search.return_value = (results, total)
            result = runner.invoke(main, ["search", "test", *args])
        assert result.exit_code == 0, result.output
        return result.output

    def test_compact_shorter_than_full(self) -> None:
        compact = self._run_search()
        full = self._run_search("--full")
        self.assertLess(
            len(compact.splitlines()),
            len(full.splitlines()),
            "compact mode should produce fewer lines than --full",
        )

    def test_compact_has_canonical_columns(self) -> None:
        out = self._run_search()
        header = out.splitlines()[0]
        for col in ("ref", "rank", "kind", "name", "location", "snippet"):
            self.assertIn(col, header)

    def test_search_saves_refs(self) -> None:
        runner = CliRunner()
        results = [self._make_result(i) for i in range(3)]
        with patch("glom.cli.Database") as MockDB:
            instance = MockDB.return_value
            instance.search.return_value = (results, 3)
            result = runner.invoke(main, ["search", "test"])

        self.assertEqual(result.exit_code, 0, result.output)
        instance.save_search_refs.assert_called_once_with(
            "documents",
            [r.path for r in results],
        )

    def test_no_trailing_whitespace(self) -> None:
        for line in self._run_search().splitlines():
            self.assertEqual(line, line.rstrip(), f"trailing whitespace: {line!r}")

    def test_json_envelope(self) -> None:
        out = self._run_search("--json")
        data = json.loads(out)
        self.assertIn("rows", data)
        self.assertIn("total", data)
        self.assertIn("displayed", data)
        self.assertIn("truncated", data)
        self.assertIn("limit", data)
        self.assertEqual(data["displayed"], 5)
        self.assertEqual(data["total"], 50)
        self.assertTrue(data["truncated"])

    def test_json_envelope_is_single_line(self) -> None:
        out = self._run_search("--json")
        json.loads(out)
        self.assertEqual(out.count("\n"), 1)

    def test_json_legacy(self) -> None:
        out = self._run_search("--json-legacy")
        data = json.loads(out)
        self.assertIsInstance(data, list)
        self.assertEqual(len(data), 5)
        self.assertIn("path", data[0])


class ContextCommandTests(unittest.TestCase):
    def test_context_json_includes_window_and_tool_calls(self) -> None:
        result_row = MagicMock()
        result_row.path = "/tmp/session.jsonl"
        result_row.kind = "session"
        result_row.source = "codex"
        result_row.project = None
        result_row.title = "session"
        result_row.snippet = "found needle"
        result_row.rank = 0.0
        result_row.size = 100

        tool_row = MagicMock()
        tool_row.tool_name = "shell"
        tool_row.line_number = 4
        tool_row.is_error = False
        tool_row.input_snippet = '{"cmd":"git status"}'
        tool_row.output_snippet = "clean"

        runner = CliRunner()
        with patch("glom.cli.Database") as MockDB:
            instance = MockDB.return_value
            instance.search.return_value = ([result_row], 1)
            instance.get_document.return_value = {
                "content": "before\nneedle appears here\nafter\n",
            }
            instance.tool_calls_for_session.return_value = [tool_row]
            invoke_result = runner.invoke(
                main,
                ["context", "needle", "--json"],
            )

        self.assertEqual(invoke_result.exit_code, 0, invoke_result.output)
        data = json.loads(invoke_result.output)
        self.assertEqual(data["displayed"], 1)
        self.assertEqual(data["rows"][0]["ref"], "@1")
        self.assertIn("needle appears here", data["rows"][0]["context"])
        self.assertEqual(data["rows"][0]["tool_calls"][0]["tool"], "shell")
        instance.save_search_refs.assert_called_once_with(
            "documents",
            ["/tmp/session.jsonl"],
        )


class CompactTableTests(unittest.TestCase):
    """Unit tests for the compact_table() formatter."""

    def test_basic_output(self) -> None:
        cols = [
            ("name", "name", {}),
            ("count", "count", {"align": "right", "max_width": 10}),
        ]
        rows = [{"name": "alpha", "count": 42}, {"name": "beta", "count": 7}]
        out = compact_table(rows, cols)
        lines = out.splitlines()
        self.assertEqual(lines[0].split(), ["name", "count"])
        self.assertTrue(all(c in "-  " for c in lines[1]))
        self.assertIn("alpha", lines[2])

    def test_no_trailing_whitespace(self) -> None:
        cols = [("a", "a", {}), ("b", "b", {})]
        rows = [{"a": "x", "b": "y"}, {"a": "long value", "b": ""}]
        out = compact_table(rows, cols)
        for line in out.splitlines():
            self.assertEqual(line, line.rstrip())

    def test_footer(self) -> None:
        cols = [("x", "x", {})]
        rows = [{"x": "a"}]
        out = compact_table(rows, cols, total=100)
        self.assertIn("showing 1 of 100", out)

    def test_no_footer_when_all_shown(self) -> None:
        cols = [("x", "x", {})]
        rows = [{"x": "a"}]
        out = compact_table(rows, cols, total=1)
        self.assertNotIn("showing", out)

    def test_zero_rows(self) -> None:
        cols = [("name", "name", {})]
        out = compact_table([], cols)
        lines = out.strip().splitlines()
        self.assertEqual(len(lines), 2)  # header + separator only

    def test_truncation(self) -> None:
        cols = [("x", "x", {"max_width": 5})]
        rows = [{"x": "abcdefghij"}]
        out = compact_table(rows, cols)
        body = out.splitlines()[2]
        self.assertIn("\u2026", body)
        self.assertLessEqual(len(body), 6)  # 4 chars + ellipsis


if __name__ == "__main__":
    unittest.main()

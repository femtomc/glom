from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

from glom.indexer import _parse_session, index_all


class FakeDB:
    def __init__(self, existing_mtimes: dict[str, float]):
        self.existing_mtimes = existing_mtimes
        self.upserted: list[object] = []
        self.deleted: list[str] = []
        self.committed = False

    def get_all_mtimes(self) -> dict[str, float]:
        return dict(self.existing_mtimes)

    def get_mtime(self, path: str) -> float | None:
        raise AssertionError("index_all should preload mtimes once")

    def begin_bulk(self) -> None:
        raise AssertionError("bulk mode should not trigger in this test")

    def end_bulk(self) -> None:
        raise AssertionError("bulk mode should not trigger in this test")

    def upsert(self, doc: object) -> None:
        self.upserted.append(doc)

    def replace_tool_calls(
        self,
        session_path: str,
        source: str,
        project: str | None,
        calls: list[tuple],
    ) -> int:
        raise AssertionError("tool-call replacement is not expected in this test")

    def delete_path(self, path: str) -> None:
        self.deleted.append(path)

    def commit(self) -> None:
        self.committed = True


class IndexAllTests(unittest.TestCase):
    def test_incremental_index_uses_preloaded_mtimes(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            claude_root = Path(tmp) / ".claude"
            claude_root.mkdir()
            instructions = claude_root / "CLAUDE.md"
            instructions.write_text("# Project instructions\n")
            mtime = instructions.stat().st_mtime

            stale = str(claude_root / "stale.md")
            db = FakeDB({
                str(instructions): mtime,
                stale: 1.0,
            })

            stats = index_all(db, claude_root=claude_root, codex_root=Path(tmp) / ".codex")

            self.assertEqual(stats.unchanged, 1)
            self.assertEqual(stats.deleted, 1)
            self.assertEqual(stats.new, 0)
            self.assertEqual(stats.updated, 0)
            self.assertEqual(db.upserted, [])
            self.assertEqual(db.deleted, [stale])
            self.assertTrue(db.committed)

    def test_parse_claude_session_keeps_transcript_and_skips_tool_payloads(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "session.jsonl"
            path.write_text(
                "\n".join([
                    json.dumps({
                        "type": "user",
                        "message": {"role": "user", "content": "Find the failure cause"},
                    }),
                    json.dumps({
                        "type": "assistant",
                        "message": {
                            "role": "assistant",
                            "content": [
                                {"type": "tool_use", "id": "1", "name": "Bash", "input": {"command": "git status"}},
                            ],
                        },
                    }),
                    json.dumps({
                        "type": "user",
                        "message": {
                            "role": "user",
                            "content": [
                                {"type": "tool_result", "tool_use_id": "1", "content": "tool stdout"},
                            ],
                        },
                        "toolUseResult": {"stdout": "tool stdout", "stderr": ""},
                    }),
                    json.dumps({
                        "type": "assistant",
                        "message": {
                            "role": "assistant",
                            "content": [
                                {"type": "text", "text": "The failure is in the indexing path."},
                            ],
                        },
                    }),
                ]) + "\n"
            )

            _, content, tool_calls = _parse_session(path, "claude")

            self.assertIn("Find the failure cause", content)
            self.assertIn("The failure is in the indexing path.", content)
            self.assertNotIn("tool stdout", content)
            self.assertEqual(len(tool_calls), 1)

    def test_parse_codex_session_skips_repeated_metadata(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "session.jsonl"
            path.write_text(
                "\n".join([
                    json.dumps({
                        "type": "session_meta",
                        "payload": {
                            "base_instructions": {"text": "Very long repeated instructions"},
                        },
                    }),
                    json.dumps({
                        "type": "event_msg",
                        "payload": {"type": "user_message", "message": "Optimize the session index"},
                    }),
                    json.dumps({
                        "type": "event_msg",
                        "payload": {"type": "agent_message", "message": "Profiling the cold build now."},
                    }),
                    json.dumps({
                        "type": "event_msg",
                        "payload": {"type": "token_count", "rate_limits": {"used_percent": 74.0}},
                    }),
                    json.dumps({
                        "type": "response_item",
                        "payload": {
                            "type": "message",
                            "role": "developer",
                            "content": [{"type": "input_text", "text": "Developer instructions"}],
                        },
                    }),
                    json.dumps({
                        "type": "event_msg",
                        "payload": {"type": "function_call_output", "call_id": "1", "output": "tool output"},
                    }),
                ]) + "\n"
            )

            _, content, tool_calls = _parse_session(path, "codex")

            self.assertIn("Optimize the session index", content)
            self.assertIn("Profiling the cold build now.", content)
            self.assertNotIn("Very long repeated instructions", content)
            self.assertNotIn("Developer instructions", content)
            self.assertNotIn("tool output", content)
            self.assertEqual(len(tool_calls), 0)


if __name__ == "__main__":
    unittest.main()

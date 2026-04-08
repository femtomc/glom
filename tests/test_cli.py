from __future__ import annotations

import unittest

from rich.progress import Progress

from glom.cli import _make_index_progress_callbacks


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


if __name__ == "__main__":
    unittest.main()

"""Canonical compact-table formatter (canonical compact-format spec)."""

from __future__ import annotations

import unicodedata
from collections.abc import Mapping, Sequence

# Column = (header_lowercase, row_key, options_dict)
# options: "align" ("left"|"right", default "left"), "max_width" (int, default 40)
Column = tuple[str, str, dict]

_GUTTER = "  "

TRUNCATION_LINE = (
    "(output truncated at 16 KB"
    " \u2014 run with --json for full data or --limit to scope down)"
)
_TRUNCATION_RESERVED = len(TRUNCATION_LINE.encode("utf-8")) + 1  # +1 for \n
_FULL_CAP = 16_384
_BODY_BUDGET = _FULL_CAP - _TRUNCATION_RESERVED


def _char_width(ch: str) -> int:
    cat = unicodedata.category(ch)
    if cat.startswith("C"):  # control
        return 0
    eaw = unicodedata.east_asian_width(ch)
    if eaw in ("W", "F"):
        return 2
    if unicodedata.combining(ch):
        return 0
    return 1


def _wcswidth(s: str) -> int:
    w = 0
    for ch in s:
        cw = _char_width(ch)
        if cw < 0:
            return -1
        w += cw
    return w


def _truncate_to_width(s: str, max_w: int) -> str:
    if _wcswidth(s) <= max_w:
        return s
    target = max_w - 1  # leave room for ellipsis
    w = 0
    cut = 0
    for i, ch in enumerate(s):
        cw = _char_width(ch)
        if w + cw > target:
            cut = i
            break
        w += cw
        cut = i + 1
    return s[:cut] + "\u2026"


def _normalize_cell(value: object) -> str:
    if value is None:
        return "-"
    s = str(value)
    if not s:
        return ""
    s = s.replace("\t", " ")
    s = s.replace("\r\n", " ").replace("\r", " ").replace("\n", " ")
    out: list[str] = []
    for ch in s:
        cat = unicodedata.category(ch)
        if cat.startswith("C") and ch not in ("\t",):
            continue
        if cat == "Cn":
            continue
        out.append(ch)
    return "".join(out)


def compact_table(
    rows: Sequence[Mapping[str, object]],
    columns: Sequence[Column],
    *,
    total: int | None = None,
) -> str:
    """Render rows as the canonical compact table.

    Column = tuple[header_lowercase: str, row_key: str, options: dict]
      options accepts:
        "align":     "left" | "right"   (default: "left")
        "max_width": int                 (default: 40)
    total: if provided and total > len(rows), a footer is emitted.
    """
    # --- compute display strings and widths ---
    col_max: list[int] = []
    col_align: list[str] = []
    headers: list[str] = []
    cell_grid: list[list[str]] = []

    for header, _key, opts in columns:
        max_w = opts.get("max_width", 40)
        align = opts.get("align", "left")
        col_align.append(align)
        col_max.append(max_w)
        headers.append(_truncate_to_width(header.lower(), max_w))

    for row in rows:
        cells: list[str] = []
        for (_header, key, opts) in columns:
            max_w = opts.get("max_width", 40)
            raw = row.get(key) if isinstance(row, Mapping) else getattr(row, key, None)
            norm = _normalize_cell(raw)
            cells.append(_truncate_to_width(norm, max_w))
        cell_grid.append(cells)

    # --- column widths ---
    n_cols = len(columns)
    widths: list[int] = []
    for ci in range(n_cols):
        hw = _wcswidth(headers[ci])
        if hw < 0:
            hw = 0
        max_cell = 0
        for row_cells in cell_grid:
            cw = _wcswidth(row_cells[ci])
            if cw > max_cell:
                max_cell = cw
        widths.append(max(hw, max_cell))

    # --- render lines ---
    lines: list[str] = []

    # header
    parts: list[str] = []
    for ci in range(n_cols):
        w = widths[ci]
        h = headers[ci]
        hw = _wcswidth(h)
        if col_align[ci] == "right":
            parts.append(" " * (w - hw) + h)
        else:
            parts.append(h + " " * (w - hw))
    lines.append(_GUTTER.join(parts).rstrip())

    # separator
    parts = ["-" * widths[ci] for ci in range(n_cols)]
    lines.append(_GUTTER.join(parts).rstrip())

    # body rows
    for row_cells in cell_grid:
        parts = []
        for ci in range(n_cols):
            w = widths[ci]
            cell = row_cells[ci]
            cw = _wcswidth(cell)
            if cw < 0:
                cw = 0
            pad = w - cw
            if col_align[ci] == "right":
                parts.append(" " * pad + cell)
            else:
                parts.append(cell + " " * pad)
        lines.append(_GUTTER.join(parts).rstrip())

    # footer
    if total is not None and total > len(rows):
        lines.append(
            f"(showing {len(rows)} of {total}"
            " \u2014 use --limit 0 for all, --json for machine-readable)"
        )

    return "\n".join(lines) + "\n"


def apply_16kb_cap(output: str) -> str:
    """Enforce the 16 KB UTF-8 byte cap with line-boundary cuts."""
    raw = output.encode("utf-8")
    if len(raw) <= _BODY_BUDGET:
        return output
    # line-boundary cut
    cumulative = 0
    last_ok = 0
    lines = output.split("\n")
    for i, line in enumerate(lines):
        line_bytes = len((line + "\n").encode("utf-8"))
        if cumulative + line_bytes > _BODY_BUDGET:
            break
        cumulative += line_bytes
        last_ok = i + 1
    if last_ok == 0:
        # first line overflow
        return TRUNCATION_LINE + "\n"
    kept = "\n".join(lines[:last_ok]) + "\n"
    return kept + TRUNCATION_LINE + "\n"

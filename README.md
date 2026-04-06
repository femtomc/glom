# glom

Index and full-text search over agent context stored in `~/.claude` and `~/.codex`.

glom walks both directories, parses every discoverable file — session
transcripts, memory files, plans, tasks, skills, instructions, settings,
history — and stores the content in a local SQLite database with FTS5
full-text indexing.  Session JSONL files also get structured tool-call
extraction (tool name, input, output, error status, line number), stored in
a separate searchable table.

Session document search indexes human-facing transcript text. Structured tool
payloads and repeated session metadata stay in the dedicated `tool_calls`
table instead of being duplicated into document search.

## Install

```
uv tool install git+https://github.com/femtomc/glom
```

## Usage

### Index

```
glom index          # incremental (mtime-based, skips unchanged files)
glom index --full   # force full re-index
```

Bulk mode (deferred FTS rebuild) activates automatically when >100 files
need processing.

### Search documents

```
glom search "bellman orchestration"
glom search "feedback" -k memory          # filter by kind
glom search "monadic core" -p tiny        # filter by project slug
glom search "protocol" -s claude -n 5     # filter by source, limit results
glom search "deploy" --json               # structured output for agents
```

Kinds: `memory`, `plan`, `task`, `session`, `skill`, `instructions`,
`settings`, `history`.

### Search tool calls

```
glom tools "git push" -t Bash             # search Bash calls for "git push"
glom tools '"pyproject.toml"' -t Read     # phrase search (FTS5 syntax)
glom tools --names                        # list all tool names with counts
glom tools "error" --json                 # JSON output
```

### Inspect

```
glom stats                                # index statistics
glom show ~/.claude/projects/.../memory/MEMORY.md   # display a document
glom show MEMORY.md                       # suffix match
```

All commands support `--json` for agent consumption.

## What gets indexed

| Kind | Source files |
|---|---|
| `session` | `projects/*/*.jsonl`, `sessions/*/*/*/*.jsonl` |
| `memory` | `projects/*/memory/*.md` |
| `plan` | `plans/*.md` |
| `task` | `tasks/*/*.json` |
| `skill` | `skills/*/SKILL.md` |
| `instructions` | `CLAUDE.md`, `AGENTS.md`, project-level `CLAUDE.md` |
| `settings` | `settings.json`, `config.toml` |
| `history` | `history.jsonl` |

Session files also produce structured `tool_calls` rows with tool name,
input arguments, output text, error flag, and source line number.

## Configuration

Set `GLOM_DB` to override the database path (default:
`~/.local/share/glom/index.db`).

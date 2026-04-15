<!-- self-improvement workflow -->
## Self-Improvement Workflow

When errors, corrections, or insights occur during development, log them immediately to `.learnings/`.

| Situation | File | ID Format |
|-----------|------|-----------|
| Command fails, scraper bug, integration error | `.learnings/ERRORS.md` | `ERR-YYYYMMDD-XXX` |
| User corrects Claude, better approach found, outdated knowledge | `.learnings/LEARNINGS.md` | `LRN-YYYYMMDD-XXX` |
| User requests a missing capability | `.learnings/FEATURE_REQUESTS.md` | `FEAT-YYYYMMDD-XXX` |

### Promotion Rules

When a learning is broadly applicable (not a one-off), promote it:

- `CLAUDE.md` — project facts, ATS quirks, pipeline conventions
- `AGENTS.md` — workflows, automation triggers, scraper patterns
- `GEMINI.md` — if relevant to Gemini-based sessions

Promote when `Recurrence-Count >= 3` and seen across 2+ tasks within 30 days.

### Quick Status Check

```bash
grep -h "Status\*\*: pending" .learnings/*.md | wc -l        # count pending
grep -B5 "Priority\*\*: high" .learnings/*.md | grep "^## \["  # high priority
```

### This Project's Key Areas

- `backend` — scraper.py, enrich.py, signal_scraper.py, verdict_engine.py
- `infra` — scrape.yml, GitHub Actions, seen_signals.json dedup
- `config` — competitors.csv, .claude/settings.json, requirements.txt
- `frontend` — dashboard.html, dashboard_v2.html

---

<!-- code-review-graph MCP tools -->
## MCP Tools: code-review-graph

**IMPORTANT: This project has a knowledge graph. ALWAYS use the
code-review-graph MCP tools BEFORE using Grep/Glob/Read to explore
the codebase.** The graph is faster, cheaper (fewer tokens), and gives
you structural context (callers, dependents, test coverage) that file
scanning cannot.

### When to use graph tools FIRST

- **Exploring code**: `semantic_search_nodes` or `query_graph` instead of Grep
- **Understanding impact**: `get_impact_radius` instead of manually tracing imports
- **Code review**: `detect_changes` + `get_review_context` instead of reading entire files
- **Finding relationships**: `query_graph` with callers_of/callees_of/imports_of/tests_for
- **Architecture questions**: `get_architecture_overview` + `list_communities`

Fall back to Grep/Glob/Read **only** when the graph doesn't cover what you need.

### Key Tools

| Tool | Use when |
|------|----------|
| `detect_changes` | Reviewing code changes — gives risk-scored analysis |
| `get_review_context` | Need source snippets for review — token-efficient |
| `get_impact_radius` | Understanding blast radius of a change |
| `get_affected_flows` | Finding which execution paths are impacted |
| `query_graph` | Tracing callers, callees, imports, tests, dependencies |
| `semantic_search_nodes` | Finding functions/classes by name or keyword |
| `get_architecture_overview` | Understanding high-level codebase structure |
| `refactor_tool` | Planning renames, finding dead code |

### Workflow

1. The graph auto-updates on file changes (via hooks).
2. Use `detect_changes` for code review.
3. Use `get_affected_flows` to understand impact.
4. Use `query_graph` pattern="tests_for" to check coverage.

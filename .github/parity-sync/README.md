# Parity sync agent

Keeps **durust** aligned with the upstream DBOS Transact SDKs by watching their
GitHub activity and filing tracking issues here for the parity-relevant changes.

## What it does

Weekly (and on demand), it:

1. **Fetches** recently merged PRs + updated issues from the four upstream SDKs —
   `dbos-transact-{ts,py,golang,java}` (`fetch.sh` → `upstream.json`).
2. **Classifies** each item with Claude Code (`classify-and-file.md`): is it a
   feature / bugfix / semantic change durust should match, and which `PARITY.md`
   phase does it map to? Docs, CI, and dependency bumps are dropped.
3. **Files** a deduplicated, labelled tracking issue in `SamuelXing/durust` for
   each new parity-relevant item, mapped to a PARITY phase with a suggested
   action.

Go is treated as the parity source of truth (per `PARITY.md`); TS/Java-only
features are filed as "early signal."

## Pieces

| File | Role |
|---|---|
| `fetch.sh` | Stateless fetch of upstream PRs/issues → `upstream.json`. |
| `classify-and-file.md` | The Claude Code prompt: classify, dedup, file issues. |
| `../workflows/parity-sync.yml` | Cron + manual trigger wiring the two together. |

## Setup

1. Add an **`ANTHROPIC_API_KEY`** repo secret (Settings → Secrets and variables →
   Actions). The built-in `GITHUB_TOKEN` covers reading the public upstream
   repos and creating issues here.
2. Confirm the `anthropics/claude-code-action` input names match the version you
   pin — the marketplace action's interface evolves; adjust `with:` if needed.
   (A self-contained alternative is to `npm i -g @anthropic-ai/claude-code` and
   run `claude -p "$(cat .github/parity-sync/classify-and-file.md)"` directly.)

## Design notes

- **Idempotent, no stored cursor.** Each issue body carries a hidden marker
  `<!-- upstream:<repo>#<number> -->`; the classifier searches for it before
  filing, so overlapping fetch windows and re-runs never duplicate. The lookback
  window just bounds the fetch — make it comfortably larger than the cron
  interval (default 21 days for a weekly run).
- **Triage, not mirror.** It surfaces what durust should *consider*, mapped to
  the roadmap — it does not copy issues verbatim and does not edit `PARITY.md`
  or source. You stay the gate: triage the filed issues into work.
- **First run.** Trigger manually with a wider `since` (e.g. 90 days) once to
  seed the backlog, then let the weekly cron keep it current.

## Run locally

```bash
LOOKBACK_DAYS=30 bash .github/parity-sync/fetch.sh /tmp/upstream.json
# then feed the prompt to Claude Code with gh available:
claude -p "$(cat .github/parity-sync/classify-and-file.md)" --allowedTools 'Bash(gh:*),Read'
```

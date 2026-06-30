#!/usr/bin/env bash
#
# Fetch recent merged PRs and updated issues from the upstream DBOS Transact
# SDKs into a single JSON array for the parity-sync classifier to triage.
#
# Stateless by design: it always looks back a fixed window (LOOKBACK_DAYS,
# comfortably larger than the cron interval) and relies on the classifier's
# per-item dedup marker to avoid re-filing. Override the window start with
# SYNC_SINCE=YYYY-MM-DD (e.g. for a one-off historical sweep).
#
# Usage: fetch.sh [output.json]   (default: upstream.json)

set -euo pipefail

LOOKBACK_DAYS="${LOOKBACK_DAYS:-21}"
OUT="${1:-upstream.json}"

# The four upstream DBOS Transact SDKs (durust tracks these for parity).
REPOS=(dbos-transact-ts dbos-transact-py dbos-transact-golang dbos-transact-java)

# Resolve the lookback start date. Honour an explicit override, else compute
# it portably across GNU date (CI runners) and BSD date (local macOS).
if [[ -n "${SYNC_SINCE:-}" ]]; then
  SINCE="$SYNC_SINCE"
elif date -u -d "1 day ago" +%Y-%m-%d >/dev/null 2>&1; then
  SINCE="$(date -u -d "${LOOKBACK_DAYS} days ago" +%Y-%m-%d)"   # GNU
else
  SINCE="$(date -u -v-"${LOOKBACK_DAYS}"d +%Y-%m-%d)"           # BSD/macOS
fi

echo "[]" > "$OUT"
tmp="$(mktemp -d)"
trap 'rm -rf "$tmp"' EXIT

# Keep only what parity work cares about, and trim bodies so the classifier
# prompt stays small. Bots are dropped here; docs/chore filtering is left to
# the classifier, which can read the diff context.
filter_prs='[.[] | select((.author.login // "") | endswith("[bot]") | not)
  | {repo:$repo, kind:"pr", number, title, url,
     body:((.body // "")[0:1200]), labels:[.labels[].name], at:.mergedAt}]'
filter_issues='[.[] | select((.author.login // "") | endswith("[bot]") | not)
  | {repo:$repo, kind:"issue", number, title, url,
     body:((.body // "")[0:1200]), labels:[.labels[].name], at:.updatedAt}]'

for repo in "${REPOS[@]}"; do
  slug="dbos-inc/$repo"

  gh pr list --repo "$slug" --state merged --limit 100 \
     --search "merged:>=$SINCE" \
     --json number,title,url,body,labels,author,mergedAt \
     2>/dev/null | jq --arg repo "$repo" "$filter_prs" > "$tmp/pr.json" \
     || echo "[]" > "$tmp/pr.json"

  gh issue list --repo "$slug" --state all --limit 100 \
     --search "updated:>=$SINCE" \
     --json number,title,url,body,labels,author,updatedAt \
     2>/dev/null | jq --arg repo "$repo" "$filter_issues" > "$tmp/issue.json" \
     || echo "[]" > "$tmp/issue.json"

  jq -s 'add' "$OUT" "$tmp/pr.json" "$tmp/issue.json" > "$tmp/merged.json"
  mv "$tmp/merged.json" "$OUT"
done

echo "Wrote $(jq length "$OUT") item(s) to $OUT (since $SINCE)"

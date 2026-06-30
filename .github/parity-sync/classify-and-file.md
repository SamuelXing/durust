# Parity sync — classify upstream activity and file tracking issues

You keep **SamuelXing/durust** (an independent Rust implementation of the DBOS
Transact SDK) in parity with the four upstream DBOS Transact SDKs. The file
`upstream.json` (in the working directory) lists merged PRs and updated issues
from those SDKs over the recent window. Surface the **parity-relevant** ones as
deduplicated tracking issues in `SamuelXing/durust`.

The upstream SDKs are: `dbos-transact-ts` (TypeScript, reference impl),
`dbos-transact-py` (Python), `dbos-transact-golang` (Go), `dbos-transact-java`
(Java). Short repo→label slugs: ts, py, go, java.

## Source of truth
`PARITY.md` says **Go is the parity source of truth**, with Python referenced
where Go trails. Weight accordingly: a feature already in Go is "do now"; a
feature only in TS/Java with no Go/Py equivalent is "early signal" — still file
it, but say so in the body and don't imply it's overdue.

## Steps

1. Read `upstream.json` and `PARITY.md`.

2. For each item, classify:
   - **kind**: `feature` | `bugfix` | `semantics` | `skip`. Skip docs, CI,
     dependency bumps, pure refactors, and anything with no behavioral effect on
     the SDK's contract.
   - **parity_relevant**: would durust need to change to match this behavior?
     If no, skip it.
   - **phase**: the `PARITY.md` phase/row it maps to, or `unmapped`.
   - Note its current durust status from PARITY.md (✅/🟡/❌). An item already ✅
     in durust is usually skip — unless it's a *bugfix* to behavior durust also
     has, in which case file it as `kind:bugfix`.

3. **Dedup before filing.** For each item you would file, run:
   ```
   gh issue list --repo SamuelXing/durust --state all \
     --search "in:body upstream:<repo>#<number>"
   ```
   If a result exists, skip it (already tracked). The marker makes re-runs safe.

4. **Ensure labels exist** (ignore "already exists" errors):
   ```
   gh label create parity        -c "#0E8A16" --repo SamuelXing/durust 2>/dev/null || true
   gh label create kind:feature  -c "#1D76DB" --repo SamuelXing/durust 2>/dev/null || true
   gh label create kind:bugfix   -c "#D93F0B" --repo SamuelXing/durust 2>/dev/null || true
   gh label create kind:semantics -c "#FBCA04" --repo SamuelXing/durust 2>/dev/null || true
   ```
   …and `from:ts|py|go|java` and `phase:0`…`phase:12` and `unmapped` as needed.

5. **File a tracking issue** for each new parity-relevant item:
   ```
   gh issue create --repo SamuelXing/durust \
     --title "[parity][<repo-slug>] <concise feature name>" \
     --label "parity,kind:<kind>,from:<repo-slug>,phase:<n>" \
     --body "<body>"
   ```
   The body MUST contain, in order:
   - `Upstream: dbos-inc/<repo>#<number> — <url> (<merged|updated> <at>)`
   - **Summary** — 2–3 lines: what changed and why it matters for durability semantics.
   - **PARITY mapping** — the phase/row and its current durust status.
   - **Suggested action** — what the Rust SDK would implement/fix, concretely.
   - **Cross-SDK presence** — which of the 4 SDKs already have this; flag "early
     signal" if Go doesn't yet.
   - A trailing HTML marker on its own line: `<!-- upstream:<repo>#<number> -->`

## Noise & cost control
- Cap at **25 new issues per run**. If more qualify, file the highest-signal
  first (merged feature PRs in Go, then Python, then bugfixes, then TS/Java
  early signals) and print a list of what you deferred.
- Prefer merged PRs over open issues as evidence; an open *issue* is only worth
  filing if it documents a real semantic gap/bug durust shares.
- Group near-duplicates (the same feature landing in several SDKs) into **one**
  durust issue, with all upstream links and one marker per upstream item.

## Hard constraints
- **Idempotent**: never create a second issue for an upstream item that already
  has a marker. When in doubt, search first.
- **Read-only on the repo**: do NOT edit `PARITY.md` or any source file, and do
  NOT close or reopen issues. You may only *create* issues and *create* labels,
  and add a missing label to an issue you just created.
- Stay within `SamuelXing/durust` for writes; the upstream repos are read-only.

## Finish
Print a digest: `scanned N · filed M · skipped K`, with skipped grouped by
reason (docs/chore, already ✅, already tracked, not parity-relevant, deferred).

# merlin-search

Fetch proxy for Merlin's lead-enrichment pipeline (Neil Fox Agency).

GitHub Actions runners fetch lead websites and extract contact emails, so
the main VM only ever talks to `api.github.com` (one domain) instead of
thousands of business domains.

## How a batch flows

1. The dispatcher (`ops/merlin-fetch/` on the VM) writes
   `batches/<batch_id>/targets.json` via the GitHub API and dispatches the
   `fetch-emails` workflow with `batch_id`. Batch IDs use the `merlin-` prefix.
2. The runner installs deps + headless Chromium, runs
   `worker/fetch_contacts.py`, and commits `batches/<batch_id>/results.json`.
3. The dispatcher polls for `results.json` and ingests found emails into
   `crm/leads.db` as `email_source=github_fetch` (best-confidence first-party
   address per lead, suppression + MX checked before storing).

## Worker output consumed

Per company, `results.json` holds `emails` (every first-party address with
`confidence`, `source_url`, `source_type`) and `role_emails`. The dispatcher
uses those fields; the `contacts` (named decision-makers) hits are ignored.

## Hard gates

- This system NEVER sends anything. It only extracts public information.
- Found emails are UNVERIFIED until they pass the normal verification tiers.
- Emails are never pattern-guessed, only reported as actually found.

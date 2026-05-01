# GCP Deployment Plan: Realistic Inline Analyzer With Simulated Ledger

## Summary

Deploy AGORA as an inline transaction analyzer on GCP using Cloud Run and Cloud SQL PostgreSQL. A sender-side client submits a transaction to AGORA, AGORA evaluates it with CatBoost plus the risk agent, then either forwards it to the receiver ledger, holds it for analyst approval, or blocks/cancels it.

No real money moves in v1. Balances are simulated in a database so the demo feels like a real payment path without payment-gateway risk.

## Target Architecture

- Deploy `agora-api` on Cloud Run as the real backend entrypoint.
- Deploy `agora-dashboard` on Cloud Run as the Streamlit analyst dashboard.
- Replace local SQLite runtime state with Cloud SQL PostgreSQL.
- Store `GROQ_API_KEY`, database credentials, and API keys in Secret Manager.
- Keep `agora_fraud_model.cbm` packaged with the API container for v1.
- Use a simulated ledger:
  - Sender has `available_balance` and `held_balance`.
  - Receiver has `available_balance`.
  - Held transactions reserve sender funds but do not credit receiver.
  - Analyst approval releases held funds to receiver.
  - Analyst block cancels the hold and returns funds to sender.
  - Escalation keeps the hold active.

## Backend API

Add a FastAPI backend, for example `api.py`, with these endpoints:

### `POST /transactions`

Called by the sender-side app.

Input:

- `transaction_id`
- `idempotency_key`
- `sender_id`
- `receiver_id`
- `type`
- `amount`
- optional metadata

Output statuses:

- `SETTLED`
- `HELD_PENDING_APPROVAL`
- `BLOCKED`
- `AGENT_ERROR`

Behavior:

- Validate sender balance.
- Run CatBoost.
- Run the risk agent only when ML flags fraud.
- Persist investigation and control-command audit data.
- Update the simulated ledger.

### `GET /transactions/{transaction_id}`

Returns transaction status, ledger movement, risk decision, and approval state.

### `POST /transactions/{transaction_id}/analyst-decision`

Input:

- `APPROVE_RELEASE`
- `CONFIRM_BLOCK`
- `ESCALATE`
- optional note

Behavior:

- `APPROVE_RELEASE`: debit held sender funds and credit receiver, status `RELEASED_AFTER_APPROVAL`.
- `CONFIRM_BLOCK`: release held funds back to sender, status `BLOCKED_CONFIRMED`.
- `ESCALATE`: keep funds held, status `ESCALATED`.

### `GET /events`

Used by the dashboard to list investigation events.

### `GET /ledger/accounts`

Used by the dashboard/demo to show sender and receiver balances.

Use an `X-AGORA-API-Key` header for the demo API. Later, replace this with Cloud Run IAM or Identity-Aware Proxy.

## App Refactor

- Extract shared risk logic from `dashboard.py` into reusable service functions so both API and dashboard use the same decision rules.
- Move database access behind a repository layer that supports PostgreSQL in cloud and can optionally support SQLite locally.
- Update `dashboard.py` so it no longer processes `X_test.csv` as the source of truth in deployed mode.
- Keep a demo sender panel or script that submits transactions into `POST /transactions`.
- Dashboard should read from API/Cloud SQL and show:
  - live submitted transactions,
  - held queue,
  - analyst decision panel,
  - sender/receiver ledger balances,
  - command/audit history.

## Data Model

Create PostgreSQL tables for:

- `accounts`: `account_id`, `available_balance`, `held_balance`, timestamps.
- `transactions`: transaction identity, sender, receiver, amount, status, idempotency key, timestamps.
- `investigation_events`: current fields from SQLite plus approval fields.
- `analyst_reviews`: existing review audit log.
- `control_commands`: `hold`, `release`, `confirm_block`, and acknowledgments.
- `ledger_entries`: immutable debit, credit, hold, release, and cancel-hold entries.

Keep transaction status values:

```text
RECEIVED
ANALYZING
SETTLED
HELD_PENDING_APPROVAL
RELEASED_AFTER_APPROVAL
BLOCKED_CONFIRMED
ESCALATED
AGENT_ERROR
```

## GCP Deployment

- Create a GCP project and enable Cloud Run, Cloud SQL Admin API, Artifact Registry, Secret Manager, and Cloud Build.
- Create a Cloud SQL PostgreSQL instance in the same region as Cloud Run.
- Create two Cloud Run services:
  - `agora-api`: FastAPI backend.
  - `agora-dashboard`: Streamlit dashboard.
- Store secrets in Secret Manager:
  - `GROQ_API_KEY`
  - `DATABASE_URL` or individual DB credentials
  - `AGORA_API_KEY`
- Deploy containers through Artifact Registry.
- Connect Cloud Run to Cloud SQL using Cloud Run's Cloud SQL integration.
- Set dashboard environment variables:
  - `AGORA_API_BASE_URL`
  - `AGORA_API_KEY`
- Set API environment variables:
  - `GROQ_API_KEY`
  - DB connection settings
  - `AGORA_API_KEY`

## Test Plan

- Unit-test ledger transitions:
  - clean transaction settles immediately,
  - blocked transaction creates hold,
  - approve release credits receiver,
  - confirm block restores sender balance,
  - escalation keeps hold active.
- API-test idempotency:
  - repeated `POST /transactions` with the same idempotency key returns the same result.
- Integration-test dashboard actions:
  - submit transaction,
  - verify held queue,
  - approve release,
  - confirm balances and audit tables.
- Cloud smoke test:
  - call deployed `POST /transactions`,
  - open deployed dashboard,
  - verify Cloud SQL rows update,
  - verify Secret Manager values are used instead of `.env`.

## Assumptions

- v1 uses simulated sender and receiver ledgers, not real payment rails.
- AGORA is inline: transactions enter AGORA before receiver settlement.
- Cloud Run is the deployment target, not Compute Engine or GKE.
- Cloud SQL PostgreSQL replaces SQLite for deployed runtime state.
- Pub/Sub is not required for v1 because the selected mode is inline gatekeeper; it can be added later for async event fanout.

## References

- Cloud Run container deployment: <https://cloud.google.com/run/docs/deploying>
- Cloud Run to Cloud SQL connection: <https://cloud.google.com/sql/docs/postgres/connect-run>
- Secret Manager: <https://cloud.google.com/secret-manager/docs>

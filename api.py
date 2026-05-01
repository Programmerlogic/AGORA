import json
import os
import sqlite3
import time
from datetime import datetime, timezone
from functools import lru_cache
from typing import Any
from uuid import uuid4

from catboost import CatBoostClassifier
from dotenv import load_dotenv
from fastapi import Depends, FastAPI, Header, HTTPException, Query
from pydantic import BaseModel, ConfigDict, Field

from risk_agent import InvestigationResult, run_investigation

load_dotenv()

DB_PATH = os.getenv("AGORA_DB_PATH", "agora_transactions.db")
MODEL_PATH = os.getenv("AGORA_MODEL_PATH", "agora_fraud_model.cbm")
API_KEY_ENV = "AGORA_API_KEY"
DEFAULT_REVIEWER = "dashboard_analyst"

MODEL_FEATURE_COLUMNS = [
    "step",
    "type",
    "amount",
    "oldbalanceOrg",
    "newbalanceOrig",
    "oldbalanceDest",
    "newbalanceDest",
]


class TransactionIn(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    transaction_id: str | None = None
    idempotency_key: str
    sender_id: str
    receiver_id: str
    txn_type: str = Field(alias="type")
    amount: float
    step: int | None = None
    metadata: dict[str, Any] = Field(default_factory=dict)


class TransactionOut(BaseModel):
    transaction_id: str
    idempotency_key: str
    status: str
    sender_id: str
    receiver_id: str
    amount: float
    txn_type: str
    ml_prediction: int | None = None
    final_verdict: str | None = None
    risk_score: float | None = None
    confidence_score: float | None = None
    reasoning: str | None = None
    event_id: str | None = None
    created_time: str
    updated_time: str


class AnalystDecisionIn(BaseModel):
    action: str
    note: str = ""
    reviewed_by: str = DEFAULT_REVIEWER


class AccountUpsertIn(BaseModel):
    account_id: str
    available_balance: float = 0.0
    held_balance: float = 0.0


class AccountOut(BaseModel):
    account_id: str
    available_balance: float
    held_balance: float
    updated_time: str


app = FastAPI(title="AGORA API", version="0.1.0")


def utc_now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")


def get_conn() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH, timeout=30.0)
    conn.row_factory = sqlite3.Row
    return conn


def _ensure_table_columns(
    conn: sqlite3.Connection, table_name: str, required_columns: dict[str, str]
) -> None:
    existing_columns = {
        str(row[1]).strip()
        for row in conn.execute(f"PRAGMA table_info({table_name})").fetchall()
    }
    for column_name, column_def in required_columns.items():
        if column_name in existing_columns:
            continue
        conn.execute(f"ALTER TABLE {table_name} ADD COLUMN {column_def}")


def init_db() -> None:
    with get_conn() as conn:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS accounts (
                account_id TEXT PRIMARY KEY,
                available_balance REAL NOT NULL DEFAULT 0.0,
                held_balance REAL NOT NULL DEFAULT 0.0,
                created_time TEXT,
                updated_time TEXT
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS transactions_runtime (
                transaction_id TEXT PRIMARY KEY,
                idempotency_key TEXT UNIQUE,
                sender_id TEXT NOT NULL,
                receiver_id TEXT NOT NULL,
                txn_type TEXT NOT NULL,
                amount REAL NOT NULL,
                step INTEGER NOT NULL,
                status TEXT NOT NULL,
                ml_prediction INTEGER,
                final_verdict TEXT,
                risk_score REAL,
                confidence_score REAL,
                reasoning TEXT,
                event_id TEXT,
                hold_amount REAL NOT NULL DEFAULT 0.0,
                metadata_json TEXT,
                created_time TEXT NOT NULL,
                updated_time TEXT NOT NULL
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS ledger_entries (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                entry_time TEXT NOT NULL,
                transaction_id TEXT NOT NULL,
                account_id TEXT NOT NULL,
                entry_type TEXT NOT NULL,
                amount REAL NOT NULL,
                balance_before REAL NOT NULL,
                balance_after REAL NOT NULL,
                note TEXT
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS investigation_events (
                event_id TEXT PRIMARY KEY,
                event_time TEXT,
                stream_index INTEGER,
                step INTEGER,
                nameOrig TEXT,
                nameDest TEXT,
                txn_type TEXT,
                amount REAL,
                ml_prediction INTEGER,
                ground_truth INTEGER,
                final_status TEXT,
                final_verdict TEXT,
                ml_correction INTEGER,
                confidence_score REAL,
                risk_score REAL,
                fallback_used INTEGER,
                reasoning TEXT,
                reason_code TEXT,
                reason_tags_json TEXT,
                control_action TEXT,
                control_status TEXT,
                control_command_json TEXT,
                control_ack_json TEXT,
                approval_status TEXT,
                approval_action TEXT,
                approval_time TEXT,
                approval_note TEXT,
                reviewed_by TEXT,
                release_command_json TEXT,
                release_ack_json TEXT,
                evidence_json TEXT,
                tool_trace_json TEXT,
                agent_latency_s REAL,
                ml_latency_ms REAL,
                total_latency_ms REAL,
                raw_investigation_json TEXT
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS analyst_reviews (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                review_time TEXT,
                event_id TEXT,
                analyst_action TEXT,
                analyst_note TEXT
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS control_commands (
                command_id TEXT PRIMARY KEY,
                event_id TEXT,
                created_time TEXT,
                nameOrig TEXT,
                nameDest TEXT,
                txn_type TEXT,
                amount REAL,
                command TEXT,
                destination TEXT,
                status TEXT,
                payload_json TEXT,
                ack_json TEXT
            )
            """
        )
        # Compatibility with older dashboard-created schema.
        _ensure_table_columns(
            conn,
            "investigation_events",
            {
                "approval_status": "approval_status TEXT",
                "approval_action": "approval_action TEXT",
                "approval_time": "approval_time TEXT",
                "approval_note": "approval_note TEXT",
                "reviewed_by": "reviewed_by TEXT",
                "release_command_json": "release_command_json TEXT",
                "release_ack_json": "release_ack_json TEXT",
            },
        )
        conn.commit()


@lru_cache(maxsize=1)
def get_model() -> CatBoostClassifier:
    model = CatBoostClassifier()
    model.load_model(MODEL_PATH)
    return model


def _safe_json_text(payload: dict | list | str | None) -> str:
    if payload in (None, ""):
        return ""
    if isinstance(payload, str):
        return payload
    try:
        return json.dumps(payload, ensure_ascii=False)
    except Exception:
        return str(payload)


def _row_to_txn(row: sqlite3.Row) -> TransactionOut:
    return TransactionOut(
        transaction_id=str(row["transaction_id"]),
        idempotency_key=str(row["idempotency_key"]),
        status=str(row["status"]),
        sender_id=str(row["sender_id"]),
        receiver_id=str(row["receiver_id"]),
        amount=float(row["amount"]),
        txn_type=str(row["txn_type"]),
        ml_prediction=int(row["ml_prediction"]) if row["ml_prediction"] is not None else None,
        final_verdict=row["final_verdict"],
        risk_score=float(row["risk_score"]) if row["risk_score"] is not None else None,
        confidence_score=float(row["confidence_score"]) if row["confidence_score"] is not None else None,
        reasoning=row["reasoning"],
        event_id=row["event_id"],
        created_time=str(row["created_time"]),
        updated_time=str(row["updated_time"]),
    )


def _persist_control_command(
    conn: sqlite3.Connection,
    *,
    event_id: str,
    sender_id: str,
    receiver_id: str,
    txn_type: str,
    amount: float,
    command: str,
    destination: str,
    status: str,
    reason: str,
    requested_by: str,
    reason_code: str = "",
    risk_score: float | None = None,
    note: str = "",
) -> tuple[dict, dict, str]:
    command_id = f"CMD_{int(time.time() * 1000)}_{event_id}_{command}"
    created_time = utc_now()
    payload = {
        "command": command,
        "command_id": command_id,
        "event_id": event_id,
        "destination": destination,
        "reason": reason,
        "reason_code": reason_code,
        "requested_by": requested_by,
        "transaction": {
            "nameOrig": sender_id,
            "nameDest": receiver_id,
            "type": txn_type,
            "amount": amount,
        },
    }
    if risk_score is not None:
        payload["risk_score"] = risk_score
    if note:
        payload["note"] = note

    ack = {
        "status": "success",
        "command": command,
        "command_id": command_id,
        "event_id": event_id,
        "control_status": status,
        "destination": destination,
        "ack_id": f"ACK_{int(time.time() * 1000)}_{event_id}_{command}",
        "acknowledged_at": created_time,
    }
    conn.execute(
        """
        INSERT OR REPLACE INTO control_commands (
            command_id, event_id, created_time, nameOrig, nameDest, txn_type,
            amount, command, destination, status, payload_json, ack_json
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            command_id,
            event_id,
            created_time,
            sender_id,
            receiver_id,
            txn_type,
            amount,
            command,
            destination,
            status,
            _safe_json_text(payload),
            _safe_json_text(ack),
        ),
    )
    return payload, ack, status


def _write_ledger_entry(
    conn: sqlite3.Connection,
    *,
    transaction_id: str,
    account_id: str,
    entry_type: str,
    amount: float,
    balance_before: float,
    balance_after: float,
    note: str = "",
) -> None:
    conn.execute(
        """
        INSERT INTO ledger_entries (
            entry_time, transaction_id, account_id, entry_type, amount,
            balance_before, balance_after, note
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            utc_now(),
            transaction_id,
            account_id,
            entry_type,
            amount,
            balance_before,
            balance_after,
            note,
        ),
    )


def _get_account(conn: sqlite3.Connection, account_id: str) -> sqlite3.Row:
    row = conn.execute(
        "SELECT * FROM accounts WHERE account_id = ?",
        (account_id,),
    ).fetchone()
    if not row:
        raise HTTPException(status_code=404, detail=f"Account '{account_id}' not found.")
    return row


def _prediction_and_investigation(
    req: TransactionIn,
    sender_available: float,
    receiver_available: float,
) -> tuple[int, InvestigationResult | None, float]:
    step = req.step if req.step is not None else int(time.time())
    features = [
        step,
        req.txn_type,
        float(req.amount),
        float(sender_available),
        float(sender_available - req.amount),
        float(receiver_available),
        float(receiver_available + req.amount),
    ]
    model = get_model()
    start = time.perf_counter()
    prediction = int(model.predict([features])[0])
    ml_latency_ms = (time.perf_counter() - start) * 1000.0

    investigation_output: InvestigationResult | None = None
    if prediction == 1:
        investigation_output = run_investigation(
            user_id=req.sender_id,
            ml_prediction=prediction,
            transaction_context={
                "step": step,
                "type": req.txn_type,
                "amount": float(req.amount),
                "nameDest": req.receiver_id,
            },
            verbose=False,
        )
    return prediction, investigation_output, ml_latency_ms


def _required_api_key() -> str:
    return os.getenv(API_KEY_ENV, "").strip()


def require_api_key(x_agora_api_key: str | None = Header(default=None)) -> None:
    required_key = _required_api_key()
    if not required_key:
        return
    if not x_agora_api_key or x_agora_api_key != required_key:
        raise HTTPException(status_code=401, detail="Invalid or missing X-AGORA-API-Key.")


def _derive_reason_code(investigation_output: InvestigationResult | None) -> str:
    if not investigation_output:
        return "NO_INVESTIGATION"
    verdict = str(investigation_output.final_verdict or "").upper()
    if investigation_output.fallback_used:
        return "FALLBACK_DECISION"
    if verdict == "BLOCK":
        return "RISK_BLOCK"
    if investigation_output.ml_correction:
        return "ML_CORRECTED_ALLOW"
    return "GENERIC_REVIEW"


def _persist_investigation_event(
    conn: sqlite3.Connection,
    *,
    event_id: str,
    req: TransactionIn,
    ml_prediction: int,
    final_status: str,
    investigation_output: InvestigationResult | None,
    control_action: str,
    control_status: str,
    control_payload: dict,
    control_ack: dict,
    total_latency_ms: float,
    ml_latency_ms: float,
) -> None:
    inv = investigation_output
    reason_code = _derive_reason_code(inv)
    reason_tags = []
    if inv:
        if inv.fallback_used:
            reason_tags.append("fallback_mode")
        if inv.final_verdict.upper() == "BLOCK":
            reason_tags.append("block_signal")
        if inv.ml_correction:
            reason_tags.append("ml_corrected")
    conn.execute(
        """
        INSERT OR REPLACE INTO investigation_events (
            event_id, event_time, stream_index, step, nameOrig, nameDest, txn_type,
            amount, ml_prediction, ground_truth, final_status, final_verdict,
            ml_correction, confidence_score, risk_score, fallback_used, reasoning,
            reason_code, reason_tags_json, control_action, control_status,
            control_command_json, control_ack_json, approval_status, approval_action,
            approval_time, approval_note, reviewed_by, release_command_json, release_ack_json,
            evidence_json, tool_trace_json, agent_latency_s, ml_latency_ms, total_latency_ms,
            raw_investigation_json
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            event_id,
            utc_now(),
            req.step if req.step is not None else int(time.time()),
            req.step if req.step is not None else int(time.time()),
            req.sender_id,
            req.receiver_id,
            req.txn_type,
            float(req.amount),
            ml_prediction,
            0,
            final_status,
            inv.final_verdict if inv else "UNKNOWN",
            int(bool(inv.ml_correction)) if inv else 0,
            float(inv.confidence_score) if inv else 0.0,
            float(inv.risk_score) if inv else 0.0,
            int(bool(inv.fallback_used)) if inv else 0,
            inv.reasoning if inv else "No investigation output available.",
            reason_code,
            _safe_json_text({"reason_tags": reason_tags}),
            control_action,
            control_status,
            _safe_json_text(control_payload),
            _safe_json_text(control_ack),
            "pending_approval" if final_status == "HELD_PENDING_APPROVAL" else "not_required",
            "",
            "",
            "",
            "",
            "",
            "",
            _safe_json_text({"evidence": inv.evidence if inv else []}),
            _safe_json_text({"tool_trace": inv.tool_trace if inv else []}),
            float(inv.latency_s) if inv else 0.0,
            round(ml_latency_ms, 3),
            round(total_latency_ms, 3),
            _safe_json_text(inv.to_dict() if inv else {}),
        ),
    )


@app.on_event("startup")
def on_startup() -> None:
    init_db()


@app.get("/health")
def health() -> dict[str, str]:
    return {"status": "ok"}


@app.post("/accounts", response_model=AccountOut, dependencies=[Depends(require_api_key)])
def upsert_account(payload: AccountUpsertIn) -> AccountOut:
    now = utc_now()
    with get_conn() as conn:
        existing = conn.execute(
            "SELECT * FROM accounts WHERE account_id = ?",
            (payload.account_id,),
        ).fetchone()
        if existing:
            conn.execute(
                """
                UPDATE accounts
                SET available_balance = ?, held_balance = ?, updated_time = ?
                WHERE account_id = ?
                """,
                (
                    float(payload.available_balance),
                    float(payload.held_balance),
                    now,
                    payload.account_id,
                ),
            )
        else:
            conn.execute(
                """
                INSERT INTO accounts (account_id, available_balance, held_balance, created_time, updated_time)
                VALUES (?, ?, ?, ?, ?)
                """,
                (
                    payload.account_id,
                    float(payload.available_balance),
                    float(payload.held_balance),
                    now,
                    now,
                ),
            )
        row = conn.execute(
            "SELECT * FROM accounts WHERE account_id = ?",
            (payload.account_id,),
        ).fetchone()
        conn.commit()
    if not row:
        raise HTTPException(status_code=500, detail="Account write failed.")
    return AccountOut(
        account_id=str(row["account_id"]),
        available_balance=float(row["available_balance"]),
        held_balance=float(row["held_balance"]),
        updated_time=str(row["updated_time"] or now),
    )


@app.get("/ledger/accounts", response_model=list[AccountOut], dependencies=[Depends(require_api_key)])
def list_accounts() -> list[AccountOut]:
    with get_conn() as conn:
        rows = conn.execute(
            "SELECT * FROM accounts ORDER BY account_id ASC"
        ).fetchall()
    return [
        AccountOut(
            account_id=str(row["account_id"]),
            available_balance=float(row["available_balance"]),
            held_balance=float(row["held_balance"]),
            updated_time=str(row["updated_time"] or ""),
        )
        for row in rows
    ]


@app.post("/transactions", response_model=TransactionOut, dependencies=[Depends(require_api_key)])
def create_transaction(payload: TransactionIn) -> TransactionOut:
    if payload.amount <= 0:
        raise HTTPException(status_code=422, detail="amount must be greater than zero.")
    if payload.sender_id == payload.receiver_id:
        raise HTTPException(status_code=422, detail="sender_id and receiver_id must be different.")

    now = utc_now()
    transaction_id = payload.transaction_id or f"txn_{uuid4().hex[:20]}"
    step = payload.step if payload.step is not None else int(time.time())

    with get_conn() as conn:
        existing_by_idempotency = conn.execute(
            "SELECT * FROM transactions_runtime WHERE idempotency_key = ?",
            (payload.idempotency_key,),
        ).fetchone()
        if existing_by_idempotency:
            return _row_to_txn(existing_by_idempotency)

        existing_by_txn = conn.execute(
            "SELECT * FROM transactions_runtime WHERE transaction_id = ?",
            (transaction_id,),
        ).fetchone()
        if existing_by_txn:
            raise HTTPException(
                status_code=409,
                detail=f"transaction_id '{transaction_id}' already exists.",
            )

        sender = _get_account(conn, payload.sender_id)
        receiver = _get_account(conn, payload.receiver_id)
        sender_available = float(sender["available_balance"])
        sender_held = float(sender["held_balance"])
        receiver_available = float(receiver["available_balance"])
        amount = float(payload.amount)

        if sender_available < amount:
            raise HTTPException(
                status_code=409,
                detail="insufficient available balance on sender account.",
            )

        ml_prediction, investigation_output, ml_latency_ms = _prediction_and_investigation(
            req=payload,
            sender_available=sender_available,
            receiver_available=receiver_available,
        )

        event_id = f"EVT_{int(time.time() * 1000)}_{transaction_id}"
        final_verdict = "ALLOW"
        risk_score = 5.0
        confidence_score = 0.99
        reasoning = "CatBoost clean classification."
        status = "SETTLED"
        hold_amount = 0.0
        control_action = "none"
        control_status = "not_required"
        control_payload: dict = {}
        control_ack: dict = {}

        if ml_prediction == 1:
            if not investigation_output:
                status = "AGENT_ERROR"
                final_verdict = "UNKNOWN"
                risk_score = 70.0
                confidence_score = 0.0
                reasoning = "Investigation unavailable."
            else:
                final_verdict = investigation_output.final_verdict.upper()
                risk_score = float(investigation_output.risk_score)
                confidence_score = float(investigation_output.confidence_score)
                reasoning = str(investigation_output.reasoning)
                if final_verdict == "BLOCK":
                    status = "HELD_PENDING_APPROVAL"
                    hold_amount = amount
                    control_action = "hold"
                    control_payload, control_ack, control_status = _persist_control_command(
                        conn,
                        event_id=event_id,
                        sender_id=payload.sender_id,
                        receiver_id=payload.receiver_id,
                        txn_type=payload.txn_type,
                        amount=amount,
                        command="hold",
                        destination="manual_review_queue",
                        status="pending_approval",
                        reason="ai_block_pending_analyst_approval",
                        requested_by="agora_agent",
                        reason_code=_derive_reason_code(investigation_output),
                        risk_score=risk_score,
                    )
                else:
                    status = "SETTLED"

        if status == "SETTLED":
            new_sender_available = sender_available - amount
            new_receiver_available = receiver_available + amount
            conn.execute(
                "UPDATE accounts SET available_balance = ?, updated_time = ? WHERE account_id = ?",
                (new_sender_available, now, payload.sender_id),
            )
            conn.execute(
                "UPDATE accounts SET available_balance = ?, updated_time = ? WHERE account_id = ?",
                (new_receiver_available, now, payload.receiver_id),
            )
            _write_ledger_entry(
                conn,
                transaction_id=transaction_id,
                account_id=payload.sender_id,
                entry_type="debit_settle",
                amount=-amount,
                balance_before=sender_available,
                balance_after=new_sender_available,
                note=f"Settled to {payload.receiver_id}",
            )
            _write_ledger_entry(
                conn,
                transaction_id=transaction_id,
                account_id=payload.receiver_id,
                entry_type="credit_settle",
                amount=amount,
                balance_before=receiver_available,
                balance_after=new_receiver_available,
                note=f"Received from {payload.sender_id}",
            )
        elif status == "HELD_PENDING_APPROVAL":
            new_sender_available = sender_available - amount
            new_sender_held = sender_held + amount
            conn.execute(
                """
                UPDATE accounts
                SET available_balance = ?, held_balance = ?, updated_time = ?
                WHERE account_id = ?
                """,
                (new_sender_available, new_sender_held, now, payload.sender_id),
            )
            _write_ledger_entry(
                conn,
                transaction_id=transaction_id,
                account_id=payload.sender_id,
                entry_type="hold_reserve",
                amount=-amount,
                balance_before=sender_available,
                balance_after=new_sender_available,
                note=f"Held pending analyst decision for {payload.receiver_id}",
            )

        agent_latency_s = float(investigation_output.latency_s) if investigation_output else 0.0
        total_latency_ms = ml_latency_ms + (agent_latency_s * 1000.0)
        conn.execute(
            """
            INSERT INTO transactions_runtime (
                transaction_id, idempotency_key, sender_id, receiver_id, txn_type, amount, step,
                status, ml_prediction, final_verdict, risk_score, confidence_score, reasoning,
                event_id, hold_amount, metadata_json, created_time, updated_time
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                transaction_id,
                payload.idempotency_key,
                payload.sender_id,
                payload.receiver_id,
                payload.txn_type,
                amount,
                int(step),
                status,
                ml_prediction,
                final_verdict,
                risk_score,
                confidence_score,
                reasoning,
                event_id,
                hold_amount,
                _safe_json_text(payload.metadata),
                now,
                now,
            ),
        )

        if ml_prediction == 1:
            _persist_investigation_event(
                conn,
                event_id=event_id,
                req=payload,
                ml_prediction=ml_prediction,
                final_status=status,
                investigation_output=investigation_output,
                control_action=control_action,
                control_status=control_status,
                control_payload=control_payload,
                control_ack=control_ack,
                total_latency_ms=total_latency_ms,
                ml_latency_ms=ml_latency_ms,
            )

        row = conn.execute(
            "SELECT * FROM transactions_runtime WHERE transaction_id = ?",
            (transaction_id,),
        ).fetchone()
        conn.commit()

    if not row:
        raise HTTPException(status_code=500, detail="Transaction creation failed.")
    return _row_to_txn(row)


@app.get(
    "/transactions/{transaction_id}",
    response_model=TransactionOut,
    dependencies=[Depends(require_api_key)],
)
def get_transaction(transaction_id: str) -> TransactionOut:
    with get_conn() as conn:
        row = conn.execute(
            "SELECT * FROM transactions_runtime WHERE transaction_id = ?",
            (transaction_id,),
        ).fetchone()
    if not row:
        raise HTTPException(status_code=404, detail="Transaction not found.")
    return _row_to_txn(row)


@app.post(
    "/transactions/{transaction_id}/analyst-decision",
    response_model=TransactionOut,
    dependencies=[Depends(require_api_key)],
)
def analyst_decision(transaction_id: str, payload: AnalystDecisionIn) -> TransactionOut:
    action = str(payload.action or "").strip().upper()
    if action not in {"APPROVE_RELEASE", "CONFIRM_BLOCK", "ESCALATE"}:
        raise HTTPException(
            status_code=422,
            detail="action must be one of APPROVE_RELEASE, CONFIRM_BLOCK, ESCALATE.",
        )
    with get_conn() as conn:
        tx = conn.execute(
            "SELECT * FROM transactions_runtime WHERE transaction_id = ?",
            (transaction_id,),
        ).fetchone()
        if not tx:
            raise HTTPException(status_code=404, detail="Transaction not found.")

        current_status = str(tx["status"])
        if current_status not in {"HELD_PENDING_APPROVAL", "ESCALATED"}:
            raise HTTPException(
                status_code=409,
                detail=f"Transaction is in '{current_status}', not pending analyst decision.",
            )

        sender = _get_account(conn, str(tx["sender_id"]))
        receiver = _get_account(conn, str(tx["receiver_id"]))
        sender_available = float(sender["available_balance"])
        sender_held = float(sender["held_balance"])
        receiver_available = float(receiver["available_balance"])
        amount = float(tx["amount"])
        event_id = str(tx["event_id"] or f"EVT_{transaction_id}")
        now = utc_now()

        next_status = current_status
        approval_status = "pending_approval"
        control_action = "hold"
        control_status = "pending_approval"
        release_payload: dict = {}
        release_ack: dict = {}

        if action == "APPROVE_RELEASE":
            if sender_held < amount:
                raise HTTPException(
                    status_code=409,
                    detail="Held balance is lower than transaction amount; cannot release.",
                )
            new_sender_held = sender_held - amount
            new_receiver_available = receiver_available + amount
            conn.execute(
                """
                UPDATE accounts
                SET held_balance = ?, updated_time = ?
                WHERE account_id = ?
                """,
                (new_sender_held, now, str(tx["sender_id"])),
            )
            conn.execute(
                """
                UPDATE accounts
                SET available_balance = ?, updated_time = ?
                WHERE account_id = ?
                """,
                (new_receiver_available, now, str(tx["receiver_id"])),
            )
            _write_ledger_entry(
                conn,
                transaction_id=transaction_id,
                account_id=str(tx["sender_id"]),
                entry_type="hold_release",
                amount=amount,
                balance_before=sender_held,
                balance_after=new_sender_held,
                note=f"Released to {tx['receiver_id']}",
            )
            _write_ledger_entry(
                conn,
                transaction_id=transaction_id,
                account_id=str(tx["receiver_id"]),
                entry_type="credit_release",
                amount=amount,
                balance_before=receiver_available,
                balance_after=new_receiver_available,
                note=f"Approved incoming from {tx['sender_id']}",
            )
            release_payload, release_ack, control_status = _persist_control_command(
                conn,
                event_id=event_id,
                sender_id=str(tx["sender_id"]),
                receiver_id=str(tx["receiver_id"]),
                txn_type=str(tx["txn_type"]),
                amount=amount,
                command="release",
                destination="payment_processor",
                status="released",
                reason="analyst_approved_release",
                requested_by=str(payload.reviewed_by or DEFAULT_REVIEWER),
                risk_score=float(tx["risk_score"] or 0.0),
                note=str(payload.note or ""),
            )
            next_status = "RELEASED_AFTER_APPROVAL"
            approval_status = "released"
            control_action = "release"
        elif action == "CONFIRM_BLOCK":
            if sender_held < amount:
                raise HTTPException(
                    status_code=409,
                    detail="Held balance is lower than transaction amount; cannot confirm block.",
                )
            new_sender_held = sender_held - amount
            new_sender_available = sender_available + amount
            conn.execute(
                """
                UPDATE accounts
                SET available_balance = ?, held_balance = ?, updated_time = ?
                WHERE account_id = ?
                """,
                (new_sender_available, new_sender_held, now, str(tx["sender_id"])),
            )
            _write_ledger_entry(
                conn,
                transaction_id=transaction_id,
                account_id=str(tx["sender_id"]),
                entry_type="hold_cancel",
                amount=amount,
                balance_before=sender_held,
                balance_after=new_sender_held,
                note="Blocked and returned to available balance.",
            )
            release_payload, release_ack, control_status = _persist_control_command(
                conn,
                event_id=event_id,
                sender_id=str(tx["sender_id"]),
                receiver_id=str(tx["receiver_id"]),
                txn_type=str(tx["txn_type"]),
                amount=amount,
                command="confirm_block",
                destination="blocked_ledger",
                status="blocked_confirmed",
                reason="analyst_confirmed_ai_block",
                requested_by=str(payload.reviewed_by or DEFAULT_REVIEWER),
                risk_score=float(tx["risk_score"] or 0.0),
                note=str(payload.note or ""),
            )
            next_status = "BLOCKED_CONFIRMED"
            approval_status = "blocked_confirmed"
            control_action = "confirm_block"
        else:
            next_status = "ESCALATED"
            approval_status = "escalated"
            control_action = "hold"
            control_status = "escalated"

        conn.execute(
            """
            UPDATE transactions_runtime
            SET status = ?, updated_time = ?
            WHERE transaction_id = ?
            """,
            (next_status, now, transaction_id),
        )
        conn.execute(
            """
            INSERT INTO analyst_reviews (review_time, event_id, analyst_action, analyst_note)
            VALUES (?, ?, ?, ?)
            """,
            (
                now,
                event_id,
                action,
                str(payload.note or ""),
            ),
        )
        conn.execute(
            """
            UPDATE investigation_events
            SET final_status = ?,
                approval_status = ?,
                approval_action = ?,
                approval_time = ?,
                approval_note = ?,
                reviewed_by = ?,
                control_action = ?,
                control_status = ?,
                release_command_json = ?,
                release_ack_json = ?
            WHERE event_id = ?
            """,
            (
                next_status,
                approval_status,
                action,
                now,
                str(payload.note or ""),
                str(payload.reviewed_by or DEFAULT_REVIEWER),
                control_action,
                control_status,
                _safe_json_text(release_payload),
                _safe_json_text(release_ack),
                event_id,
            ),
        )

        row = conn.execute(
            "SELECT * FROM transactions_runtime WHERE transaction_id = ?",
            (transaction_id,),
        ).fetchone()
        conn.commit()

    if not row:
        raise HTTPException(status_code=500, detail="Unable to fetch updated transaction.")
    return _row_to_txn(row)


@app.get("/events", dependencies=[Depends(require_api_key)])
def list_events(limit: int = Query(default=50, ge=1, le=500)) -> list[dict[str, Any]]:
    with get_conn() as conn:
        rows = conn.execute(
            """
            SELECT event_id, event_time, nameOrig, nameDest, txn_type, amount,
                   final_status, final_verdict, approval_status, approval_action,
                   control_action, control_status, risk_score, confidence_score, reasoning
            FROM investigation_events
            ORDER BY event_time DESC
            LIMIT ?
            """,
            (int(limit),),
        ).fetchall()
    return [{key: row[key] for key in row.keys()} for row in rows]

import time
import sqlite3
import pandas as pd
from typing import Optional, Dict, Any, List
from fastapi import FastAPI, HTTPException, BackgroundTasks
from pydantic import BaseModel, Field
from catboost import CatBoostClassifier

# Import existing helpers from dashboard and risk_agent
from dashboard import (
    DB_PATH,
    get_db_connection,
    MODEL_FEATURE_COLUMNS,
    classify_outcome,
    _derive_reason_metadata,
    issue_hold_command,
    persist_investigation_event,
    update_investigation_approval,
    _safe_json_text,
    _transaction_payload
)

from risk_agent import run_investigation

app = FastAPI(title="AGORA API", description="Payload-style transaction ingestion for AGORA")

# Global ML Model
model = CatBoostClassifier()
model.load_model("agora_fraud_model.cbm")

# --- Pydantic Models ---

class TransactionPayload(BaseModel):
    step: int
    type: str
    amount: float
    nameOrig: str
    oldbalanceOrg: float
    newbalanceOrig: float
    nameDest: str
    oldbalanceDest: float
    newbalanceDest: float
    isFraud: Optional[int] = 0

class AnalystDecisionPayload(BaseModel):
    action: str = Field(..., description="APPROVE_RELEASE, CONFIRM_BLOCK, ESCALATE")
    note: str = ""
    reviewer: str = "api_user"

class AccountPayload(BaseModel):
    account_id: str
    balance: float
    owner_name: Optional[str] = ""

# --- DB Initialization for API-specific tables ---

def init_api_tables():
    with get_db_connection() as conn:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS accounts (
                account_id TEXT PRIMARY KEY,
                balance REAL,
                owner_name TEXT,
                updated_at TEXT
            )
            """
        )
        conn.commit()

init_api_tables()

# --- Helper functions for DB ---

def update_account_balance(account_id: str, balance: float, owner_name: str = ""):
    with get_db_connection() as conn:
        conn.execute(
            """
            INSERT INTO accounts (account_id, balance, owner_name, updated_at)
            VALUES (?, ?, ?, ?)
            ON CONFLICT(account_id) DO UPDATE SET 
                balance=excluded.balance, 
                updated_at=excluded.updated_at
            """,
            (account_id, balance, owner_name, pd.Timestamp.now().strftime("%Y-%m-%d %H:%M:%S"))
        )
        conn.commit()

def fetch_transaction_event(event_id: str) -> Optional[Dict[str, Any]]:
    with get_db_connection() as conn:
        conn.row_factory = sqlite3.Row
        row = conn.execute("SELECT * FROM investigation_events WHERE event_id = ?", (event_id,)).fetchone()
        return dict(row) if row else None

# --- Endpoints ---

@app.post("/transactions")
def ingest_transaction(payload: TransactionPayload, background_tasks: BackgroundTasks):
    """
    Ingest a transaction, perform ML scoring, and run agent investigation if flagged.
    """
    # 1. Update accounts based on payload (optional ledger simulation)
    update_account_balance(payload.nameOrig, payload.newbalanceOrig)
    update_account_balance(payload.nameDest, payload.newbalanceDest)

    # 2. Extract features for ML
    sample_dict = payload.dict()
    features = [sample_dict.get(col, 0) for col in MODEL_FEATURE_COLUMNS]

    ml_start = time.perf_counter()
    prediction = int(model.predict([features])[0])
    ml_latency_ms = (time.perf_counter() - ml_start) * 1000
    agent_latency_s = 0.0
    investigation_output = None

    # 3. Agentic Investigation if ML flags as fraud
    if prediction == 1:
        investigation_output = run_investigation(
            payload.nameOrig,
            prediction,
            transaction_context={
                "step": payload.step,
                "type": payload.type,
                "amount": payload.amount,
                "nameDest": payload.nameDest,
            },
        )
        agent_latency_s = float(investigation_output.latency_s)

    # 4. Classify outcome
    final_status, anomaly_label, is_ml_anomaly, is_agent_blocked = classify_outcome(
        prediction, investigation_output
    )

    total_latency_ms = ml_latency_ms + (agent_latency_s * 1000.0)
    timestamp = pd.Timestamp.now().strftime("%Y-%m-%d %H:%M:%S")

    response_data = {
        "status": "success",
        "prediction": prediction,
        "final_status": final_status,
        "latency_ms": round(total_latency_ms, 2)
    }

    # 5. Persist event to DB so it appears in the dashboard
    reason_code = "CLEAN"
    reason_tags = []
    event_id = f"EVT_{int(time.time() * 1000)}_{payload.step}_{payload.nameOrig}".replace(" ", "_")
    sample_series = pd.Series(sample_dict)
    
    control_payload = {}
    control_ack = {}
    control_status = "not_required"
    is_held_for_approval = final_status == "HELD_PENDING_APPROVAL"
    
    if investigation_output:
        reason_code, reason_tags = _derive_reason_metadata(investigation_output)
        if is_held_for_approval:
            control_payload, control_ack, control_status = issue_hold_command(
                event_id=event_id,
                sample=sample_series,
                investigation_output=investigation_output,
                reason_code=reason_code,
            )
            
    investigation_event = {
        "event_id": event_id,
        "event_time": timestamp,
        "stream_index": payload.step,
        "step": payload.step,
        "nameOrig": payload.nameOrig,
        "nameDest": payload.nameDest,
        "txn_type": payload.type,
        "amount": payload.amount,
        "ml_prediction": prediction,
        "ground_truth": payload.isFraud,
        "final_status": final_status,
        "final_verdict": investigation_output.final_verdict if investigation_output else "Clean",
        "ml_correction": investigation_output.ml_correction if investigation_output else 0,
        "confidence_score": investigation_output.confidence_score if investigation_output else 1.0,
        "risk_score": investigation_output.risk_score if investigation_output else 0.0,
        "fallback_used": investigation_output.fallback_used if investigation_output else 0,
        "reasoning": investigation_output.reasoning if investigation_output else "No anomaly detected by ML.",
        "reason_code": reason_code,
        "reason_tags_json": _safe_json_text({"reason_tags": reason_tags}),
        "control_action": ("hold" if is_held_for_approval else investigation_output.control_action) if investigation_output else "allow",
        "control_status": control_status,
        "control_command_json": _safe_json_text(control_payload),
        "control_ack_json": _safe_json_text(control_ack),
        "approval_status": "pending_approval" if is_held_for_approval else "not_required",
        "approval_action": "",
        "approval_time": "",
        "approval_note": "",
        "reviewed_by": "",
        "release_command_json": "",
        "release_ack_json": "",
        "evidence_json": _safe_json_text({"evidence": investigation_output.evidence if investigation_output else []}),
        "tool_trace_json": _safe_json_text({"tool_trace": investigation_output.tool_trace if investigation_output else []}),
        "agent_latency_s": round(agent_latency_s, 4),
        "ml_latency_ms": round(ml_latency_ms, 3),
        "total_latency_ms": round(total_latency_ms, 3),
        "raw_investigation_json": _safe_json_text(investigation_output.to_dict() if investigation_output else {}),
    }
    
    persist_investigation_event(investigation_event)
    response_data["event_id"] = event_id
    if investigation_output:
        response_data["investigation"] = investigation_output.to_dict()

    return response_data

@app.post("/transactions/{transaction_id}/analyst-decision")
def analyst_decision(transaction_id: str, payload: AnalystDecisionPayload):
    """
    Handle analyst review actions (APPROVE_RELEASE, CONFIRM_BLOCK, ESCALATE)
    """
    event = fetch_transaction_event(transaction_id)
    if not event:
        raise HTTPException(status_code=404, detail="Transaction event not found")

    # Re-use logic from dashboard.py apply_analyst_approval
    from dashboard import apply_analyst_approval
    # apply_analyst_approval expects a pd.Series, let's wrap it
    row_series = pd.Series(event)
    
    # Temporarily override reviewer if we wanted, but dashboard logic uses DEFAULT_REVIEWER hardcoded inside apply_analyst_approval
    # For now we just call it
    try:
        msg = apply_analyst_approval(row_series, payload.action, payload.note)
        return {"status": "success", "message": msg, "action_applied": payload.action}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/accounts")
def create_or_update_account(payload: AccountPayload):
    update_account_balance(payload.account_id, payload.balance, payload.owner_name)
    return {"status": "success", "account_id": payload.account_id}

@app.get("/ledger/accounts")
def get_accounts():
    with get_db_connection() as conn:
        conn.row_factory = sqlite3.Row
        rows = conn.execute("SELECT * FROM accounts").fetchall()
        return [dict(row) for row in rows]

@app.get("/transactions/{transaction_id}")
def get_transaction(transaction_id: str):
    event = fetch_transaction_event(transaction_id)
    if not event:
        raise HTTPException(status_code=404, detail="Transaction not found")
    return event

@app.get("/events")
def get_events(limit: int = 50):
    with get_db_connection() as conn:
        conn.row_factory = sqlite3.Row
        rows = conn.execute("SELECT * FROM investigation_events ORDER BY event_time DESC LIMIT ?", (limit,)).fetchall()
        return [dict(row) for row in rows]

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="127.0.0.1", port=8000)

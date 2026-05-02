# import json
# import html
# import sqlite3
# import time

# import pandas as pd
# import plotly.express as px
# import plotly.graph_objects as go
# import streamlit as st
# from catboost import CatBoostClassifier

# from db_chat import answer_db_question, get_suggested_questions
# from risk_agent import InvestigationResult, run_investigation

# STREAM_SLEEP_SECONDS = 0.75
# MAX_LOG_ROWS = 200
# MAX_CHART_ROWS = 500
# MAX_INVESTIGATION_EVENT_ROWS = 2000
# LIVE_STREAM_FILE = "X_test.csv"
# DB_PATH = "agora_transactions.db"
# FRESH_BOOT_CLEARS_AUDIT = True
# MODEL_FEATURE_COLUMNS = [
#     "step",
#     "type",
#     "amount",
#     "oldbalanceOrg",
#     "newbalanceOrig",
#     "oldbalanceDest",
#     "newbalanceDest",
# ]
# DEFAULT_REVIEWER = "dashboard_analyst"


# @st.cache_resource
# def load_assets() -> tuple[CatBoostClassifier, pd.DataFrame]:
#     model = CatBoostClassifier()
#     model.load_model("agora_fraud_model.cbm")
#     df = pd.read_csv(LIVE_STREAM_FILE, nrows=1000)

#     if "nameOrig" not in df.columns:
#         df["nameOrig"] = [f"XUSER_{i:06d}" for i in range(len(df))]
#     if "nameDest" not in df.columns:
#         df["nameDest"] = [f"XDEST_{i:06d}" for i in range(len(df))]
#     if "isFraud" not in df.columns:
#         df["isFraud"] = 0
#     if "isFlaggedFraud" not in df.columns:
#         df["isFlaggedFraud"] = 0

#     return model, df


# def init_state() -> None:
#     defaults = {
#         "stream_running": False,
#         "stream_index": 0,
#         "log_history": [],
#         "chart_history": [],
#         "latest_event_id": "",
#         "ml_anomaly_count": 0,
#         "agent_block_count": 0,
#         "override_count": 0,
#         "last_ml_latency_ms": 0.0,
#         "last_agent_latency_s": 0.0,
#         "last_total_latency_ms": 0.0,
#         "chat_history": [],
#         "investigation_event_log": [],
#         "stream_done_message": "",
#     }
#     for key, value in defaults.items():
#         if key not in st.session_state:
#             st.session_state[key] = value


# def reset_stream_state() -> None:
#     st.session_state.stream_running = False
#     st.session_state.stream_index = 0
#     st.session_state.log_history = []
#     st.session_state.chart_history = []
#     st.session_state.latest_event_id = ""
#     st.session_state.ml_anomaly_count = 0
#     st.session_state.agent_block_count = 0
#     st.session_state.override_count = 0
#     st.session_state.last_ml_latency_ms = 0.0
#     st.session_state.last_agent_latency_s = 0.0
#     st.session_state.last_total_latency_ms = 0.0
#     st.session_state.investigation_event_log = []
#     st.session_state.stream_done_message = ""


# def refresh_application_state(clear_audit: bool = True) -> None:
#     if clear_audit:
#         clear_audit_tables()
#     reset_stream_state()
#     st.session_state.chat_history = []
#     st.session_state.db_chat_suggestion = ""
#     st.session_state.pop("selected_investigation_event_id", None)
#     st.session_state.pop("analyst_selected_event_id", None)
#     st.session_state.pop("analyst_action_inline", None)
#     st.session_state.pop("analyst_note_inline", None)
#     st.session_state["active_view"] = "Live Monitoring"


# def init_audit_tables() -> None:
#     with sqlite3.connect(DB_PATH) as conn:
#         conn.execute(
#             """
#             CREATE TABLE IF NOT EXISTS investigation_events (
#                 event_id TEXT PRIMARY KEY,
#                 event_time TEXT,
#                 stream_index INTEGER,
#                 step INTEGER,
#                 nameOrig TEXT,
#                 nameDest TEXT,
#                 txn_type TEXT,
#                 amount REAL,
#                 ml_prediction INTEGER,
#                 ground_truth INTEGER,
#                 final_status TEXT,
#                 final_verdict TEXT,
#                 ml_correction INTEGER,
#                 confidence_score REAL,
#                 risk_score REAL,
#                 fallback_used INTEGER,
#                 reasoning TEXT,
#                 evidence_json TEXT,
#                 tool_trace_json TEXT,
#                 agent_latency_s REAL,
#                 ml_latency_ms REAL,
#                 total_latency_ms REAL,
#                 raw_investigation_json TEXT
#             )
#             """
#         )
#         conn.execute(
#             """
#             CREATE TABLE IF NOT EXISTS analyst_reviews (
#                 id INTEGER PRIMARY KEY AUTOINCREMENT,
#                 review_time TEXT,
#                 event_id TEXT,
#                 analyst_action TEXT,
#                 analyst_note TEXT
#             )
#             """
#         )
#         conn.execute(
#             """
#             CREATE TABLE IF NOT EXISTS control_commands (
#                 command_id TEXT PRIMARY KEY,
#                 event_id TEXT,
#                 created_time TEXT,
#                 nameOrig TEXT,
#                 nameDest TEXT,
#                 txn_type TEXT,
#                 amount REAL,
#                 command TEXT,
#                 destination TEXT,
#                 status TEXT,
#                 payload_json TEXT,
#                 ack_json TEXT
#             )
#             """
#         )
#         # Migration-safe schema extension for structured reason storage.
#         _ensure_table_columns(
#             conn,
#             "investigation_events",
#             {
#                 "reason_code": "reason_code TEXT",
#                 "reason_tags_json": "reason_tags_json TEXT",
#                 "control_action": "control_action TEXT",
#                 "control_status": "control_status TEXT",
#                 "control_command_json": "control_command_json TEXT",
#                 "control_ack_json": "control_ack_json TEXT",
#                 "approval_status": "approval_status TEXT",
#                 "approval_action": "approval_action TEXT",
#                 "approval_time": "approval_time TEXT",
#                 "approval_note": "approval_note TEXT",
#                 "reviewed_by": "reviewed_by TEXT",
#                 "release_command_json": "release_command_json TEXT",
#                 "release_ack_json": "release_ack_json TEXT",
#             },
#         )
#         conn.commit()


# def _ensure_table_columns(
#     conn: sqlite3.Connection, table_name: str, required_columns: dict[str, str]
# ) -> None:
#     existing_columns = {
#         str(row[1]).strip()
#         for row in conn.execute(f"PRAGMA table_info({table_name})").fetchall()
#     }
#     for column_name, column_def in required_columns.items():
#         if column_name in existing_columns:
#             continue
#         conn.execute(f"ALTER TABLE {table_name} ADD COLUMN {column_def}")


# def clear_audit_tables() -> None:
#     with sqlite3.connect(DB_PATH) as conn:
#         conn.execute("DELETE FROM analyst_reviews")
#         conn.execute("DELETE FROM control_commands")
#         conn.execute("DELETE FROM investigation_events")
#         conn.commit()


# def hard_reset_application(clear_audit: bool = True) -> None:
#     if clear_audit:
#         clear_audit_tables()
#     load_assets.clear()
#     st.session_state.clear()


# def ensure_fresh_boot_state() -> None:
#     if st.session_state.get("_fresh_boot_initialized"):
#         return
#     if FRESH_BOOT_CLEARS_AUDIT:
#         clear_audit_tables()
#     st.session_state["_fresh_boot_initialized"] = True


# def persist_investigation_event(event: dict) -> None:
#     columns = [
#         "event_id",
#         "event_time",
#         "stream_index",
#         "step",
#         "nameOrig",
#         "nameDest",
#         "txn_type",
#         "amount",
#         "ml_prediction",
#         "ground_truth",
#         "final_status",
#         "final_verdict",
#         "ml_correction",
#         "confidence_score",
#         "risk_score",
#         "fallback_used",
#         "reasoning",
#         "reason_code",
#         "reason_tags_json",
#         "control_action",
#         "control_status",
#         "control_command_json",
#         "control_ack_json",
#         "approval_status",
#         "approval_action",
#         "approval_time",
#         "approval_note",
#         "reviewed_by",
#         "release_command_json",
#         "release_ack_json",
#         "evidence_json",
#         "tool_trace_json",
#         "agent_latency_s",
#         "ml_latency_ms",
#         "total_latency_ms",
#         "raw_investigation_json",
#     ]
#     values = []
#     for column in columns:
#         value = event.get(column)
#         if column in {"ml_correction", "fallback_used"}:
#             value = int(bool(value))
#         values.append(value)

#     with sqlite3.connect(DB_PATH) as conn:
#         conn.execute(
#             f"""
#             INSERT OR REPLACE INTO investigation_events (
#                 {", ".join(columns)}
#             ) VALUES ({", ".join(["?"] * len(columns))})
#             """,
#             tuple(values),
#         )
#         conn.commit()


# def persist_analyst_review(event_id: str, analyst_action: str, analyst_note: str) -> None:
#     with sqlite3.connect(DB_PATH) as conn:
#         conn.execute(
#             """
#             INSERT INTO analyst_reviews (review_time, event_id, analyst_action, analyst_note)
#             VALUES (?, ?, ?, ?)
#             """,
#             (
#                 pd.Timestamp.now().strftime("%Y-%m-%d %H:%M:%S"),
#                 event_id,
#                 analyst_action,
#                 analyst_note,
#             ),
#         )
#         conn.commit()


# def persist_control_command(command_row: dict) -> None:
#     with sqlite3.connect(DB_PATH) as conn:
#         conn.execute(
#             """
#             INSERT OR REPLACE INTO control_commands (
#                 command_id, event_id, created_time, nameOrig, nameDest, txn_type,
#                 amount, command, destination, status, payload_json, ack_json
#             ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
#             """,
#             (
#                 command_row.get("command_id"),
#                 command_row.get("event_id"),
#                 command_row.get("created_time"),
#                 command_row.get("nameOrig"),
#                 command_row.get("nameDest"),
#                 command_row.get("txn_type"),
#                 command_row.get("amount"),
#                 command_row.get("command"),
#                 command_row.get("destination"),
#                 command_row.get("status"),
#                 command_row.get("payload_json"),
#                 command_row.get("ack_json"),
#             ),
#         )
#         conn.commit()


# def fetch_recent_audit_events(limit: int = 300) -> pd.DataFrame:
#     with sqlite3.connect(DB_PATH) as conn:
#         query = f"""
#         SELECT * FROM investigation_events
#         ORDER BY event_time DESC
#         LIMIT {int(limit)}
#         """
#         return pd.read_sql_query(query, conn)


# def fetch_recent_reviews(limit: int = 40) -> pd.DataFrame:
#     with sqlite3.connect(DB_PATH) as conn:
#         query = f"""
#         SELECT * FROM analyst_reviews
#         ORDER BY review_time DESC
#         LIMIT {int(limit)}
#         """
#         return pd.read_sql_query(query, conn)


# def classify_outcome(
#     prediction: int, investigation_output: InvestigationResult | None
# ) -> tuple[str, str, bool, bool]:
#     if prediction == 0:
#         return "ALLOW", "Clean", False, False

#     if not investigation_output:
#         return "AGENT_ERROR", "ML Fraud", True, False

#     verdict = investigation_output.final_verdict.upper()
#     if verdict == "BLOCK":
#         return "HELD_PENDING_APPROVAL", "Held Pending Approval", True, True
#     return "ML_OVERRIDDEN_ALLOW", "ML Fraud", True, False


# def _safe_json_text(payload: dict | None) -> str:
#     if not payload:
#         return ""
#     try:
#         return json.dumps(payload, ensure_ascii=False)
#     except Exception:
#         return str(payload)


# def _transaction_payload(
#     step: object,
#     name_orig: object,
#     name_dest: object,
#     txn_type: object,
#     amount: object,
# ) -> dict:
#     return {
#         "step": int(step),
#         "nameOrig": str(name_orig),
#         "nameDest": str(name_dest),
#         "type": str(txn_type),
#         "amount": float(amount),
#     }


# def _transaction_payload_from_sample(sample: pd.Series) -> dict:
#     return _transaction_payload(
#         step=sample["step"],
#         name_orig=sample["nameOrig"],
#         name_dest=sample["nameDest"],
#         txn_type=sample["type"],
#         amount=sample["amount"],
#     )


# def _transaction_payload_from_event(row: pd.Series) -> dict:
#     return _transaction_payload(
#         step=row.get("step", 0),
#         name_orig=row.get("nameOrig", ""),
#         name_dest=row.get("nameDest", ""),
#         txn_type=row.get("txn_type", ""),
#         amount=row.get("amount", 0.0),
#     )


# def issue_control_command(
#     event_id: str,
#     transaction: dict,
#     command: str,
#     destination: str,
#     status: str,
#     reason: str,
#     requested_by: str,
#     reason_code: str = "",
#     risk_score: float | None = None,
#     note: str = "",
# ) -> tuple[dict, dict, str]:
#     command_id = f"CMD_{int(time.time() * 1000)}_{event_id}_{command}"
#     created_time = pd.Timestamp.now().strftime("%Y-%m-%d %H:%M:%S")
#     payload = {
#         "command": command,
#         "command_id": command_id,
#         "event_id": event_id,
#         "destination": destination,
#         "reason": reason,
#         "reason_code": reason_code,
#         "requested_by": requested_by,
#         "transaction": transaction,
#     }
#     if risk_score is not None:
#         payload["risk_score"] = risk_score
#     if note:
#         payload["note"] = note

#     ack = {
#         "status": "success",
#         "command": command,
#         "command_id": command_id,
#         "event_id": event_id,
#         "control_status": status,
#         "destination": destination,
#         "ack_id": f"ACK_{int(time.time() * 1000)}_{event_id}_{command}",
#         "acknowledged_at": created_time,
#     }
#     persist_control_command(
#         {
#             "command_id": command_id,
#             "event_id": event_id,
#             "created_time": created_time,
#             "nameOrig": transaction["nameOrig"],
#             "nameDest": transaction["nameDest"],
#             "txn_type": transaction["type"],
#             "amount": transaction["amount"],
#             "command": command,
#             "destination": destination,
#             "status": status,
#             "payload_json": _safe_json_text(payload),
#             "ack_json": _safe_json_text(ack),
#         }
#     )
#     return payload, ack, status


# def issue_hold_command(
#     event_id: str,
#     sample: pd.Series,
#     investigation_output: InvestigationResult,
#     reason_code: str,
# ) -> tuple[dict, dict, str]:
#     return issue_control_command(
#         event_id=event_id,
#         transaction=_transaction_payload_from_sample(sample),
#         command="hold",
#         destination=investigation_output.control_destination or "manual_review_queue",
#         status="pending_approval",
#         reason="ai_block_pending_analyst_approval",
#         requested_by="agora_agent",
#         reason_code=reason_code,
#         risk_score=investigation_output.risk_score,
#     )


# def update_investigation_approval(
#     event_id: str,
#     final_status: str,
#     approval_status: str,
#     approval_action: str,
#     approval_note: str,
#     reviewed_by: str,
#     control_action: str,
#     control_status: str,
#     release_payload: dict | None = None,
#     release_ack: dict | None = None,
# ) -> None:
#     approval_time = pd.Timestamp.now().strftime("%Y-%m-%d %H:%M:%S")
#     with sqlite3.connect(DB_PATH) as conn:
#         conn.execute(
#             """
#             UPDATE investigation_events
#             SET final_status = ?,
#                 approval_status = ?,
#                 approval_action = ?,
#                 approval_time = ?,
#                 approval_note = ?,
#                 reviewed_by = ?,
#                 control_action = ?,
#                 control_status = ?,
#                 release_command_json = ?,
#                 release_ack_json = ?
#             WHERE event_id = ?
#             """,
#             (
#                 final_status,
#                 approval_status,
#                 approval_action,
#                 approval_time,
#                 approval_note,
#                 reviewed_by,
#                 control_action,
#                 control_status,
#                 _safe_json_text(release_payload),
#                 _safe_json_text(release_ack),
#                 event_id,
#             ),
#         )
#         conn.commit()


# def apply_analyst_approval(row: pd.Series, analyst_action: str, analyst_note: str) -> str:
#     event_id = str(row.get("event_id") or "")
#     transaction = _transaction_payload_from_event(row)
#     note = str(analyst_note or "")

#     if analyst_action == "APPROVE_RELEASE":
#         release_payload, release_ack, control_status = issue_control_command(
#             event_id=event_id,
#             transaction=transaction,
#             command="release",
#             destination="payment_processor",
#             status="released",
#             reason="analyst_approved_release",
#             requested_by=DEFAULT_REVIEWER,
#             risk_score=float(row.get("risk_score") or 0.0),
#             note=note,
#         )
#         update_investigation_approval(
#             event_id=event_id,
#             final_status="RELEASED_AFTER_APPROVAL",
#             approval_status="released",
#             approval_action=analyst_action,
#             approval_note=note,
#             reviewed_by=DEFAULT_REVIEWER,
#             control_action="release",
#             control_status=control_status,
#             release_payload=release_payload,
#             release_ack=release_ack,
#         )
#         return "Transaction released after analyst approval."

#     if analyst_action == "CONFIRM_BLOCK":
#         _, _, control_status = issue_control_command(
#             event_id=event_id,
#             transaction=transaction,
#             command="confirm_block",
#             destination="blocked_ledger",
#             status="blocked_confirmed",
#             reason="analyst_confirmed_ai_block",
#             requested_by=DEFAULT_REVIEWER,
#             risk_score=float(row.get("risk_score") or 0.0),
#             note=note,
#         )
#         update_investigation_approval(
#             event_id=event_id,
#             final_status="BLOCKED_CONFIRMED",
#             approval_status="blocked_confirmed",
#             approval_action=analyst_action,
#             approval_note=note,
#             reviewed_by=DEFAULT_REVIEWER,
#             control_action="confirm_block",
#             control_status=control_status,
#         )
#         return "Block confirmed by analyst."

#     update_investigation_approval(
#         event_id=event_id,
#         final_status="ESCALATED",
#         approval_status="escalated",
#         approval_action="ESCALATE",
#         approval_note=note,
#         reviewed_by=DEFAULT_REVIEWER,
#         control_action=str(row.get("control_action") or "hold"),
#         control_status="escalated",
#     )
#     return "Transaction escalated for further review."


# def _derive_reason_metadata(investigation_output: InvestigationResult) -> tuple[str, list[str]]:
#     evidence_text = " ".join([str(item) for item in investigation_output.evidence]).lower()
#     reasoning_text = str(investigation_output.reasoning or "").lower()

#     tags: list[str] = []
#     if investigation_output.fallback_used:
#         tags.append("fallback_mode")
#     if "liquidation" in evidence_text or "cash_out" in evidence_text:
#         tags.append("liquidation_pattern")
#     if "repetitive" in evidence_text or "low-variance" in evidence_text or "false positive" in evidence_text:
#         tags.append("repetitive_payment_pattern")
#     if "new account" in evidence_text or "no user history" in evidence_text:
#         tags.append("insufficient_history")
#     if "similar" in evidence_text:
#         tags.append("similar_txn_context")

#     reason_code = "GENERIC_REVIEW"
#     if "liquidation_pattern" in tags and investigation_output.final_verdict.upper() == "BLOCK":
#         reason_code = "LIQUIDATION_BLOCK"
#     elif "repetitive_payment_pattern" in tags and investigation_output.final_verdict.upper() == "ALLOW":
#         reason_code = "REPETITIVE_ALLOW"
#     elif investigation_output.fallback_used:
#         reason_code = "FALLBACK_DECISION"
#     elif investigation_output.final_verdict.upper() == "BLOCK":
#         reason_code = "RISK_BLOCK"
#     elif investigation_output.ml_correction:
#         reason_code = "ML_CORRECTED_ALLOW"

#     if not tags and reasoning_text:
#         if "liquidation" in reasoning_text:
#             tags.append("liquidation_pattern")
#         elif "false positive" in reasoning_text or "repetitive" in reasoning_text:
#             tags.append("repetitive_payment_pattern")
#         elif "fallback" in reasoning_text:
#             tags.append("fallback_mode")

#     return reason_code, tags


# def process_next_transaction(model: CatBoostClassifier, raw_data: pd.DataFrame) -> None:
#     idx = st.session_state.stream_index
#     if idx >= len(raw_data):
#         st.session_state.stream_running = False
#         st.session_state.stream_done_message = "Stream completed all available rows."
#         return

#     sample = raw_data.iloc[idx]
#     features = sample[MODEL_FEATURE_COLUMNS].values

#     ml_start = time.perf_counter()
#     prediction = int(model.predict([features])[0])
#     ml_latency_ms = (time.perf_counter() - ml_start) * 1000
#     agent_latency_s = 0.0
#     investigation_output: InvestigationResult | None = None

#     if prediction == 1:
#         investigation_output = run_investigation(
#             str(sample["nameOrig"]),
#             prediction,
#             transaction_context={
#                 "step": int(sample["step"]),
#                 "type": str(sample["type"]),
#                 "amount": float(sample["amount"]),
#                 "nameDest": str(sample["nameDest"]),
#             },
#         )
#         agent_latency_s = float(investigation_output.latency_s)

#     final_status, anomaly_label, is_ml_anomaly, is_agent_blocked = classify_outcome(
#         prediction, investigation_output
#     )
#     ground_truth = int(sample.get("isFraud", 0))

#     timestamp = pd.Timestamp.now().strftime("%Y-%m-%d %H:%M:%S")
#     log_entry = {
#         "Time": timestamp,
#         "Step": int(sample["step"]),
#         "Type": str(sample["type"]),
#         "User": str(sample["nameOrig"]),
#         "Amount": float(sample["amount"]),
#         "CatBoost": "Fraud" if prediction == 1 else "Clean",
#         "Final Status": final_status,
#     }
#     st.session_state.log_history.insert(0, log_entry)
#     st.session_state.log_history = st.session_state.log_history[:MAX_LOG_ROWS]

#     chart_entry = {
#         "time": timestamp,
#         "step": int(sample["step"]),
#         "type": str(sample["type"]),
#         "anomaly_label": anomaly_label,
#         "is_ml_anomaly": is_ml_anomaly,
#         "is_agent_blocked": is_agent_blocked,
#         "risk_score": (
#             float(investigation_output.risk_score) if investigation_output else 0.0
#         ),
#     }
#     st.session_state.chart_history.append(chart_entry)
#     st.session_state.chart_history = st.session_state.chart_history[-MAX_CHART_ROWS:]

#     if is_ml_anomaly:
#         st.session_state.ml_anomaly_count += 1
#     if is_agent_blocked:
#         st.session_state.agent_block_count += 1
#     if final_status == "ML_OVERRIDDEN_ALLOW":
#         st.session_state.override_count += 1

#     total_latency_ms = ml_latency_ms + (agent_latency_s * 1000.0)

#     if prediction == 1 and investigation_output:
#         investigation_output_dict = investigation_output.to_dict()
#         reason_code, reason_tags = _derive_reason_metadata(investigation_output)
#         event_id = (
#             f"EVT_{int(time.time() * 1000)}_{idx}_{str(sample['nameOrig'])}"
#             .replace(" ", "_")
#             .replace("|", "_")
#         )
#         control_payload = {}
#         control_ack = {}
#         control_status = "not_required"
#         is_held_for_approval = final_status == "HELD_PENDING_APPROVAL"
#         if is_held_for_approval:
#             control_payload, control_ack, control_status = issue_hold_command(
#                 event_id=event_id,
#                 sample=sample,
#                 investigation_output=investigation_output,
#                 reason_code=reason_code,
#             )
#         investigation_event = {
#             "event_id": event_id,
#             "event_time": timestamp,
#             "stream_index": int(idx),
#             "step": int(sample["step"]),
#             "nameOrig": str(sample["nameOrig"]),
#             "nameDest": str(sample["nameDest"]),
#             "txn_type": str(sample["type"]),
#             "amount": float(sample["amount"]),
#             "ml_prediction": prediction,
#             "ground_truth": ground_truth,
#             "final_status": final_status,
#             "final_verdict": investigation_output.final_verdict,
#             "ml_correction": investigation_output.ml_correction,
#             "confidence_score": investigation_output.confidence_score,
#             "risk_score": investigation_output.risk_score,
#             "fallback_used": investigation_output.fallback_used,
#             "reasoning": investigation_output.reasoning,
#             "reason_code": reason_code,
#             "reason_tags_json": _safe_json_text({"reason_tags": reason_tags}),
#             "control_action": "hold" if is_held_for_approval else investigation_output.control_action,
#             "control_status": control_status,
#             "control_command_json": _safe_json_text(control_payload),
#             "control_ack_json": _safe_json_text(control_ack),
#             "approval_status": "pending_approval" if is_held_for_approval else "not_required",
#             "approval_action": "",
#             "approval_time": "",
#             "approval_note": "",
#             "reviewed_by": "",
#             "release_command_json": "",
#             "release_ack_json": "",
#             "evidence_json": _safe_json_text({"evidence": investigation_output.evidence}),
#             "tool_trace_json": _safe_json_text({"tool_trace": investigation_output.tool_trace}),
#             "agent_latency_s": round(agent_latency_s, 4),
#             "ml_latency_ms": round(ml_latency_ms, 3),
#             "total_latency_ms": round(total_latency_ms, 3),
#             "raw_investigation_json": _safe_json_text(investigation_output_dict),
#         }
#         persist_investigation_event(investigation_event)
#         st.session_state.investigation_event_log.insert(0, investigation_event)
#         st.session_state.investigation_event_log = st.session_state.investigation_event_log[
#             :MAX_INVESTIGATION_EVENT_ROWS
#         ]
#         st.session_state.latest_event_id = event_id

#     st.session_state.last_ml_latency_ms = ml_latency_ms
#     st.session_state.last_agent_latency_s = agent_latency_s
#     st.session_state.last_total_latency_ms = total_latency_ms
#     st.session_state.stream_index += 1


# def render_graphs() -> None:
#     if not st.session_state.chart_history:
#         st.info("No stream data yet. Start the stream to see live anomaly detection.")
#         return

#     chart_df = pd.DataFrame(st.session_state.chart_history)
#     # Backward compatibility: older session rows may not have risk_score yet.
#     if "risk_score" not in chart_df.columns:
#         chart_df["risk_score"] = (
#             chart_df.get("anomaly_label", pd.Series(dtype=str))
#             .map({"Held Pending Approval": 90.0, "Agent Blocked": 90.0, "ML Fraud": 65.0, "Clean": 5.0})
#             .fillna(0.0)
#         )
#     else:
#         chart_df["risk_score"] = pd.to_numeric(chart_df["risk_score"], errors="coerce").fillna(0.0)
#     grouped = (
#         chart_df.groupby(["type", "anomaly_label"], as_index=False)
#         .size()
#         .rename(columns={"size": "count"})
#     )
#     order = ["Clean", "ML Fraud", "Held Pending Approval"]
#     fig = px.bar(
#         grouped,
#         x="type",
#         y="count",
#         color="anomaly_label",
#         barmode="stack",
#         category_orders={"anomaly_label": order},
#         color_discrete_map={
#             "Clean": "#4C78A8",
#             "ML Fraud": "#F58518",
#             "Held Pending Approval": "#E45756",
#             "Agent Blocked": "#E45756",
#         },
#         title="Transaction Type vs Classification Layer",
#     )
#     fig.update_layout(legend_title_text="Classification")
#     st.plotly_chart(fig, use_container_width=True)

#     timeline_df = chart_df.copy()
#     timeline_df["step"] = pd.to_numeric(timeline_df["step"], errors="coerce")
#     timeline_df = timeline_df.dropna(subset=["step"]).sort_values("step")
#     if not timeline_df.empty:
#         timeline = px.line(
#             timeline_df,
#             x="step",
#             y="risk_score",
#             markers=True,
#             title="Risk Score Timeline (Agent-Investigated Transactions)",
#         )
#         timeline.update_traces(line_color="#E45756")
#         st.plotly_chart(timeline, use_container_width=True)


# def render_decision_funnel() -> None:
#     processed = st.session_state.stream_index
#     ml_fraud = st.session_state.ml_anomaly_count
#     held = st.session_state.agent_block_count
#     overrides = st.session_state.override_count
#     fig = go.Figure(
#         go.Bar(
#             x=["Processed", "ML Fraud", "Held", "ML Overrides"],
#             y=[processed, ml_fraud, held, overrides],
#             marker_color=["#4C78A8", "#F58518", "#E45756", "#72B7B2"],
#         )
#     )
#     fig.update_layout(title="Decision Funnel", xaxis_title="", yaxis_title="Count")
#     st.plotly_chart(fig, use_container_width=True)


# def render_chatbot() -> None:
#     st.subheader("DB Insights Chat")
#     st.caption("Ask read-only analytics questions about the transactions table.")

#     for msg in st.session_state.chat_history:
#         if msg["role"] == "user":
#             st.markdown(f"**Question:** {msg['content']}")
#             continue

#         st.markdown(f"**Answer:** {msg['content']}")
#         if msg.get("sql"):
#             st.code(msg["sql"], language="sql")
#         if msg.get("rows"):
#             st.dataframe(pd.DataFrame(msg["rows"]), use_container_width=True)
#         if msg.get("insight"):
#             st.info(msg["insight"])
#         if msg.get("safety_note"):
#             st.caption(msg["safety_note"])
#         if msg.get("error"):
#             st.caption(f"Note: {msg['error']}")
#         st.divider()

#     suggestions = [""] + get_suggested_questions()
#     selected_prompt = st.selectbox(
#         "Suggested question",
#         suggestions,
#         format_func=lambda value: value or "Write your own question",
#         key="db_chat_suggestion",
#     )

#     with st.form("db_chat_form", clear_on_submit=True):
#         prompt = st.text_input(
#             "Database question",
#             value=selected_prompt,
#             placeholder="Example: Show fraud count by transaction type",
#         )
#         submitted = st.form_submit_button("Ask DB Insights", use_container_width=True)

#     if not submitted:
#         return

#     prompt = (prompt or "").strip()
#     if not prompt:
#         st.warning("Please enter a database question.")
#         return

#     st.session_state.chat_history.append({"role": "user", "content": prompt})
#     with st.spinner("Querying the database..."):
#         answer = answer_db_question(prompt)

#     st.session_state.chat_history.append(
#         {
#             "role": "assistant",
#             "content": answer["answer"],
#             "sql": answer.get("sql", ""),
#             "rows": answer.get("rows", []),
#             "insight": answer.get("insight", ""),
#             "safety_note": answer.get("safety_note", ""),
#             "error": answer.get("error"),
#         }
#     )
#     st.rerun()


# def _humanize_tool_trace(trace_items: list) -> list[str]:
#     readable_steps: list[str] = []
#     for raw_item in trace_items:
#         item = str(raw_item)
#         lowered = item.lower()

#         if "get_user_transaction_history" in lowered:
#             sentence = "Reviewed the sender's recent transaction history to understand normal account behavior."
#         elif "get_recipient_transaction_history" in lowered:
#             sentence = "Reviewed the recipient's recent activity to check whether the destination account looked unusual."
#         elif "detect_user_liquidation_pattern" in lowered:
#             sentence = "Checked whether the sender showed a liquidation pattern, such as draining balance through transfer and cash-out activity."
#         elif "detect_user_repetitive_payments" in lowered:
#             sentence = "Checked whether the transaction looked like a repeated, consistent payment that may be a false alarm."
#         elif "find_similar_transactions" in lowered:
#             sentence = "Compared this transaction with similar historical transactions by type and amount."
#         else:
#             sentence = "Reviewed an additional transaction signal from the investigation workflow."

#         if '"detected": true' in lowered or "'detected': true" in lowered:
#             sentence += " The signal was present."
#         elif '"detected": false' in lowered or "'detected': false" in lowered:
#             sentence += " No strong signal was found."
#         elif "not found" in lowered:
#             sentence += " No matching history was found."
#         elif "database error" in lowered:
#             sentence += " The lookup could not be completed, so the decision used the remaining available context."

#         if sentence not in readable_steps:
#             readable_steps.append(sentence)

#     return readable_steps


# def _compact_display(value: object, decimals: int = 2) -> str:
#     if value is None or str(value) == "nan":
#         return "N/A"
#     try:
#         numeric_value = float(value)
#         return f"{numeric_value:.{decimals}f}"
#     except Exception:
#         text = str(value)
#         return text if len(text) <= 18 else f"{text[:15]}..."


# def _render_detail_summary(row: pd.Series) -> None:
#     status = html.escape(str(row.get("final_status") or "N/A"))
#     verdict = html.escape(str(row.get("final_verdict") or "N/A"))
#     risk_score = html.escape(_compact_display(row.get("risk_score"), decimals=1))
#     reason_code = html.escape(_compact_display(row.get("reason_code"), decimals=0))
#     confidence = html.escape(_compact_display(row.get("confidence_score"), decimals=2))

#     st.markdown(
#         f"""
#         <div class="investigation-summary">
#             <div class="summary-tile">
#                 <span>Status</span>
#                 <strong>{status}</strong>
#             </div>
#             <div class="summary-tile">
#                 <span>Verdict</span>
#                 <strong>{verdict}</strong>
#             </div>
#             <div class="summary-tile">
#                 <span>Risk</span>
#                 <strong>{risk_score}</strong>
#             </div>
#             <div class="summary-tile">
#                 <span>Reason</span>
#                 <strong>{reason_code}</strong>
#             </div>
#             <div class="summary-tile">
#                 <span>Confidence</span>
#                 <strong>{confidence}</strong>
#             </div>
#         </div>
#         """,
#         unsafe_allow_html=True,
#     )


# def render_investigation_event_log() -> None:
#     st.subheader("Investigation Event Log")

#     event_df = fetch_recent_audit_events(limit=400)
#     if event_df.empty:
#         st.info("No investigation events yet. Fraud-flagged transactions will appear here.")
#         return

#     visible_columns = [
#         "event_time",
#         "nameOrig",
#         "nameDest",
#         "txn_type",
#         "amount",
#         "final_status",
#         "final_verdict",
#         "approval_status",
#         "approval_action",
#         "reason_code",
#         "control_action",
#         "control_status",
#         "risk_score",
#         "reasoning",
#         "ml_correction",
#         "confidence_score",
#         "agent_latency_s",
#         "fallback_used",
#     ]
#     visible_columns = [col for col in visible_columns if col in event_df.columns]
#     event_display_df = event_df.copy()
#     if "amount" in event_display_df.columns:
#         event_display_df["amount"] = event_display_df["amount"].map(lambda v: f"${v:,.2f}")
#     if "reasoning" in event_display_df.columns:
#         event_display_df["reasoning"] = event_display_df["reasoning"].fillna("").map(
#             lambda txt: (str(txt)[:80] + "...") if len(str(txt)) > 80 else str(txt)
#         )
#     if "reason_tags_json" in event_display_df.columns:
#         def _render_reason_tags(tags_json: str) -> str:
#             try:
#                 payload = json.loads(str(tags_json or "{}"))
#                 tags = payload.get("reason_tags", [])
#                 if isinstance(tags, list):
#                     return ", ".join([str(t) for t in tags[:4]])
#             except Exception:
#                 pass
#             return ""
#         event_display_df["reason_tags"] = event_display_df["reason_tags_json"].map(_render_reason_tags)
#         if "reason_tags" not in visible_columns:
#             visible_columns.append("reason_tags")
#     st.dataframe(event_display_df[visible_columns], use_container_width=True, height=260)

#     st.markdown("**Investigation Reason Details**")
#     pending_df = event_df[event_df["final_status"].astype(str) == "HELD_PENDING_APPROVAL"]
#     details_df = (
#         pd.concat([pending_df, event_df.head(10)], ignore_index=True)
#         .drop_duplicates(subset=["event_id"])
#         .head(30)
#         .copy()
#     )
#     details_df["event_id"] = details_df["event_id"].astype(str)
#     event_ids = details_df["event_id"].tolist()

#     if st.session_state.get("selected_investigation_event_id") not in event_ids:
#         st.session_state.pop("selected_investigation_event_id", None)
#         default_index = 0
#     else:
#         default_index = event_ids.index(st.session_state["selected_investigation_event_id"])

#     label_lookup = {}
#     for _, row in details_df.reset_index(drop=True).iterrows():
#         event_time = str(row.get("event_time", ""))
#         event_time = event_time[-8:] if len(event_time) >= 8 else event_time
#         verdict = str(row.get("final_verdict") or "N/A").upper()
#         status = str(row.get("final_status") or "N/A")
#         label_lookup[str(row.get("event_id"))] = (
#             f"{row.get('nameOrig', 'N/A')} | {event_time} | {verdict} | {status}"
#         )

#     selected_event_id = st.selectbox(
#         "View investigation",
#         options=event_ids,
#         index=default_index,
#         format_func=lambda event_id: label_lookup.get(str(event_id), str(event_id)),
#         key="selected_investigation_event_id",
#         accept_new_options=False,
#         width="stretch",
#     )
#     selected_event_id = str(selected_event_id or event_ids[default_index])
#     row = details_df[details_df["event_id"] == selected_event_id].iloc[0]

#     # Keep inline analyst inputs scoped to the currently selected investigation.
#     if st.session_state.get("analyst_selected_event_id") != selected_event_id:
#         st.session_state["analyst_selected_event_id"] = selected_event_id
#         st.session_state["analyst_action_inline"] = "APPROVE_RELEASE"
#         st.session_state["analyst_note_inline"] = ""

#     _render_detail_summary(row)

#     control_action = str(row.get("control_action") or "none")
#     if control_action == "reroute":
#         # Reroute status is retained in the event table but intentionally hidden
#         # from the detail card to keep the UI focused for business users.
#         pass

#     try:
#         tags_payload = json.loads(str(row.get("reason_tags_json") or "{}"))
#         tags = tags_payload.get("reason_tags", [])
#         if tags:
#             st.write(f"Reason Tags: {', '.join([str(t) for t in tags])}")
#     except Exception:
#         pass

#     reasoning_text = row.get("reasoning") or "No reasoning provided."
#     st.write(reasoning_text)

#     try:
#         evidence_payload = json.loads(str(row.get("evidence_json") or "{}"))
#         evidence = evidence_payload.get("evidence", [])
#         if evidence:
#             st.write("Evidence:")
#             for item in evidence:
#                 st.write(f"- {item}")
#     except Exception:
#         pass

#     try:
#         trace_payload = json.loads(str(row.get("tool_trace_json") or "{}"))
#         trace = trace_payload.get("tool_trace", [])
#         if trace:
#             st.write("Investigation Steps:")
#             for step in _humanize_tool_trace(trace):
#                 st.write(f"- {step}")
#     except Exception:
#         pass

#     st.markdown("&nbsp;", unsafe_allow_html=True)
#     st.markdown("**Analyst Review**")
#     st.caption(f"Selected Event: {selected_event_id}")
#     review_success_message = st.session_state.pop("analyst_review_success", "")
#     if review_success_message:
#         st.success(review_success_message)

#     selected_verdict = str(row.get("final_verdict") or "").upper()
#     final_status = str(row.get("final_status") or "").upper()
#     if selected_verdict == "BLOCK" and final_status == "HELD_PENDING_APPROVAL":
#         valid_actions = {"APPROVE_RELEASE", "CONFIRM_BLOCK", "ESCALATE"}
#         if st.session_state.get("analyst_action_inline") not in valid_actions:
#             st.session_state["analyst_action_inline"] = "APPROVE_RELEASE"
#         st.selectbox(
#             "Analyst Action",
#             ["APPROVE_RELEASE", "CONFIRM_BLOCK", "ESCALATE"],
#             key="analyst_action_inline",
#         )
#         st.text_area(
#             "Analyst Note",
#             placeholder="Manual review note...",
#             key="analyst_note_inline",
#             height=100,
#         )
#         if st.button("Submit Analyst Decision", use_container_width=True, key="save_analyst_review_inline"):
#             analyst_action = str(st.session_state.get("analyst_action_inline", "APPROVE_RELEASE"))
#             analyst_note = str(st.session_state.get("analyst_note_inline", ""))
#             persist_analyst_review(
#                 selected_event_id,
#                 analyst_action,
#                 analyst_note,
#             )
#             st.session_state["analyst_review_success"] = apply_analyst_approval(
#                 row,
#                 analyst_action,
#                 analyst_note,
#             )
#             st.rerun()
#     elif selected_verdict == "BLOCK":
#         approval_status = row.get("approval_status") or "reviewed"
#         approval_time = row.get("approval_time") or "N/A"
#         reviewed_by = row.get("reviewed_by") or DEFAULT_REVIEWER
#         st.success(
#             f"Decision completed: {final_status or approval_status}. "
#             f"Reviewed by {reviewed_by} at {approval_time}."
#         )
#     else:
#         st.info("Analyst review is enabled only for blocked investigations.")

#     reviews_df = fetch_recent_reviews(limit=20)
#     if reviews_df.empty:
#         st.caption("No analyst reviews saved yet.")
#     else:
#         st.dataframe(reviews_df, use_container_width=True, height=220)

#     export_ts = pd.Timestamp.now().strftime("%Y%m%d_%H%M%S")
#     csv_data = event_df.to_csv(index=False).encode("utf-8")
#     json_data = event_df.to_json(orient="records", indent=2)

#     dl_col1, dl_col2 = st.columns(2)
#     dl_col1.download_button(
#         "Download Event Log (CSV)",
#         data=csv_data,
#         file_name=f"investigation_event_log_{export_ts}.csv",
#         mime="text/csv",
#         use_container_width=True,
#     )
#     dl_col2.download_button(
#         "Download Event Log (JSON)",
#         data=json_data,
#         file_name=f"investigation_event_log_{export_ts}.json",
#         mime="application/json",
#         use_container_width=True,
#     )


# def render_visual_theme() -> None:
#     st.markdown(
#         """
#         <style>
#         .stApp {
#             background: radial-gradient(circle at 15% 20%, #f0f7ff 0%, #f6f2ea 45%, #f8fbff 100%);
#         }
#         .block-container {
#             padding-top: 1.3rem;
#         }
#         h1, h2, h3 {
#             color: #12344d;
#         }
#         .investigation-summary {
#             display: grid;
#             grid-template-columns: repeat(5, minmax(0, 1fr));
#             gap: 8px;
#             margin: 10px 0 14px;
#         }
#         .summary-tile {
#             border: 1px solid rgba(18, 52, 77, 0.12);
#             background: rgba(255, 255, 255, 0.58);
#             border-radius: 8px;
#             padding: 8px 10px;
#             min-width: 0;
#         }
#         .summary-tile span {
#             display: block;
#             color: #6b7280;
#             font-size: 0.72rem;
#             line-height: 1.1;
#             margin-bottom: 4px;
#         }
#         .summary-tile strong {
#             display: block;
#             color: #12344d;
#             font-size: 0.94rem;
#             line-height: 1.15;
#             font-weight: 650;
#             white-space: nowrap;
#             overflow: hidden;
#             text-overflow: ellipsis;
#         }
#         /* Reduce rerun fade/blur artifacts during live updates. */
#         .element-container {
#             opacity: 1 !important;
#         }
#         @media (max-width: 760px) {
#             .investigation-summary {
#                 grid-template-columns: repeat(2, minmax(0, 1fr));
#             }
#         }
#         </style>
#         """,
#         unsafe_allow_html=True,
#     )


# def render_live_monitoring_fragment(model: CatBoostClassifier, raw_data: pd.DataFrame) -> None:
#     if st.session_state.stream_running:
#         process_next_transaction(model, raw_data)

#     st.caption(f"Rows processed: {st.session_state.stream_index}/{len(raw_data)}")

#     if st.session_state.stream_done_message:
#         st.success(st.session_state.stream_done_message)

#     met1, met2, met3, met4, met5 = st.columns(5)
#     met1.metric("Processed", st.session_state.stream_index)
#     met2.metric("ML Anomalies", st.session_state.ml_anomaly_count)
#     met3.metric("Held", st.session_state.agent_block_count)
#     met4.metric("ML Overrides", st.session_state.override_count)
#     met5.metric("Total Latency", f"{st.session_state.last_total_latency_ms:.1f} ms")

#     left, right = st.columns([2, 1.2])
#     with left:
#         render_graphs()
#         render_decision_funnel()
#         if st.session_state.log_history:
#             log_df = pd.DataFrame(st.session_state.log_history)
#             log_df["Amount"] = log_df["Amount"].map(lambda v: f"${v:,.2f}")
#             st.subheader("Recent Stream Events")
#             st.dataframe(log_df, use_container_width=True)
#         else:
#             st.info("No transactions processed yet.")

#     with right:
#         render_investigation_event_log()


# def render_live_monitoring(model: CatBoostClassifier, raw_data: pd.DataFrame) -> None:
#     run_every = STREAM_SLEEP_SECONDS if st.session_state.stream_running else None
#     st.fragment(render_live_monitoring_fragment, run_every=run_every)(model, raw_data)


# def main() -> None:
#     st.set_page_config(page_title="AGORA Fraud Monitoring", layout="wide")
#     render_visual_theme()
#     init_audit_tables()
#     ensure_fresh_boot_state()
#     st.title("AGORA Risk Command Center")
#     # st.caption("CatBoost flags risk, then the investigation agent confirms or overturns each fraud alert.")
#     # st.caption(f"Live stream source: {LIVE_STREAM_FILE}")

#     init_state()
#     model, raw_data = load_assets()

#     ctrl1, ctrl2, ctrl3, ctrl4 = st.columns([1, 1, 1.4, 1.6])
#     if ctrl1.button("Start Live Stream", use_container_width=True):
#         st.session_state.stream_running = True
#         st.session_state.stream_done_message = ""
#     if ctrl2.button("Stop Stream", use_container_width=True):
#         st.session_state.stream_running = False
#     if ctrl3.button("Reset Fresh", use_container_width=True):
#         hard_reset_application(clear_audit=True)
#         st.rerun()
#     if ctrl4.button("Refresh New Session", use_container_width=True):
#         refresh_application_state(clear_audit=True)

#     active_view = st.segmented_control(
#         "View",
#         ["Live Monitoring", "DB Insights Chat"],
#         default="Live Monitoring",
#         key="active_view",
#     )

#     if active_view == "Live Monitoring":
#         render_live_monitoring(model, raw_data)
#     else:
#         render_chatbot()


# if __name__ == "__main__":
#     main()
import json
import html
import sqlite3
import time

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st
from catboost import CatBoostClassifier

from db_chat import answer_db_question, get_suggested_questions
from risk_agent import InvestigationResult, run_investigation

STREAM_SLEEP_SECONDS = 0.75
MAX_LOG_ROWS = 200
MAX_CHART_ROWS = 500
MAX_INVESTIGATION_EVENT_ROWS = 2000
LIVE_STREAM_FILE = "X_test.csv"
DB_PATH = "agora_transactions.db"
FRESH_BOOT_CLEARS_AUDIT = True


def get_db_connection(timeout: int = 20) -> sqlite3.Connection:
    """Create a database connection with WAL mode and a custom timeout."""
    conn = sqlite3.connect(DB_PATH, timeout=timeout)
    # Enable WAL mode for better concurrency (multiple readers, one writer)
    conn.execute("PRAGMA journal_mode=WAL")
    return conn
MODEL_FEATURE_COLUMNS = [
    "step",
    "type",
    "amount",
    "oldbalanceOrg",
    "newbalanceOrig",
    "oldbalanceDest",
    "newbalanceDest",
]
DEFAULT_REVIEWER = "dashboard_analyst"


@st.cache_resource
def load_assets() -> tuple[CatBoostClassifier, pd.DataFrame]:
    model = CatBoostClassifier()
    model.load_model("agora_fraud_model.cbm")
    df = pd.read_csv(LIVE_STREAM_FILE, nrows=1000)

    if "nameOrig" not in df.columns:
        df["nameOrig"] = [f"XUSER_{i:06d}" for i in range(len(df))]
    if "nameDest" not in df.columns:
        df["nameDest"] = [f"XDEST_{i:06d}" for i in range(len(df))]
    if "isFraud" not in df.columns:
        df["isFraud"] = 0
    if "isFlaggedFraud" not in df.columns:
        df["isFlaggedFraud"] = 0

    return model, df


def init_state() -> None:
    defaults = {
        "stream_running": False,
        "stream_index": 0,
        "log_history": [],
        "chart_history": [],
        "latest_event_id": "",
        "ml_anomaly_count": 0,
        "agent_block_count": 0,
        "override_count": 0,
        "last_ml_latency_ms": 0.0,
        "last_agent_latency_s": 0.0,
        "last_total_latency_ms": 0.0,
        "chat_history": [],
        "investigation_event_log": [],
        "stream_done_message": "",
    }
    for key, value in defaults.items():
        if key not in st.session_state:
            st.session_state[key] = value


def reset_stream_state() -> None:
    st.session_state.stream_running = False
    st.session_state.stream_index = 0
    st.session_state.log_history = []
    st.session_state.chart_history = []
    st.session_state.latest_event_id = ""
    st.session_state.ml_anomaly_count = 0
    st.session_state.agent_block_count = 0
    st.session_state.override_count = 0
    st.session_state.last_ml_latency_ms = 0.0
    st.session_state.last_agent_latency_s = 0.0
    st.session_state.last_total_latency_ms = 0.0
    st.session_state.investigation_event_log = []
    st.session_state.stream_done_message = ""


def refresh_application_state(clear_audit: bool = True) -> None:
    if clear_audit:
        clear_audit_tables()
    reset_stream_state()
    st.session_state.chat_history = []
    st.session_state.db_chat_suggestion = ""
    st.session_state.pop("selected_investigation_event_id", None)
    st.session_state.pop("analyst_selected_event_id", None)
    st.session_state.pop("analyst_action_inline", None)
    st.session_state.pop("analyst_note_inline", None)
    st.session_state["active_view"] = "Live Monitoring"


def init_audit_tables() -> None:
    with get_db_connection() as conn:
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
        _ensure_table_columns(
            conn,
            "investigation_events",
            {
                "reason_code": "reason_code TEXT",
                "reason_tags_json": "reason_tags_json TEXT",
                "control_action": "control_action TEXT",
                "control_status": "control_status TEXT",
                "control_command_json": "control_command_json TEXT",
                "control_ack_json": "control_ack_json TEXT",
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


def clear_audit_tables() -> None:
    with get_db_connection() as conn:
        conn.execute("DELETE FROM analyst_reviews")
        conn.execute("DELETE FROM control_commands")
        conn.execute("DELETE FROM investigation_events")
        conn.commit()


def hard_reset_application(clear_audit: bool = True) -> None:
    if clear_audit:
        clear_audit_tables()
    load_assets.clear()
    st.session_state.clear()


def ensure_fresh_boot_state() -> None:
    if st.session_state.get("_fresh_boot_initialized"):
        return
    if FRESH_BOOT_CLEARS_AUDIT:
        clear_audit_tables()
    st.session_state["_fresh_boot_initialized"] = True


def persist_investigation_event(event: dict) -> None:
    columns = [
        "event_id", "event_time", "stream_index", "step", "nameOrig", "nameDest",
        "txn_type", "amount", "ml_prediction", "ground_truth", "final_status",
        "final_verdict", "ml_correction", "confidence_score", "risk_score",
        "fallback_used", "reasoning", "reason_code", "reason_tags_json",
        "control_action", "control_status", "control_command_json", "control_ack_json",
        "approval_status", "approval_action", "approval_time", "approval_note",
        "reviewed_by", "release_command_json", "release_ack_json", "evidence_json",
        "tool_trace_json", "agent_latency_s", "ml_latency_ms", "total_latency_ms",
        "raw_investigation_json",
    ]
    values = []
    for column in columns:
        value = event.get(column)
        if column in {"ml_correction", "fallback_used"}:
            value = int(bool(value))
        values.append(value)

    with get_db_connection() as conn:
        conn.execute(
            f"""
            INSERT OR REPLACE INTO investigation_events (
                {", ".join(columns)}
            ) VALUES ({", ".join(["?"] * len(columns))})
            """,
            tuple(values),
        )
        conn.commit()


def persist_analyst_review(event_id: str, analyst_action: str, analyst_note: str) -> None:
    with get_db_connection() as conn:
        conn.execute(
            """
            INSERT INTO analyst_reviews (review_time, event_id, analyst_action, analyst_note)
            VALUES (?, ?, ?, ?)
            """,
            (
                pd.Timestamp.now().strftime("%Y-%m-%d %H:%M:%S"),
                event_id,
                analyst_action,
                analyst_note,
            ),
        )
        conn.commit()


def persist_control_command(command_row: dict) -> None:
    with get_db_connection() as conn:
        conn.execute(
            """
            INSERT OR REPLACE INTO control_commands (
                command_id, event_id, created_time, nameOrig, nameDest, txn_type,
                amount, command, destination, status, payload_json, ack_json
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                command_row.get("command_id"), command_row.get("event_id"),
                command_row.get("created_time"), command_row.get("nameOrig"),
                command_row.get("nameDest"), command_row.get("txn_type"),
                command_row.get("amount"), command_row.get("command"),
                command_row.get("destination"), command_row.get("status"),
                command_row.get("payload_json"), command_row.get("ack_json"),
            ),
        )
        conn.commit()


def fetch_recent_audit_events(limit: int = 300) -> pd.DataFrame:
    with get_db_connection() as conn:
        return pd.read_sql_query(
            f"SELECT * FROM investigation_events ORDER BY event_time DESC LIMIT {int(limit)}",
            conn,
        )


def fetch_recent_reviews(limit: int = 40) -> pd.DataFrame:
    with get_db_connection() as conn:
        return pd.read_sql_query(
            f"SELECT * FROM analyst_reviews ORDER BY review_time DESC LIMIT {int(limit)}",
            conn,
        )


def classify_outcome(
    prediction: int, investigation_output: InvestigationResult | None
) -> tuple[str, str, bool, bool]:
    if prediction == 0:
        return "ALLOW", "Clean", False, False
    if not investigation_output:
        return "AGENT_ERROR", "ML Fraud", True, False
    verdict = investigation_output.final_verdict.upper()
    if verdict == "BLOCK":
        return "HELD_PENDING_APPROVAL", "Held Pending Approval", True, True
    return "ML_OVERRIDDEN_ALLOW", "ML Fraud", True, False


def _safe_json_text(payload: dict | None) -> str:
    if not payload:
        return ""
    try:
        return json.dumps(payload, ensure_ascii=False)
    except Exception:
        return str(payload)


def _transaction_payload(step, name_orig, name_dest, txn_type, amount) -> dict:
    return {
        "step": int(step), "nameOrig": str(name_orig), "nameDest": str(name_dest),
        "type": str(txn_type), "amount": float(amount),
    }


def _transaction_payload_from_sample(sample: pd.Series) -> dict:
    return _transaction_payload(
        sample["step"], sample["nameOrig"], sample["nameDest"],
        sample["type"], sample["amount"],
    )


def _transaction_payload_from_event(row: pd.Series) -> dict:
    return _transaction_payload(
        row.get("step", 0), row.get("nameOrig", ""), row.get("nameDest", ""),
        row.get("txn_type", ""), row.get("amount", 0.0),
    )


def issue_control_command(
    event_id, transaction, command, destination, status, reason,
    requested_by, reason_code="", risk_score=None, note="",
) -> tuple[dict, dict, str]:
    command_id = f"CMD_{int(time.time() * 1000)}_{event_id}_{command}"
    created_time = pd.Timestamp.now().strftime("%Y-%m-%d %H:%M:%S")
    payload = {
        "command": command, "command_id": command_id, "event_id": event_id,
        "destination": destination, "reason": reason, "reason_code": reason_code,
        "requested_by": requested_by, "transaction": transaction,
    }
    if risk_score is not None:
        payload["risk_score"] = risk_score
    if note:
        payload["note"] = note

    ack = {
        "status": "success", "command": command, "command_id": command_id,
        "event_id": event_id, "control_status": status, "destination": destination,
        "ack_id": f"ACK_{int(time.time() * 1000)}_{event_id}_{command}",
        "acknowledged_at": created_time,
    }
    persist_control_command({
        "command_id": command_id, "event_id": event_id, "created_time": created_time,
        "nameOrig": transaction["nameOrig"], "nameDest": transaction["nameDest"],
        "txn_type": transaction["type"], "amount": transaction["amount"],
        "command": command, "destination": destination, "status": status,
        "payload_json": _safe_json_text(payload), "ack_json": _safe_json_text(ack),
    })
    return payload, ack, status


def issue_hold_command(event_id, sample, investigation_output, reason_code):
    return issue_control_command(
        event_id=event_id, transaction=_transaction_payload_from_sample(sample),
        command="hold", destination=investigation_output.control_destination or "manual_review_queue",
        status="pending_approval", reason="ai_block_pending_analyst_approval",
        requested_by="agora_agent", reason_code=reason_code,
        risk_score=investigation_output.risk_score,
    )


def update_investigation_approval(
    event_id, final_status, approval_status, approval_action,
    approval_note, reviewed_by, control_action, control_status,
    release_payload=None, release_ack=None,
) -> None:
    approval_time = pd.Timestamp.now().strftime("%Y-%m-%d %H:%M:%S")
    with get_db_connection() as conn:
        conn.execute(
            """
            UPDATE investigation_events
            SET final_status=?, approval_status=?, approval_action=?, approval_time=?,
                approval_note=?, reviewed_by=?, control_action=?, control_status=?,
                release_command_json=?, release_ack_json=?
            WHERE event_id=?
            """,
            (
                final_status, approval_status, approval_action, approval_time,
                approval_note, reviewed_by, control_action, control_status,
                _safe_json_text(release_payload), _safe_json_text(release_ack), event_id,
            ),
        )
        conn.commit()


def apply_analyst_approval(row: pd.Series, analyst_action: str, analyst_note: str) -> str:
    event_id = str(row.get("event_id") or "")
    transaction = _transaction_payload_from_event(row)
    note = str(analyst_note or "")

    if analyst_action == "APPROVE_RELEASE":
        release_payload, release_ack, control_status = issue_control_command(
            event_id=event_id, transaction=transaction, command="release",
            destination="payment_processor", status="released",
            reason="analyst_approved_release", requested_by=DEFAULT_REVIEWER,
            risk_score=float(row.get("risk_score") or 0.0), note=note,
        )
        update_investigation_approval(
            event_id=event_id, final_status="RELEASED_AFTER_APPROVAL",
            approval_status="released", approval_action=analyst_action, approval_note=note,
            reviewed_by=DEFAULT_REVIEWER, control_action="release",
            control_status=control_status, release_payload=release_payload, release_ack=release_ack,
        )
        return "Transaction released after analyst approval."

    if analyst_action == "CONFIRM_BLOCK":
        _, _, control_status = issue_control_command(
            event_id=event_id, transaction=transaction, command="confirm_block",
            destination="blocked_ledger", status="blocked_confirmed",
            reason="analyst_confirmed_ai_block", requested_by=DEFAULT_REVIEWER,
            risk_score=float(row.get("risk_score") or 0.0), note=note,
        )
        update_investigation_approval(
            event_id=event_id, final_status="BLOCKED_CONFIRMED",
            approval_status="blocked_confirmed", approval_action=analyst_action,
            approval_note=note, reviewed_by=DEFAULT_REVIEWER,
            control_action="confirm_block", control_status=control_status,
        )
        return "Block confirmed by analyst."

    update_investigation_approval(
        event_id=event_id, final_status="ESCALATED", approval_status="escalated",
        approval_action="ESCALATE", approval_note=note, reviewed_by=DEFAULT_REVIEWER,
        control_action=str(row.get("control_action") or "hold"), control_status="escalated",
    )
    return "Transaction escalated for further review."


def _derive_reason_metadata(investigation_output: InvestigationResult) -> tuple[str, list[str]]:
    evidence_text = " ".join([str(item) for item in investigation_output.evidence]).lower()
    reasoning_text = str(investigation_output.reasoning or "").lower()
    tags: list[str] = []
    if investigation_output.fallback_used:
        tags.append("fallback_mode")
    if "liquidation" in evidence_text or "cash_out" in evidence_text:
        tags.append("liquidation_pattern")
    if "repetitive" in evidence_text or "low-variance" in evidence_text or "false positive" in evidence_text:
        tags.append("repetitive_payment_pattern")
    if "new account" in evidence_text or "no user history" in evidence_text:
        tags.append("insufficient_history")
    if "similar" in evidence_text:
        tags.append("similar_txn_context")

    reason_code = "GENERIC_REVIEW"
    if "liquidation_pattern" in tags and investigation_output.final_verdict.upper() == "BLOCK":
        reason_code = "LIQUIDATION_BLOCK"
    elif "repetitive_payment_pattern" in tags and investigation_output.final_verdict.upper() == "ALLOW":
        reason_code = "REPETITIVE_ALLOW"
    elif investigation_output.fallback_used:
        reason_code = "FALLBACK_DECISION"
    elif investigation_output.final_verdict.upper() == "BLOCK":
        reason_code = "RISK_BLOCK"
    elif investigation_output.ml_correction:
        reason_code = "ML_CORRECTED_ALLOW"

    if not tags and reasoning_text:
        if "liquidation" in reasoning_text:
            tags.append("liquidation_pattern")
        elif "false positive" in reasoning_text or "repetitive" in reasoning_text:
            tags.append("repetitive_payment_pattern")
        elif "fallback" in reasoning_text:
            tags.append("fallback_mode")

    return reason_code, tags


def process_next_transaction(model: CatBoostClassifier, raw_data: pd.DataFrame) -> None:
    idx = st.session_state.stream_index
    if idx >= len(raw_data):
        st.session_state.stream_running = False
        st.session_state.stream_done_message = "Stream completed all available rows."
        return

    sample = raw_data.iloc[idx]
    features = sample[MODEL_FEATURE_COLUMNS].values

    ml_start = time.perf_counter()
    prediction = int(model.predict([features])[0])
    ml_latency_ms = (time.perf_counter() - ml_start) * 1000
    agent_latency_s = 0.0
    investigation_output: InvestigationResult | None = None

    if prediction == 1:
        investigation_output = run_investigation(
            str(sample["nameOrig"]), prediction,
            transaction_context={
                "step": int(sample["step"]), "type": str(sample["type"]),
                "amount": float(sample["amount"]), "nameDest": str(sample["nameDest"]),
            },
        )
        agent_latency_s = float(investigation_output.latency_s)

    final_status, anomaly_label, is_ml_anomaly, is_agent_blocked = classify_outcome(
        prediction, investigation_output
    )
    ground_truth = int(sample.get("isFraud", 0))
    timestamp = pd.Timestamp.now().strftime("%Y-%m-%d %H:%M:%S")

    log_entry = {
        "Time": timestamp, "Step": int(sample["step"]), "Type": str(sample["type"]),
        "User": str(sample["nameOrig"]), "Amount": float(sample["amount"]),
        "CatBoost": "Fraud" if prediction == 1 else "Clean", "Final Status": final_status,
    }
    st.session_state.log_history.insert(0, log_entry)
    st.session_state.log_history = st.session_state.log_history[:MAX_LOG_ROWS]

    chart_entry = {
        "time": timestamp, "step": int(sample["step"]), "type": str(sample["type"]),
        "anomaly_label": anomaly_label, "is_ml_anomaly": is_ml_anomaly,
        "is_agent_blocked": is_agent_blocked,
        "risk_score": float(investigation_output.risk_score) if investigation_output else 0.0,
    }
    st.session_state.chart_history.append(chart_entry)
    st.session_state.chart_history = st.session_state.chart_history[-MAX_CHART_ROWS:]

    if is_ml_anomaly:
        st.session_state.ml_anomaly_count += 1
    if is_agent_blocked:
        st.session_state.agent_block_count += 1
    if final_status == "ML_OVERRIDDEN_ALLOW":
        st.session_state.override_count += 1

    total_latency_ms = ml_latency_ms + (agent_latency_s * 1000.0)

    if prediction == 1 and investigation_output:
        investigation_output_dict = investigation_output.to_dict()
        reason_code, reason_tags = _derive_reason_metadata(investigation_output)
        event_id = (
            f"EVT_{int(time.time() * 1000)}_{idx}_{str(sample['nameOrig'])}"
            .replace(" ", "_").replace("|", "_")
        )
        control_payload = {}
        control_ack = {}
        control_status = "not_required"
        is_held_for_approval = final_status == "HELD_PENDING_APPROVAL"
        if is_held_for_approval:
            control_payload, control_ack, control_status = issue_hold_command(
                event_id=event_id, sample=sample,
                investigation_output=investigation_output, reason_code=reason_code,
            )
        investigation_event = {
            "event_id": event_id, "event_time": timestamp, "stream_index": int(idx),
            "step": int(sample["step"]), "nameOrig": str(sample["nameOrig"]),
            "nameDest": str(sample["nameDest"]), "txn_type": str(sample["type"]),
            "amount": float(sample["amount"]), "ml_prediction": prediction,
            "ground_truth": ground_truth, "final_status": final_status,
            "final_verdict": investigation_output.final_verdict,
            "ml_correction": investigation_output.ml_correction,
            "confidence_score": investigation_output.confidence_score,
            "risk_score": investigation_output.risk_score,
            "fallback_used": investigation_output.fallback_used,
            "reasoning": investigation_output.reasoning,
            "reason_code": reason_code,
            "reason_tags_json": _safe_json_text({"reason_tags": reason_tags}),
            "control_action": "hold" if is_held_for_approval else investigation_output.control_action,
            "control_status": control_status,
            "control_command_json": _safe_json_text(control_payload),
            "control_ack_json": _safe_json_text(control_ack),
            "approval_status": "pending_approval" if is_held_for_approval else "not_required",
            "approval_action": "", "approval_time": "", "approval_note": "", "reviewed_by": "",
            "release_command_json": "", "release_ack_json": "",
            "evidence_json": _safe_json_text({"evidence": investigation_output.evidence}),
            "tool_trace_json": _safe_json_text({"tool_trace": investigation_output.tool_trace}),
            "agent_latency_s": round(agent_latency_s, 4),
            "ml_latency_ms": round(ml_latency_ms, 3),
            "total_latency_ms": round(total_latency_ms, 3),
            "raw_investigation_json": _safe_json_text(investigation_output_dict),
        }
        persist_investigation_event(investigation_event)
        st.session_state.investigation_event_log.insert(0, investigation_event)
        st.session_state.investigation_event_log = st.session_state.investigation_event_log[:MAX_INVESTIGATION_EVENT_ROWS]
        st.session_state.latest_event_id = event_id

    st.session_state.last_ml_latency_ms = ml_latency_ms
    st.session_state.last_agent_latency_s = agent_latency_s
    st.session_state.last_total_latency_ms = total_latency_ms
    st.session_state.stream_index += 1


# ─── Chart helpers ────────────────────────────────────────────────────────────

_CHART_COLORS = {
    "Clean": "#3B82F6",
    "ML Fraud": "#F97316",
    "Held Pending Approval": "#EF4444",
    "Agent Blocked": "#EF4444",
}

_CHART_BG = "rgba(0,0,0,0)"
_CHART_PAPER_BG = "rgba(0,0,0,0)"
_CHART_FONT_COLOR = "#94A3B8"
_CHART_GRID_COLOR = "rgba(148,163,184,0.10)"


def _apply_chart_theme(fig: go.Figure, title: str = "") -> go.Figure:
    fig.update_layout(
        title=dict(text=title, font=dict(size=13, color="#CBD5E1"), x=0),
        paper_bgcolor=_CHART_PAPER_BG,
        plot_bgcolor=_CHART_BG,
        font=dict(family="'DM Sans', sans-serif", color=_CHART_FONT_COLOR, size=11),
        margin=dict(l=8, r=8, t=36, b=8),
        legend=dict(
            bgcolor="rgba(15,23,42,0.55)",
            bordercolor="rgba(148,163,184,0.15)",
            borderwidth=1,
            font=dict(color="#94A3B8", size=10),
        ),
        xaxis=dict(
            gridcolor=_CHART_GRID_COLOR, linecolor="rgba(148,163,184,0.15)",
            tickcolor="rgba(0,0,0,0)", tickfont=dict(color="#64748B"),
        ),
        yaxis=dict(
            gridcolor=_CHART_GRID_COLOR, linecolor="rgba(148,163,184,0.15)",
            tickcolor="rgba(0,0,0,0)", tickfont=dict(color="#64748B"),
        ),
    )
    return fig


def render_graphs() -> None:
    if not st.session_state.chart_history:
        st.info("No stream data yet — start the live stream to see detection results.")
        return

    chart_df = pd.DataFrame(st.session_state.chart_history)
    if "risk_score" not in chart_df.columns:
        chart_df["risk_score"] = (
            chart_df.get("anomaly_label", pd.Series(dtype=str))
            .map({"Held Pending Approval": 90.0, "Agent Blocked": 90.0, "ML Fraud": 65.0, "Clean": 5.0})
            .fillna(0.0)
        )
    else:
        chart_df["risk_score"] = pd.to_numeric(chart_df["risk_score"], errors="coerce").fillna(0.0)

    grouped = (
        chart_df.groupby(["type", "anomaly_label"], as_index=False)
        .size()
        .rename(columns={"size": "count"})
    )
    order = ["Clean", "ML Fraud", "Held Pending Approval"]
    fig = px.bar(
        grouped, x="type", y="count", color="anomaly_label", barmode="stack",
        category_orders={"anomaly_label": order}, color_discrete_map=_CHART_COLORS,
    )
    fig.update_traces(marker_line_width=0)
    _apply_chart_theme(fig, "Transaction Type vs Classification Layer")
    fig.update_layout(legend_title_text="", bargap=0.35)
    st.plotly_chart(fig, use_container_width=True, config={"displayModeBar": False}, key="chart_txn_type_class")

    timeline_df = chart_df.copy()
    timeline_df["step"] = pd.to_numeric(timeline_df["step"], errors="coerce")
    timeline_df = timeline_df.dropna(subset=["step"]).sort_values("step")
    if not timeline_df.empty:
        timeline = px.line(
            timeline_df, x="step", y="risk_score", markers=True,
        )
        timeline.update_traces(
            line=dict(color="#EF4444", width=2),
            marker=dict(color="#EF4444", size=5, line=dict(color="#1E293B", width=1.5)),
        )
        _apply_chart_theme(timeline, "Risk Score Timeline")
        st.plotly_chart(timeline, use_container_width=True, config={"displayModeBar": False}, key="chart_risk_timeline")


def render_decision_funnel() -> None:
    processed = st.session_state.stream_index
    ml_fraud = st.session_state.ml_anomaly_count
    held = st.session_state.agent_block_count
    overrides = st.session_state.override_count

    fig = go.Figure(go.Bar(
        x=["Processed", "ML Fraud", "Held", "Overrides"],
        y=[processed, ml_fraud, held, overrides],
        marker=dict(
            color=["#3B82F6", "#F97316", "#EF4444", "#22D3EE"],
            line=dict(width=0),
        ),
        width=0.55,
    ))
    _apply_chart_theme(fig, "Decision Funnel")
    fig.update_layout(bargap=0.4, showlegend=False)
    st.plotly_chart(fig, use_container_width=True, config={"displayModeBar": False}, key="chart_decision_funnel")


def render_chatbot() -> None:
    st.markdown(
        '<p class="section-label">DB Insights Chat</p>'
        '<p class="section-caption">Ask read-only analytics questions about the transactions table.</p>',
        unsafe_allow_html=True,
    )

    for msg in st.session_state.chat_history:
        if msg["role"] == "user":
            st.markdown(
                f'<div class="chat-bubble chat-user">{html.escape(msg["content"])}</div>',
                unsafe_allow_html=True,
            )
            continue
        st.markdown(
            f'<div class="chat-bubble chat-assistant">{html.escape(msg["content"])}</div>',
            unsafe_allow_html=True,
        )
        if msg.get("sql"):
            st.code(msg["sql"], language="sql")
        if msg.get("rows"):
            st.dataframe(pd.DataFrame(msg["rows"]), use_container_width=True)
        if msg.get("insight"):
            st.info(msg["insight"])
        if msg.get("safety_note"):
            st.caption(msg["safety_note"])
        if msg.get("error"):
            st.caption(f"Note: {msg['error']}")
        st.markdown('<div class="divider"></div>', unsafe_allow_html=True)

    suggestions = [""] + get_suggested_questions()
    selected_prompt = st.selectbox(
        "Suggested question",
        suggestions,
        format_func=lambda v: v or "Write your own question…",
        key="db_chat_suggestion",
    )

    with st.form("db_chat_form", clear_on_submit=True):
        prompt = st.text_input(
            "Database question",
            value=selected_prompt,
            placeholder="e.g. Show fraud count by transaction type",
        )
        submitted = st.form_submit_button("Ask DB Insights", use_container_width=True)

    if not submitted:
        return
    prompt = (prompt or "").strip()
    if not prompt:
        st.warning("Please enter a database question.")
        return

    st.session_state.chat_history.append({"role": "user", "content": prompt})
    with st.spinner("Querying the database…"):
        answer = answer_db_question(prompt)

    st.session_state.chat_history.append({
        "role": "assistant", "content": answer["answer"],
        "sql": answer.get("sql", ""), "rows": answer.get("rows", []),
        "insight": answer.get("insight", ""),
        "safety_note": answer.get("safety_note", ""),
        "error": answer.get("error"),
    })
    st.rerun()


def _humanize_tool_trace(trace_items: list) -> list[str]:
    readable_steps: list[str] = []
    for raw_item in trace_items:
        item = str(raw_item)
        lowered = item.lower()
        if "get_user_transaction_history" in lowered:
            sentence = "Reviewed the sender's recent transaction history to understand normal account behavior."
        elif "get_recipient_transaction_history" in lowered:
            sentence = "Reviewed the recipient's recent activity to check whether the destination account looked unusual."
        elif "detect_user_liquidation_pattern" in lowered:
            sentence = "Checked whether the sender showed a liquidation pattern (draining balance via transfers)."
        elif "detect_user_repetitive_payments" in lowered:
            sentence = "Checked whether the transaction resembled a repeated, consistent payment (possible false alarm)."
        elif "find_similar_transactions" in lowered:
            sentence = "Compared this transaction with similar historical transactions by type and amount."
        else:
            sentence = "Reviewed an additional transaction signal from the investigation workflow."

        if '"detected": true' in lowered or "'detected': true" in lowered:
            sentence += " Signal was present."
        elif '"detected": false' in lowered or "'detected': false" in lowered:
            sentence += " No strong signal found."
        elif "not found" in lowered:
            sentence += " No matching history found."
        elif "database error" in lowered:
            sentence += " Lookup could not be completed; decision used remaining context."

        if sentence not in readable_steps:
            readable_steps.append(sentence)
    return readable_steps


def _compact_display(value: object, decimals: int = 2) -> str:
    if value is None or str(value) == "nan":
        return "N/A"
    try:
        return f"{float(value):.{decimals}f}"
    except Exception:
        text = str(value)
        return text if len(text) <= 18 else f"{text[:15]}…"


# Status colour palette used in the summary tiles
_STATUS_COLORS = {
    "HELD_PENDING_APPROVAL": ("#FEF9C3", "#854D0E", "#FDE047"),   # amber
    "BLOCKED_CONFIRMED": ("#FEE2E2", "#991B1B", "#FCA5A5"),        # red
    "RELEASED_AFTER_APPROVAL": ("#DCFCE7", "#166534", "#86EFAC"),  # green
    "ML_OVERRIDDEN_ALLOW": ("#E0F2FE", "#0C4A6E", "#7DD3FC"),      # sky
    "ALLOW": ("#F0FDF4", "#166534", "#86EFAC"),                    # green-light
    "ESCALATED": ("#F3E8FF", "#6B21A8", "#D8B4FE"),                # purple
    "AGENT_ERROR": ("#FFF7ED", "#9A3412", "#FB923C"),              # orange
}

_VERDICT_PILL = {
    "BLOCK": ("bg-red", "BLOCK"),
    "ALLOW": ("bg-blue", "ALLOW"),
}


def _render_detail_summary(row: pd.Series) -> None:
    status = str(row.get("final_status") or "N/A")
    verdict = str(row.get("final_verdict") or "N/A").upper()
    risk_raw = row.get("risk_score")
    risk_val = float(risk_raw) if risk_raw is not None and str(risk_raw) != "nan" else None
    risk_display = f"{risk_val:.1f}" if risk_val is not None else "N/A"
    reason_code = str(row.get("reason_code") or "N/A")
    confidence_raw = row.get("confidence_score")
    confidence_display = f"{float(confidence_raw):.2f}" if confidence_raw is not None and str(confidence_raw) != "nan" else "N/A"

    # Risk bar width (0–100 clamped)
    risk_pct = min(max(risk_val or 0, 0), 100)
    risk_color = "#EF4444" if risk_pct >= 75 else "#F97316" if risk_pct >= 40 else "#3B82F6"

    status_styles = _STATUS_COLORS.get(status, ("#F8FAFC", "#1E293B", "#94A3B8"))
    bg, text_col, accent = status_styles

    verdict_pill_color = "#EF4444" if verdict == "BLOCK" else "#3B82F6" if verdict == "ALLOW" else "#64748B"

    st.markdown(
        f"""
        <div class="summary-grid">
            <div class="s-tile" style="background:{bg}; border-color:{accent}40;">
                <span class="s-label">STATUS</span>
                <strong class="s-value" style="color:{text_col}; font-size:0.78rem;">{html.escape(status)}</strong>
            </div>
            <div class="s-tile">
                <span class="s-label">VERDICT</span>
                <span class="verdict-pill" style="background:{verdict_pill_color}20; color:{verdict_pill_color}; border:1px solid {verdict_pill_color}40;">
                    {html.escape(verdict)}
                </span>
            </div>
            <div class="s-tile">
                <span class="s-label">RISK SCORE</span>
                <strong class="s-value">{html.escape(risk_display)}</strong>
                <div class="risk-bar-track">
                    <div class="risk-bar-fill" style="width:{risk_pct}%; background:{risk_color};"></div>
                </div>
            </div>
            <div class="s-tile">
                <span class="s-label">REASON CODE</span>
                <strong class="s-value" style="font-size:0.75rem;">{html.escape(reason_code)}</strong>
            </div>
            <div class="s-tile">
                <span class="s-label">CONFIDENCE</span>
                <strong class="s-value">{html.escape(confidence_display)}</strong>
            </div>
        </div>
        """,
        unsafe_allow_html=True,
    )



# ─── Visual theme ─────────────────────────────────────────────────────────────

def render_visual_theme() -> None:
    st.markdown(
        """
        <style>
        /* ── Google Fonts ────────────────────────────────────────────── */
        @import url('https://fonts.googleapis.com/css2?family=DM+Sans:wght@300;400;500;600;700&family=DM+Mono:wght@400;500&display=swap');

        /* ── CSS Custom Properties (light + dark adaptive) ──────────── */
        :root {
            --bg-primary:    #0F172A;
            --bg-secondary:  #1E293B;
            --bg-card:       #1E293B;
            --bg-card-hover: #273548;
            --border:        rgba(148,163,184,0.12);
            --border-accent: rgba(59,130,246,0.35);
            --text-primary:  #F1F5F9;
            --text-secondary:#94A3B8;
            --text-muted:    #64748B;
            --accent-blue:   #3B82F6;
            --accent-orange: #F97316;
            --accent-red:    #EF4444;
            --accent-teal:   #22D3EE;
            --accent-green:  #22C55E;
            --shadow-sm:     0 1px 3px rgba(0,0,0,0.4);
            --shadow-md:     0 4px 16px rgba(0,0,0,0.45);
            --radius:        10px;
            --radius-sm:     6px;
            --font-main:     'DM Sans', sans-serif;
            --font-mono:     'DM Mono', monospace;
        }

        /* Force dark background regardless of Streamlit theme */
        .stApp {
            background: var(--bg-primary) !important;
            font-family: var(--font-main) !important;
        }

        /* Remove any light default gradients Streamlit applies */
        [data-testid="stAppViewContainer"],
        [data-testid="stHeader"],
        section.main {
            background: var(--bg-primary) !important;
        }

        /* Sidebar (if ever shown) */
        [data-testid="stSidebar"] {
            background: var(--bg-secondary) !important;
            border-right: 1px solid var(--border);
        }

        /* ── Block layout ───────────────────────────────────────────── */
        .block-container {
            padding: 1.5rem 2rem 3rem !important;
            max-width: 1600px;
        }

        /* ── Typography ─────────────────────────────────────────────── */
        h1, h2, h3, h4, h5, h6,
        .stMarkdown h1, .stMarkdown h2, .stMarkdown h3 {
            font-family: var(--font-main) !important;
            color: var(--text-primary) !important;
            letter-spacing: -0.02em;
        }
        h1 { font-size: 1.65rem !important; font-weight: 700 !important; }
        h2 { font-size: 1.2rem  !important; font-weight: 600 !important; }
        h3 { font-size: 1.05rem !important; font-weight: 600 !important; }

        p, li, span, label, div {
            font-family: var(--font-main) !important;
        }

        /* Caption / muted text */
        .stCaption, small, .stMarkdown small {
            color: var(--text-muted) !important;
            font-size: 0.78rem !important;
        }

        /* ── Metric cards ───────────────────────────────────────────── */
        [data-testid="stMetric"] {
            background: var(--bg-card) !important;
            border: 1px solid var(--border) !important;
            border-radius: var(--radius) !important;
            padding: 0.85rem 1rem !important;
            box-shadow: var(--shadow-sm);
            transition: border-color 0.2s;
        }
        [data-testid="stMetric"]:hover {
            border-color: var(--border-accent) !important;
        }
        [data-testid="stMetricLabel"] {
            color: var(--text-muted) !important;
            font-size: 0.72rem !important;
            font-weight: 500 !important;
            letter-spacing: 0.08em !important;
            text-transform: uppercase;
        }
        [data-testid="stMetricValue"] {
            color: var(--text-primary) !important;
            font-size: 1.55rem !important;
            font-weight: 700 !important;
        }
        [data-testid="stMetricDelta"] svg { display: none; }

        /* ── Buttons ────────────────────────────────────────────────── */
        .stButton > button {
            font-family: var(--font-main) !important;
            font-weight: 500 !important;
            font-size: 0.82rem !important;
            letter-spacing: 0.01em;
            border-radius: var(--radius-sm) !important;
            border: 1px solid var(--border) !important;
            background: var(--bg-card) !important;
            color: var(--text-secondary) !important;
            padding: 0.45rem 1rem !important;
            transition: all 0.18s ease !important;
            box-shadow: var(--shadow-sm) !important;
        }
        .stButton > button:hover {
            background: var(--bg-card-hover) !important;
            border-color: var(--accent-blue) !important;
            color: var(--text-primary) !important;
            box-shadow: 0 0 0 2px rgba(59,130,246,0.18) !important;
        }
        .stButton > button:active {
            transform: scale(0.98);
        }
        /* Primary (Submit Decision) */
        .stButton > button[kind="primary"] {
            background: var(--accent-blue) !important;
            border-color: var(--accent-blue) !important;
            color: #fff !important;
        }
        .stButton > button[kind="primary"]:hover {
            background: #2563EB !important;
            box-shadow: 0 0 0 3px rgba(59,130,246,0.25) !important;
        }

        /* ── Form submit button ─────────────────────────────────────── */
        [data-testid="stFormSubmitButton"] > button {
            background: var(--accent-blue) !important;
            border-color: var(--accent-blue) !important;
            color: #fff !important;
            font-weight: 600 !important;
        }
        [data-testid="stFormSubmitButton"] > button:hover {
            background: #2563EB !important;
        }

        /* ── Inputs ─────────────────────────────────────────────────── */
        .stTextInput > div > div > input,
        .stTextArea > div > div > textarea,
        .stSelectbox > div > div {
            background: var(--bg-secondary) !important;
            border: 1px solid var(--border) !important;
            border-radius: var(--radius-sm) !important;
            color: var(--text-primary) !important;
            font-family: var(--font-main) !important;
            font-size: 0.85rem !important;
        }
        .stTextInput > div > div > input:focus,
        .stTextArea > div > div > textarea:focus {
            border-color: var(--accent-blue) !important;
            box-shadow: 0 0 0 3px rgba(59,130,246,0.15) !important;
            outline: none !important;
        }

        /* Input labels */
        .stTextInput label, .stTextArea label,
        .stSelectbox label, .stMultiSelect label {
            color: var(--text-secondary) !important;
            font-size: 0.78rem !important;
            font-weight: 500 !important;
            letter-spacing: 0.04em;
            text-transform: uppercase;
        }

        /* ── Selectbox dropdown ─────────────────────────────────────── */
        [data-baseweb="select"] > div {
            background: var(--bg-secondary) !important;
            border-color: var(--border) !important;
            border-radius: var(--radius-sm) !important;
        }
        [data-baseweb="popover"] {
            background: var(--bg-secondary) !important;
            border: 1px solid var(--border) !important;
            border-radius: var(--radius) !important;
        }
        [data-baseweb="menu"] li {
            color: var(--text-secondary) !important;
            font-size: 0.83rem !important;
        }
        [data-baseweb="menu"] li:hover {
            background: var(--bg-card-hover) !important;
            color: var(--text-primary) !important;
        }

        /* ── Dataframes / Tables ────────────────────────────────────── */
        [data-testid="stDataFrame"] {
            border-radius: var(--radius) !important;
            border: 1px solid var(--border) !important;
            overflow: hidden;
        }
        [data-testid="stDataFrame"] iframe {
            border-radius: var(--radius) !important;
        }

        /* ── Segmented control (tab selector) ───────────────────────── */
        [data-testid="stSegmentedControl"] {
            background: var(--bg-secondary) !important;
            border: 1px solid var(--border) !important;
            border-radius: var(--radius) !important;
            padding: 3px !important;
        }
        [data-testid="stSegmentedControl"] button {
            border-radius: var(--radius-sm) !important;
            color: var(--text-muted) !important;
            font-size: 0.82rem !important;
            font-weight: 500 !important;
            transition: all 0.15s !important;
        }
        [data-testid="stSegmentedControl"] button[aria-selected="true"] {
            background: var(--accent-blue) !important;
            color: #fff !important;
        }

        /* ── Tabs (investigation panel) ─────────────────────────────── */
        .stTabs [data-baseweb="tab-list"] {
            background: var(--bg-secondary) !important;
            border-radius: var(--radius) !important;
            border: 1px solid var(--border) !important;
            padding: 3px !important;
            gap: 0 !important;
        }
        .stTabs [data-baseweb="tab"] {
            background: transparent !important;
            color: var(--text-muted) !important;
            font-size: 0.82rem !important;
            font-weight: 500 !important;
            border-radius: var(--radius-sm) !important;
            padding: 0.45rem 1.2rem !important;
            border: none !important;
            transition: all 0.15s ease !important;
        }
        .stTabs [data-baseweb="tab"][aria-selected="true"] {
            background: var(--accent-blue) !important;
            color: #fff !important;
        }
        .stTabs [data-baseweb="tab"]:hover:not([aria-selected="true"]) {
            color: var(--text-secondary) !important;
            background: var(--bg-card-hover) !important;
        }
        .stTabs [data-baseweb="tab-highlight"] {
            display: none !important;
        }
        .stTabs [data-baseweb="tab-border"] {
            display: none !important;
        }

        /* ── Alert / info / success boxes ───────────────────────────── */
        [data-testid="stAlert"] {
            border-radius: var(--radius) !important;
            border-width: 1px !important;
            font-size: 0.83rem !important;
        }

        /* ── Download buttons ───────────────────────────────────────── */
        [data-testid="stDownloadButton"] > button {
            background: var(--bg-secondary) !important;
            border: 1px solid var(--border) !important;
            color: var(--text-secondary) !important;
            font-size: 0.8rem !important;
            border-radius: var(--radius-sm) !important;
        }
        [data-testid="stDownloadButton"] > button:hover {
            border-color: var(--accent-teal) !important;
            color: var(--accent-teal) !important;
        }

        /* ── Code blocks ─────────────────────────────────────────────── */
        .stCodeBlock, pre, code {
            background: #0D1526 !important;
            border: 1px solid var(--border) !important;
            border-radius: var(--radius-sm) !important;
            font-family: var(--font-mono) !important;
            font-size: 0.8rem !important;
            color: #7DD3FC !important;
        }

        /* ── Spinner ─────────────────────────────────────────────────── */
        [data-testid="stSpinner"] {
            color: var(--accent-blue) !important;
        }

        /* ── Divider ────────────────────────────────────────────────── */
        hr {
            border-color: var(--border) !important;
            margin: 1rem 0 !important;
        }

        /* ── Fade artifact fix — suppress ALL rerun/fragment flash ──── */
        .element-container { opacity: 1 !important; }
        [data-testid="stVerticalBlockBorderWrapper"],
        .stPlotlyChart,
        [data-testid="stMetric"] {
            opacity: 1 !important;
            transition: none !important;
            animation: none !important;
        }
        /* Disable Streamlit skeleton shimmer on fragment reruns */
        .stale-element { opacity: 1 !important; }
        [data-stale="true"] { opacity: 1 !important; }

        /* ══════════════════════════════════════════════════════════════
           CUSTOM COMPONENTS
        ══════════════════════════════════════════════════════════════ */

        /* Section label */
        .section-label {
            color: var(--text-secondary) !important;
            font-size: 0.72rem !important;
            font-weight: 600 !important;
            letter-spacing: 0.1em;
            text-transform: uppercase;
            margin: 0 0 4px !important;
        }
        .section-caption {
            color: var(--text-muted) !important;
            font-size: 0.78rem !important;
            margin: 0 0 12px !important;
        }
        .sub-label {
            color: var(--text-muted) !important;
            font-size: 0.72rem !important;
            font-weight: 600 !important;
            letter-spacing: 0.08em;
            text-transform: uppercase;
            margin: 10px 0 4px !important;
        }
        .divider {
            border-top: 1px solid var(--border);
            margin: 10px 0;
        }

        /* ── Investigation summary grid ─────────────────────────────── */
        .summary-grid {
            display: grid;
            grid-template-columns: repeat(5, 1fr);
            gap: 8px;
            margin: 10px 0 14px;
        }
        .s-tile {
            background: var(--bg-card);
            border: 1px solid var(--border);
            border-radius: var(--radius);
            padding: 10px 12px;
            min-width: 0;
            box-shadow: var(--shadow-sm);
            transition: border-color 0.2s;
        }
        .s-tile:hover { border-color: var(--border-accent); }
        .s-label {
            display: block;
            color: var(--text-muted);
            font-size: 0.65rem;
            font-weight: 600;
            letter-spacing: 0.1em;
            text-transform: uppercase;
            margin-bottom: 5px;
        }
        .s-value {
            display: block;
            color: var(--text-primary);
            font-size: 0.92rem;
            font-weight: 600;
            white-space: nowrap;
            overflow: hidden;
            text-overflow: ellipsis;
        }

        /* Verdict pill */
        .verdict-pill {
            display: inline-block;
            padding: 2px 10px;
            border-radius: 20px;
            font-size: 0.72rem;
            font-weight: 700;
            letter-spacing: 0.08em;
            margin-top: 2px;
        }

        /* Risk bar */
        .risk-bar-track {
            height: 4px;
            background: rgba(148,163,184,0.12);
            border-radius: 4px;
            margin-top: 8px;
            overflow: hidden;
        }
        .risk-bar-fill {
            height: 100%;
            border-radius: 4px;
            transition: width 0.4s ease;
        }

        /* ── Reasoning block ────────────────────────────────────────── */
        .reasoning-block {
            background: rgba(15, 23, 42, 0.6);
            border-left: 3px solid var(--accent-blue);
            border-radius: 0 var(--radius-sm) var(--radius-sm) 0;
            padding: 10px 14px;
            color: var(--text-secondary);
            font-size: 0.83rem;
            line-height: 1.55;
            margin: 8px 0 10px;
        }

        /* ── Evidence / trace items ─────────────────────────────────── */
        .evidence-item, .trace-item {
            padding: 5px 10px;
            color: var(--text-secondary);
            font-size: 0.8rem;
            line-height: 1.5;
            border-left: 2px solid var(--border);
            margin: 3px 0;
        }
        .trace-item { color: var(--text-muted); }

        /* ── Tag pills ──────────────────────────────────────────────── */
        .tag-row { display: flex; flex-wrap: wrap; gap: 6px; margin: 6px 0 10px; }
        .tag-pill {
            background: rgba(59,130,246,0.12);
            border: 1px solid rgba(59,130,246,0.25);
            color: #7DD3FC;
            border-radius: 20px;
            font-size: 0.68rem;
            font-weight: 500;
            padding: 2px 10px;
            letter-spacing: 0.04em;
        }

        /* ── Analyst panel ───────────────────────────────────────────── */
        .analyst-panel {
            background: var(--bg-secondary);
            border: 1px solid var(--border);
            border-radius: var(--radius);
            padding: 14px 16px;
            margin: 14px 0 10px;
        }

        /* ── Chat bubbles ────────────────────────────────────────────── */
        .chat-bubble {
            padding: 10px 14px;
            border-radius: var(--radius);
            font-size: 0.84rem;
            line-height: 1.55;
            margin: 6px 0;
        }
        .chat-user {
            background: rgba(59,130,246,0.12);
            border: 1px solid rgba(59,130,246,0.2);
            color: #93C5FD;
            text-align: right;
        }
        .chat-assistant {
            background: var(--bg-card);
            border: 1px solid var(--border);
            color: var(--text-secondary);
        }

        /* ── Responsive ─────────────────────────────────────────────── */
        @media (max-width: 768px) {
            .summary-grid { grid-template-columns: repeat(2, 1fr); }
            .block-container { padding: 1rem !important; }
        }
        </style>
        """,
        unsafe_allow_html=True,
    )


# ─── Monitoring fragment ───────────────────────────────────────────────────────

def render_live_monitoring_fragment(model: CatBoostClassifier, raw_data: pd.DataFrame) -> None:
    """Lightweight fragment that auto-refreshes: metrics, charts, stream log only."""
    if st.session_state.stream_running:
        process_next_transaction(model, raw_data)

    total = len(raw_data)
    idx = st.session_state.stream_index
    pct = int(idx / total * 100) if total else 0
    st.caption(f"Rows processed: {idx} / {total}  ({pct}%)")

    if st.session_state.stream_done_message:
        st.success(st.session_state.stream_done_message)

    met1, met2, met3, met4, met5 = st.columns(5)
    met1.metric("Processed", st.session_state.stream_index)
    met2.metric("ML Anomalies", st.session_state.ml_anomaly_count)
    met3.metric("Held for Review", st.session_state.agent_block_count)
    met4.metric("ML Overrides", st.session_state.override_count)
    met5.metric("Total Latency", f"{st.session_state.last_total_latency_ms:.1f} ms")

    chart_col, funnel_col = st.columns([1.6, 1])
    with chart_col:
        render_graphs()
    with funnel_col:
        render_decision_funnel()
        if st.session_state.log_history:
            st.markdown('<p class="section-label" style="margin-top:1rem;">Recent Stream Events</p>', unsafe_allow_html=True)
            log_df = pd.DataFrame(st.session_state.log_history)
            log_df["Amount"] = log_df["Amount"].map(lambda v: f"${v:,.2f}")
            st.dataframe(log_df, use_container_width=True, height=260)
        else:
            st.info("No transactions processed yet. Start the stream.")


def render_investigation_panel() -> None:
    """Separate section for investigation detail — not auto-refreshed."""
    st.markdown('<div class="divider"></div>', unsafe_allow_html=True)
    tab_events, tab_detail = st.tabs(["📋 Investigation Events", "🔍 Investigation Detail"])

    with tab_events:
        _render_investigation_events_tab()

    with tab_detail:
        _render_investigation_detail_tab()


def _render_investigation_events_tab() -> None:
    """Events table + export buttons."""
    event_df = fetch_recent_audit_events(limit=400)
    if event_df.empty:
        st.info("No investigation events yet. Fraud-flagged transactions will appear here.")
        return

    visible_columns = [
        "event_time", "nameOrig", "nameDest", "txn_type", "amount",
        "final_status", "final_verdict", "approval_status", "approval_action",
        "reason_code", "control_action", "control_status", "risk_score",
        "reasoning", "ml_correction", "confidence_score", "agent_latency_s", "fallback_used",
    ]
    visible_columns = [c for c in visible_columns if c in event_df.columns]
    event_display_df = event_df.copy()
    if "amount" in event_display_df.columns:
        event_display_df["amount"] = event_display_df["amount"].map(lambda v: f"${v:,.2f}")
    if "reasoning" in event_display_df.columns:
        event_display_df["reasoning"] = event_display_df["reasoning"].fillna("").map(
            lambda t: (str(t)[:80] + "…") if len(str(t)) > 80 else str(t)
        )
    if "reason_tags_json" in event_display_df.columns:
        def _render_reason_tags(tags_json: str) -> str:
            try:
                payload = json.loads(str(tags_json or "{}"))
                tags = payload.get("reason_tags", [])
                if isinstance(tags, list):
                    return ", ".join([str(t) for t in tags[:4]])
            except Exception:
                pass
            return ""
        event_display_df["reason_tags"] = event_display_df["reason_tags_json"].map(_render_reason_tags)
        if "reason_tags" not in visible_columns:
            visible_columns.append("reason_tags")

    st.dataframe(event_display_df[visible_columns], use_container_width=True, height=360)

    # Export buttons
    export_ts = pd.Timestamp.now().strftime("%Y%m%d_%H%M%S")
    dl1, dl2, _ = st.columns([1, 1, 3])
    dl1.download_button(
        "⬇ CSV Export", data=event_df.to_csv(index=False).encode("utf-8"),
        file_name=f"event_log_{export_ts}.csv", mime="text/csv", use_container_width=True,
    )
    dl2.download_button(
        "⬇ JSON Export", data=event_df.to_json(orient="records", indent=2),
        file_name=f"event_log_{export_ts}.json", mime="application/json", use_container_width=True,
    )


def _render_investigation_detail_tab() -> None:
    """Detail view + analyst review for a selected investigation."""
    event_df = fetch_recent_audit_events(limit=400)
    if event_df.empty:
        st.info("No investigation events to inspect.")
        return

    pending_df = event_df[event_df["final_status"].astype(str) == "HELD_PENDING_APPROVAL"]
    details_df = (
        pd.concat([pending_df, event_df.head(10)], ignore_index=True)
        .drop_duplicates(subset=["event_id"]).head(30).copy()
    )
    details_df["event_id"] = details_df["event_id"].astype(str)
    event_ids = details_df["event_id"].tolist()

    if st.session_state.get("selected_investigation_event_id") not in event_ids:
        st.session_state.pop("selected_investigation_event_id", None)
        default_index = 0
    else:
        default_index = event_ids.index(st.session_state["selected_investigation_event_id"])

    label_lookup = {}
    for _, row in details_df.reset_index(drop=True).iterrows():
        event_time = str(row.get("event_time", ""))
        event_time = event_time[-8:] if len(event_time) >= 8 else event_time
        verdict = str(row.get("final_verdict") or "N/A").upper()
        status = str(row.get("final_status") or "N/A")
        label_lookup[str(row.get("event_id"))] = (
            f"{row.get('nameOrig', 'N/A')} · {event_time} · {verdict} · {status}"
        )

    selected_event_id = st.selectbox(
        "Select investigation",
        options=event_ids,
        index=default_index,
        format_func=lambda eid: label_lookup.get(str(eid), str(eid)),
        key="selected_investigation_event_id",
        accept_new_options=False,
    )
    selected_event_id = str(selected_event_id or event_ids[default_index])
    row = details_df[details_df["event_id"] == selected_event_id].iloc[0]

    if st.session_state.get("analyst_selected_event_id") != selected_event_id:
        st.session_state["analyst_selected_event_id"] = selected_event_id
        st.session_state["analyst_action_inline"] = "APPROVE_RELEASE"
        st.session_state["analyst_note_inline"] = ""

    _render_detail_summary(row)

    # Reason tags
    try:
        tags_payload = json.loads(str(row.get("reason_tags_json") or "{}"))
        tags = tags_payload.get("reason_tags", [])
        if tags:
            pills_html = "".join(
                f'<span class="tag-pill">{html.escape(str(t))}</span>' for t in tags
            )
            st.markdown(f'<div class="tag-row">{pills_html}</div>', unsafe_allow_html=True)
    except Exception:
        pass

    # Layout: reasoning + evidence on left, analyst review on right
    detail_left, detail_right = st.columns([1.5, 1])

    with detail_left:
        # Reasoning
        reasoning_text = row.get("reasoning") or "No reasoning provided."
        st.markdown(
            f'<div class="reasoning-block">{html.escape(str(reasoning_text))}</div>',
            unsafe_allow_html=True,
        )

        # Evidence
        try:
            evidence_payload = json.loads(str(row.get("evidence_json") or "{}"))
            evidence = evidence_payload.get("evidence", [])
            if evidence:
                st.markdown('<p class="sub-label">Evidence</p>', unsafe_allow_html=True)
                for item in evidence:
                    st.markdown(
                        f'<div class="evidence-item">› {html.escape(str(item))}</div>',
                        unsafe_allow_html=True,
                    )
        except Exception:
            pass

        # Tool trace
        try:
            trace_payload = json.loads(str(row.get("tool_trace_json") or "{}"))
            trace = trace_payload.get("tool_trace", [])
            if trace:
                st.markdown('<p class="sub-label">Investigation Steps</p>', unsafe_allow_html=True)
                for step in _humanize_tool_trace(trace):
                    st.markdown(
                        f'<div class="trace-item">⬡ {html.escape(step)}</div>',
                        unsafe_allow_html=True,
                    )
        except Exception:
            pass

    with detail_right:
        # Analyst review section
        st.markdown('<div class="analyst-panel">', unsafe_allow_html=True)
        st.markdown('<p class="section-label">Analyst Review</p>', unsafe_allow_html=True)
        st.caption(f"Event: {selected_event_id}")

        review_success_message = st.session_state.pop("analyst_review_success", "")
        if review_success_message:
            st.success(review_success_message)

        selected_verdict = str(row.get("final_verdict") or "").upper()
        final_status = str(row.get("final_status") or "").upper()

        if selected_verdict == "BLOCK" and final_status == "HELD_PENDING_APPROVAL":
            valid_actions = {"APPROVE_RELEASE", "CONFIRM_BLOCK", "ESCALATE"}
            if st.session_state.get("analyst_action_inline") not in valid_actions:
                st.session_state["analyst_action_inline"] = "APPROVE_RELEASE"
            st.selectbox(
                "Analyst Action",
                ["APPROVE_RELEASE", "CONFIRM_BLOCK", "ESCALATE"],
                key="analyst_action_inline",
            )
            st.text_area(
                "Analyst Note",
                placeholder="Manual review note…",
                key="analyst_note_inline",
                height=90,
            )
            if st.button("Submit Decision", use_container_width=True, key="save_analyst_review_inline"):
                analyst_action = str(st.session_state.get("analyst_action_inline", "APPROVE_RELEASE"))
                analyst_note = str(st.session_state.get("analyst_note_inline", ""))
                persist_analyst_review(selected_event_id, analyst_action, analyst_note)
                st.session_state["analyst_review_success"] = apply_analyst_approval(row, analyst_action, analyst_note)
                st.rerun()
        elif selected_verdict == "BLOCK":
            approval_status = row.get("approval_status") or "reviewed"
            approval_time = row.get("approval_time") or "N/A"
            reviewed_by = row.get("reviewed_by") or DEFAULT_REVIEWER
            st.success(
                f"Decision completed: **{final_status or approval_status}**.  "
                f"Reviewed by `{reviewed_by}` at {approval_time}."
            )
        else:
            st.info("Analyst review is available only for held investigations.")

        st.markdown('</div>', unsafe_allow_html=True)

        # Review log
        reviews_df = fetch_recent_reviews(limit=20)
        if reviews_df.empty:
            st.caption("No analyst reviews saved yet.")
        else:
            st.dataframe(reviews_df, use_container_width=True, height=200)


def render_live_monitoring(model: CatBoostClassifier, raw_data: pd.DataFrame) -> None:
    # Auto-refreshing fragment: metrics + charts only (fast refresh)
    run_every = STREAM_SLEEP_SECONDS if st.session_state.stream_running else None
    st.fragment(render_live_monitoring_fragment, run_every=run_every)(model, raw_data)

    # Investigation panel: separate slower fragment (refreshes every 3s when streaming)
    investigation_run_every = 3.0 if st.session_state.stream_running else None
    st.fragment(render_investigation_panel, run_every=investigation_run_every)()


# ─── Main ─────────────────────────────────────────────────────────────────────

def main() -> None:
    st.set_page_config(
        page_title="AGORA · Risk Command Center",
        page_icon="🛡️",
        layout="wide",
        initial_sidebar_state="collapsed",
    )
    render_visual_theme()
    init_audit_tables()
    ensure_fresh_boot_state()

    # Header
    st.markdown(
        """
        <div style="display:flex; align-items:center; gap:12px; margin-bottom:0.2rem;">
            <span style="font-size:1.6rem;">🛡️</span>
            <div>
                <h1 style="margin:0; line-height:1.1;">AGORA Risk Command Center</h1>
                <p style="color:var(--text-muted); font-size:0.78rem; margin:2px 0 0;">
                    CatBoost ML detection · Agent investigation · Analyst review workflow
                </p>
            </div>
        </div>
        """,
        unsafe_allow_html=True,
    )
    st.markdown('<div class="divider"></div>', unsafe_allow_html=True)

    init_state()
    model, raw_data = load_assets()

    # Control bar
    ctrl1, ctrl2, ctrl3, ctrl4, _, _ = st.columns([1, 1, 1.2, 1.4, 0.5, 0.5])
    if ctrl1.button("▶ Start Stream", use_container_width=True):
        st.session_state.stream_running = True
        st.session_state.stream_done_message = ""
    if ctrl2.button("⏹ Stop Stream", use_container_width=True):
        st.session_state.stream_running = False
    if ctrl3.button("↺ Reset Fresh", use_container_width=True):
        hard_reset_application(clear_audit=True)
        st.rerun()
    if ctrl4.button("⟳ Refresh Session", use_container_width=True):
        refresh_application_state(clear_audit=True)

    active_view = st.segmented_control(
        "View",
        ["Live Monitoring", "DB Insights Chat"],
        default="Live Monitoring",
        key="active_view",
    )

    if active_view == "Live Monitoring":
        render_live_monitoring(model, raw_data)
    else:
        render_chatbot()


if __name__ == "__main__":
    main()
import json
import os
import re
import sqlite3
import time
from dataclasses import asdict, dataclass, field
from typing import Any

import pandas as pd
from dotenv import load_dotenv
from langchain_core.tools import tool
from langchain_groq import ChatGroq

load_dotenv()

try:
    # LangChain <=0.x / 0.3 style API
    from langchain.agents import AgentExecutor, create_tool_calling_agent
    from langchain_core.prompts import ChatPromptTemplate, MessagesPlaceholder

    LANGCHAIN_AGENT_API = "legacy"
except ImportError:
    # LangChain >=1.x style API
    from langchain.agents import create_agent

    LANGCHAIN_AGENT_API = "modern"


GROQ_API_KEY_ENV = "GROQ_API_KEY"
GROQ_REQUEST_TIMEOUT_SEC = 30
ENABLE_OFFLINE_FALLBACK = True
DB_PATH = "agora_transactions.db"


@dataclass
class InvestigationResult:
    final_verdict: str
    ml_correction: bool
    reasoning: str
    confidence_score: float
    risk_score: float
    control_action: str = "none"
    control_destination: str = ""
    human_review_required: bool = False
    evidence: list[str] = field(default_factory=list)
    tool_trace: list[str] = field(default_factory=list)
    latency_s: float = 0.0
    fallback_used: bool = False
    raw_agent_output: str = ""
    error: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)

    def to_json(self) -> str:
        return json.dumps(self.to_dict(), ensure_ascii=False)


def _get_groq_api_key() -> str:
    key = os.getenv(GROQ_API_KEY_ENV, "").strip()
    if not key:
        raise EnvironmentError(
            f"{GROQ_API_KEY_ENV} environment variable is not set. "
            "Set it before running this script."
        )
    return key


def _content_to_text(content: Any) -> str:
    if isinstance(content, str):
        return content.strip()

    if isinstance(content, list):
        parts: list[str] = []
        for item in content:
            if isinstance(item, str):
                parts.append(item.strip())
            elif isinstance(item, dict):
                text = item.get("text")
                if text:
                    parts.append(str(text).strip())
        return "\n".join(p for p in parts if p).strip()

    if isinstance(content, dict):
        text = content.get("text")
        if text is not None:
            return str(text).strip()

    return str(content).strip()


def _extract_agent_output(response: Any) -> str:
    if isinstance(response, dict):
        output = response.get("output")
        if output:
            return _content_to_text(output)

        structured = response.get("structured_response")
        if structured is not None:
            if isinstance(structured, str):
                return structured
            return json.dumps(structured, ensure_ascii=False)

        messages = response.get("messages")
        if isinstance(messages, list):
            for message in reversed(messages):
                if isinstance(message, dict):
                    role = message.get("role")
                    content = message.get("content")
                else:
                    role = getattr(message, "type", None) or getattr(message, "role", None)
                    content = getattr(message, "content", None)

                if role in ("assistant", "ai", None):
                    text = _content_to_text(content)
                    if text:
                        return text

    raise RuntimeError("Agent finished without a readable output.")


def _extract_tool_trace(response: Any) -> list[str]:
    trace: list[str] = []
    if not isinstance(response, dict):
        return trace

    steps = response.get("intermediate_steps")
    if isinstance(steps, list):
        for step in steps:
            if not isinstance(step, (tuple, list)) or len(step) < 2:
                continue
            action, observation = step[0], step[1]
            tool_name = getattr(action, "tool", None) or "tool"
            tool_input = getattr(action, "tool_input", None)
            tool_input_txt = str(tool_input)[:120]
            obs_txt = str(observation).replace("\n", " ")[:180]
            trace.append(f"{tool_name}({tool_input_txt}) -> {obs_txt}")

    messages = response.get("messages")
    if isinstance(messages, list):
        for msg in messages:
            msg_type = getattr(msg, "type", None) or getattr(msg, "role", None)
            if msg_type != "tool":
                continue
            name = getattr(msg, "name", "tool")
            content = _content_to_text(getattr(msg, "content", ""))
            trace.append(f"{name} -> {content[:180]}")

    deduped: list[str] = []
    seen: set[str] = set()
    for item in trace:
        if item in seen:
            continue
        seen.add(item)
        deduped.append(item)
    return deduped[:12]


def _load_user_history_df(nameOrig: str, limit: int = 25) -> pd.DataFrame:
    query = """
    SELECT step, type, amount, oldbalanceOrg, newbalanceOrig, nameDest
    FROM transactions
    WHERE nameOrig = ?
    ORDER BY step DESC
    LIMIT ?
    """
    with sqlite3.connect(DB_PATH) as conn:
        return pd.read_sql_query(query, conn, params=(nameOrig, limit))


def _load_recipient_history_df(nameDest: str, limit: int = 25) -> pd.DataFrame:
    query = """
    SELECT step, type, amount, oldbalanceDest, newbalanceDest, nameOrig
    FROM transactions
    WHERE nameDest = ?
    ORDER BY step DESC
    LIMIT ?
    """
    with sqlite3.connect(DB_PATH) as conn:
        return pd.read_sql_query(query, conn, params=(nameDest, limit))


def _find_similar_df(txn_type: str, amount: float, tolerance_pct: float = 15.0, limit: int = 10) -> pd.DataFrame:
    lo = amount * (1.0 - tolerance_pct / 100.0)
    hi = amount * (1.0 + tolerance_pct / 100.0)
    query = """
    SELECT step, type, amount, nameOrig, nameDest, isFraud
    FROM transactions
    WHERE UPPER(type) = UPPER(?) AND amount BETWEEN ? AND ?
    ORDER BY step DESC
    LIMIT ?
    """
    with sqlite3.connect(DB_PATH) as conn:
        return pd.read_sql_query(query, conn, params=(txn_type, lo, hi, limit))


def _is_zero_balance(value: Any) -> bool:
    try:
        return abs(float(value)) < 1e-9
    except Exception:
        return False


def _liquidation_signal(df: pd.DataFrame) -> tuple[bool, str]:
    if df.empty:
        return False, "No user history available."

    work_df = df.copy()
    work_df["type"] = work_df["type"].astype(str).str.upper()
    work_df["step"] = pd.to_numeric(work_df["step"], errors="coerce")
    work_df["amount"] = pd.to_numeric(work_df["amount"], errors="coerce")
    work_df["newbalanceOrig"] = pd.to_numeric(work_df["newbalanceOrig"], errors="coerce")
    work_df = work_df.dropna(subset=["step", "amount", "newbalanceOrig"]).sort_values("step")
    if work_df.empty:
        return False, "History was not numeric enough for liquidation checks."

    for _, row in work_df.iterrows():
        if row["type"] != "TRANSFER":
            continue
        if not _is_zero_balance(row["newbalanceOrig"]):
            continue

        transfer_step = int(row["step"])
        transfer_amount = float(row["amount"])
        next_steps = work_df[
            (work_df["type"] == "CASH_OUT")
            & (work_df["step"] >= transfer_step)
            & (work_df["step"] <= transfer_step + 2)
        ]
        if next_steps.empty:
            continue
        if (next_steps["amount"] >= 0.5 * transfer_amount).any():
            detail = (
                f"TRANSFER drained balance at step {transfer_step}, followed by nearby "
                "CASH_OUT of similar magnitude."
            )
            return True, detail

    return False, "No TRANSFER->CASH_OUT liquidation sequence detected."


def _repetitive_payment_signal(df: pd.DataFrame) -> tuple[bool, str]:
    if df.empty:
        return False, "No user history available."

    work_df = df.copy()
    work_df["amount"] = pd.to_numeric(work_df["amount"], errors="coerce")
    work_df = work_df.dropna(subset=["nameDest", "amount"])
    if work_df.empty:
        return False, "No usable recipient payment history available."

    for name_dest, grp in work_df.groupby("nameDest"):
        if len(grp) < 3:
            continue
        mean_amount = float(grp["amount"].mean())
        std_amount = float(grp["amount"].std(ddof=0))
        if mean_amount <= 0:
            continue
        coeff_var = std_amount / mean_amount
        if coeff_var <= 0.15:
            detail = (
                f"Found repetitive payments to {name_dest} with low amount variance "
                f"(CV={coeff_var:.2f}) across {len(grp)} transactions."
            )
            return True, detail
    return False, "No repetitive low-variance payment pattern detected."


def _clamp(value: float, low: float, high: float) -> float:
    return max(low, min(high, value))


def _coerce_float(value: Any, default: float) -> float:
    try:
        return float(value)
    except Exception:
        return default


def _normalize_verdict(raw_verdict: Any, ml_prediction: int) -> str:
    verdict_txt = str(raw_verdict or "").strip().upper()
    if verdict_txt in {"BLOCK", "ALLOW"}:
        return verdict_txt
    return "BLOCK" if ml_prediction == 1 else "ALLOW"


def _derive_risk_score(final_verdict: str, confidence_score: float) -> float:
    conf = _clamp(confidence_score, 0.0, 1.0)
    if final_verdict == "BLOCK":
        return round(60.0 + 40.0 * conf, 2)
    return round(40.0 * (1.0 - conf), 2)


def _coerce_bool(value: Any, default: bool = False) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        lowered = value.strip().lower()
        if lowered in {"true", "1", "yes", "y"}:
            return True
        if lowered in {"false", "0", "no", "n"}:
            return False
    return default


def _derive_control_fields(
    final_verdict: str,
    risk_score: float,
    payload: dict[str, Any] | None = None,
) -> tuple[str, str, bool]:
    payload = payload or {}
    default_human_review = final_verdict == "BLOCK" or risk_score >= 70.0
    human_review_required = _coerce_bool(
        payload.get("human_review_required"),
        default=default_human_review,
    )

    control_action = str(payload.get("control_action") or "").strip().lower()
    if control_action not in {"reroute", "none"}:
        control_action = "reroute" if human_review_required else "none"

    if control_action == "reroute":
        human_review_required = True

    control_destination = str(payload.get("control_destination") or "").strip()
    if control_action == "reroute" and not control_destination:
        control_destination = "manual_review_queue"
    if control_action == "none":
        control_destination = ""

    return control_action, control_destination, human_review_required


def _parse_result_payload(raw_text: str, ml_prediction: int) -> InvestigationResult:
    text = (raw_text or "").strip()
    if not text:
        raise ValueError("Empty agent output.")

    payload: dict[str, Any]
    try:
        payload = json.loads(text)
    except Exception:
        match = re.search(r"\{.*\}", text, flags=re.DOTALL)
        if not match:
            raise ValueError("Agent output was not valid JSON.")
        payload = json.loads(match.group(0))

    final_verdict = _normalize_verdict(payload.get("final_verdict"), ml_prediction)
    confidence = _clamp(_coerce_float(payload.get("confidence_score"), 0.7), 0.0, 1.0)
    risk_score = _coerce_float(payload.get("risk_score"), _derive_risk_score(final_verdict, confidence))
    reasoning = str(payload.get("reasoning") or "No reasoning provided.")

    evidence_raw = payload.get("evidence")
    evidence: list[str]
    if isinstance(evidence_raw, list):
        evidence = [str(item) for item in evidence_raw if str(item).strip()]
    elif isinstance(evidence_raw, str) and evidence_raw.strip():
        evidence = [evidence_raw.strip()]
    else:
        evidence = []

    default_verdict = "BLOCK" if ml_prediction == 1 else "ALLOW"
    ml_correction_raw = payload.get("ml_correction")
    if isinstance(ml_correction_raw, bool):
        ml_correction = ml_correction_raw
    else:
        ml_correction = final_verdict != default_verdict

    control_action, control_destination, human_review_required = _derive_control_fields(
        final_verdict=final_verdict,
        risk_score=risk_score,
        payload=payload,
    )

    return InvestigationResult(
        final_verdict=final_verdict,
        ml_correction=ml_correction,
        reasoning=reasoning,
        confidence_score=round(confidence, 3),
        risk_score=round(risk_score, 2),
        control_action=control_action,
        control_destination=control_destination,
        human_review_required=human_review_required,
        evidence=evidence,
        raw_agent_output=text,
    )


def _offline_rule_verdict(user_id: str, ml_prediction: int, root_error: str) -> InvestigationResult:
    default_verdict = "BLOCK" if ml_prediction == 1 else "ALLOW"
    fallback_prefix = f"Fallback mode: LLM unavailable ({root_error})."

    try:
        user_df = _load_user_history_df(user_id, limit=120)
    except Exception as db_exc:
        confidence = 0.55
        risk_score = _derive_risk_score(default_verdict, confidence)
        control_action, control_destination, human_review_required = _derive_control_fields(
            default_verdict, risk_score
        )
        return InvestigationResult(
            final_verdict=default_verdict,
            ml_correction=False,
            reasoning=(
                f"{fallback_prefix} Database lookup failed ({type(db_exc).__name__}); "
                "keeping ML decision."
            ),
            confidence_score=confidence,
            risk_score=risk_score,
            control_action=control_action,
            control_destination=control_destination,
            human_review_required=human_review_required,
            evidence=["Fallback decision used due to unavailable LLM and DB lookup failure."],
            fallback_used=True,
            error=f"{type(db_exc).__name__}: {db_exc}",
        )

    if user_df.empty:
        confidence = 0.56
        risk_score = _derive_risk_score(default_verdict, confidence)
        control_action, control_destination, human_review_required = _derive_control_fields(
            default_verdict, risk_score
        )
        return InvestigationResult(
            final_verdict=default_verdict,
            ml_correction=False,
            reasoning=f"{fallback_prefix} No user history found; keeping ML decision.",
            confidence_score=confidence,
            risk_score=risk_score,
            control_action=control_action,
            control_destination=control_destination,
            human_review_required=human_review_required,
            evidence=["No local user history for additional rule checks."],
            fallback_used=True,
        )

    has_liq, liq_reason = _liquidation_signal(user_df)
    has_repeat, rep_reason = _repetitive_payment_signal(user_df)

    final_verdict = default_verdict
    confidence = 0.65
    evidence = [liq_reason, rep_reason]
    reasoning = "Fallback mode: keeping ML decision from local rule checks."

    if has_liq:
        final_verdict = "BLOCK"
        confidence = 0.9
        reasoning = "Fallback mode: liquidation sequence indicates high fraud risk."
    elif ml_prediction == 1 and has_repeat:
        final_verdict = "ALLOW"
        confidence = 0.82
        reasoning = "Fallback mode: repetitive payment pattern indicates likely false positive."
    elif ml_prediction == 0 and has_liq:
        final_verdict = "BLOCK"
        confidence = 0.84
        reasoning = "Fallback mode: ML clean prediction overridden by strong liquidation evidence."

    risk_score = _derive_risk_score(final_verdict, confidence)
    control_action, control_destination, human_review_required = _derive_control_fields(
        final_verdict, risk_score
    )

    return InvestigationResult(
        final_verdict=final_verdict,
        ml_correction=final_verdict != default_verdict,
        reasoning=f"{reasoning} {fallback_prefix}",
        confidence_score=round(confidence, 3),
        risk_score=risk_score,
        control_action=control_action,
        control_destination=control_destination,
        human_review_required=human_review_required,
        evidence=evidence,
        fallback_used=True,
    )


@tool
def get_user_transaction_history(nameOrig: str) -> str:
    """Fetch recent origin-account transactions for a user."""
    try:
        df = _load_user_history_df(nameOrig=nameOrig, limit=15)
        if df.empty:
            return "User history not found."
        return df.to_string(index=False)
    except Exception as exc:
        return f"Database Error: {type(exc).__name__}: {exc}"


@tool
def get_recipient_transaction_history(nameDest: str) -> str:
    """Fetch recent recipient-account transactions for a destination account."""
    try:
        df = _load_recipient_history_df(nameDest=nameDest, limit=15)
        if df.empty:
            return "Recipient history not found."
        return df.to_string(index=False)
    except Exception as exc:
        return f"Database Error: {type(exc).__name__}: {exc}"


@tool
def detect_user_liquidation_pattern(nameOrig: str) -> str:
    """Detect TRANSFER-to-CASH_OUT liquidation behavior for a user."""
    try:
        df = _load_user_history_df(nameOrig=nameOrig, limit=120)
        detected, reason = _liquidation_signal(df)
        payload = {
            "pattern": "liquidation",
            "detected": detected,
            "reason": reason,
        }
        return json.dumps(payload, ensure_ascii=False)
    except Exception as exc:
        return f"Database Error: {type(exc).__name__}: {exc}"


@tool
def detect_user_repetitive_payments(nameOrig: str) -> str:
    """Detect repetitive low-variance payment behavior for a user."""
    try:
        df = _load_user_history_df(nameOrig=nameOrig, limit=120)
        detected, reason = _repetitive_payment_signal(df)
        payload = {
            "pattern": "repetitive_consistent_payments",
            "detected": detected,
            "reason": reason,
        }
        return json.dumps(payload, ensure_ascii=False)
    except Exception as exc:
        return f"Database Error: {type(exc).__name__}: {exc}"


@tool
def find_similar_transactions(txn_type: str, amount: float) -> str:
    """Find historical transactions with similar type and amount range."""
    try:
        df = _find_similar_df(txn_type=txn_type, amount=amount, tolerance_pct=15.0, limit=12)
        if df.empty:
            return "No similar transactions found in the configured amount window."
        return df.to_string(index=False)
    except Exception as exc:
        return f"Database Error: {type(exc).__name__}: {exc}"


def create_agora_agent(verbose: bool = False):
    groq_api_key = _get_groq_api_key()

    llm = ChatGroq(
        model="openai/gpt-oss-20b",
        groq_api_key=groq_api_key,
        temperature=0,
        request_timeout=GROQ_REQUEST_TIMEOUT_SEC,
        max_retries=1,
    )
    tools = [
        get_user_transaction_history,
        get_recipient_transaction_history,
        detect_user_liquidation_pattern,
        detect_user_repetitive_payments,
        find_similar_transactions,
    ]

    system_prompt = """
    You are the AGORA Risk Investigator for Razorpay.
    You receive a CatBoost fraud signal and must produce a second-look decision.

    Rules:
    1) Use tools before finalizing a verdict.
    2) Look for liquidation pattern: TRANSFER draining origin balance followed by rapid CASH_OUT.
    3) Consider repetitive, low-variance recipient payments as false-positive evidence.
    4) If the transaction needs human verification, set control_action to "reroute",
       control_destination to "manual_review_queue", and human_review_required to true.
    5) Return your final answer as raw JSON only (no markdown fences).
    6) Keep reasoning concise and technical.

    Required output JSON keys:
    {
      "final_verdict": "BLOCK or ALLOW",
      "ml_correction": true or false,
      "reasoning": "short explanation",
      "confidence_score": 0.0 to 1.0,
      "risk_score": 0 to 100,
      "control_action": "reroute or none",
      "control_destination": "manual_review_queue or empty string",
      "human_review_required": true or false,
      "evidence": ["fact 1", "fact 2"]
    }
    """

    if LANGCHAIN_AGENT_API == "legacy":
        prompt = ChatPromptTemplate.from_messages(
            [
                ("system", system_prompt),
                ("human", "{input}"),
                MessagesPlaceholder(variable_name="agent_scratchpad"),
            ]
        )
        agent = create_tool_calling_agent(llm, tools, prompt)
        return AgentExecutor(
            agent=agent,
            tools=tools,
            verbose=verbose,
            return_intermediate_steps=True,
            handle_parsing_errors=True,
        )

    return create_agent(
        model=llm,
        tools=tools,
        system_prompt=system_prompt,
        debug=verbose,
    )


def run_investigation(
    user_id: str,
    ml_prediction: int,
    transaction_context: dict[str, Any] | None = None,
    verbose: bool = False,
) -> InvestigationResult:
    """
    Bridge CatBoost output and agentic second-look investigation.
    Returns a structured InvestigationResult.
    """
    status_label = "Fraud" if ml_prediction == 1 else "Non-Fraud"
    context = transaction_context or {}
    context_txt = ", ".join([f"{k}={context[k]}" for k in sorted(context)]) if context else "none"
    input_text = (
        f"CatBoost flagged user {user_id} as {status_label}. "
        f"Transaction context: {context_txt}. "
        "Investigate and return the final risk verdict."
    )

    if verbose:
        print(f"[INFO] Running investigation for user '{user_id}' ({status_label})...", flush=True)

    start_time = time.perf_counter()
    try:
        agent_executor = create_agora_agent(verbose=verbose)
        if LANGCHAIN_AGENT_API == "legacy":
            response = agent_executor.invoke({"input": input_text})
        else:
            response = agent_executor.invoke(
                {"messages": [{"role": "user", "content": input_text}]}
            )

        raw_output = _extract_agent_output(response)
        result = _parse_result_payload(raw_output, ml_prediction=ml_prediction)
        result.tool_trace = _extract_tool_trace(response)
    except Exception as exc:
        if not ENABLE_OFFLINE_FALLBACK:
            raise
        if verbose:
            print(f"[WARN] LLM call failed; using offline fallback: {exc}", flush=True)
        result = _offline_rule_verdict(
            user_id=user_id,
            ml_prediction=ml_prediction,
            root_error=type(exc).__name__,
        )
        result.error = f"{type(exc).__name__}: {exc}"

    result.latency_s = time.perf_counter() - start_time
    return result


if __name__ == "__main__":
    test_id = "C712410124"
    prediction = 1
    print("[INFO] Starting AGORA risk investigation...", flush=True)
    try:
        verdict = run_investigation(test_id, prediction, verbose=False)
        print(f"Investigation Time: {verdict.latency_s:.2f}s")
        print(f"Final Decision: {verdict.to_json()}")
    except Exception as exc:
        print(f"[ERROR] Investigation failed: {exc}", flush=True)
        print(
            "[HINT] Check GROQ_API_KEY, network/firewall settings, and model availability.",
            flush=True,
        )
        raise SystemExit(1)

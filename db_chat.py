import os
import re
import sqlite3
from typing import Any

from dotenv import load_dotenv
from langchain_groq import ChatGroq
from langchain_core.messages import HumanMessage, SystemMessage

load_dotenv()

DB_PATH = "agora_transactions.db"
GROQ_MODEL = "openai/gpt-oss-120b"
GROQ_API_KEY_ENV = "GROQ_API_KEY"
DEFAULT_LIMIT = 50


def _get_db_connection(timeout: int = 20) -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH, timeout=timeout)
    conn.execute("PRAGMA journal_mode=WAL")
    return conn
SUGGESTED_QUESTIONS = [
    "Show fraud count by transaction type",
    "How many transactions were processed in total?",
    "List top 10 largest transfer transactions",
    "Which users have the highest fraud transaction counts?",
]

FORBIDDEN_SQL_PATTERNS = [
    r"\binsert\b",
    r"\bupdate\b",
    r"\bdelete\b",
    r"\bdrop\b",
    r"\balter\b",
    r"\bcreate\b",
    r"\breplace\b",
    r"\btruncate\b",
    r"\battach\b",
    r"\bdetach\b",
    r"\bpragma\b",
    r"\bvacuum\b",
]


def _get_groq_api_key() -> str:
    key = os.getenv(GROQ_API_KEY_ENV, "").strip()
    if not key:
        raise EnvironmentError(
            f"{GROQ_API_KEY_ENV} is not set. Add it to your environment or .env file."
        )
    return key


def _get_schema_text() -> str:
    with _get_db_connection() as conn:
        rows = conn.execute("PRAGMA table_info(transactions)").fetchall()
    if not rows:
        return "Table transactions is unavailable."

    lines = ["transactions("]
    for _, col_name, col_type, _, _, _ in rows:
        lines.append(f"  {col_name} {col_type}")
    lines.append(")")
    return "\n".join(lines)


def _strip_markdown_fences(text: str) -> str:
    cleaned = text.strip()
    cleaned = cleaned.replace("```sql", "").replace("```", "").strip()
    return cleaned


def _sanitize_sql(sql_text: str) -> str:
    sql = _strip_markdown_fences(sql_text)
    sql = re.sub(r"\s+", " ", sql).strip()

    if not sql:
        raise ValueError("The model returned an empty SQL statement.")
    if ";" in sql:
        raise ValueError("Multiple statements are not allowed.")

    lowered = sql.lower()
    if not (lowered.startswith("select ") or lowered.startswith("with ")):
        raise ValueError("Only SELECT queries are allowed.")

    for pattern in FORBIDDEN_SQL_PATTERNS:
        if re.search(pattern, lowered):
            raise ValueError("Unsafe SQL detected. Query blocked.")

    if not re.search(r"\blimit\s+\d+\b", lowered):
        sql = f"{sql} LIMIT {DEFAULT_LIMIT}"

    return sql


def _execute_readonly_query(sql: str) -> list[dict[str, Any]]:
    with _get_db_connection() as conn:
        conn.row_factory = sqlite3.Row
        rows = conn.execute(sql).fetchall()
    return [dict(row) for row in rows]


def _summarize_rows(rows: list[dict[str, Any]]) -> str:
    if not rows:
        return "No rows found."
    if len(rows) == 1 and len(rows[0]) == 1:
        only_value = next(iter(rows[0].values()))
        return f"Result: {only_value}"

    first = rows[0]
    key_cols = [k for k in first.keys()][:3]
    if key_cols:
        sample = ", ".join([f"{k}={first.get(k)}" for k in key_cols])
        return f"Returned {len(rows)} rows. First row snapshot: {sample}"
    return f"Returned {len(rows)} rows."


def _explain_result(rows: list[dict[str, Any]]) -> str:
    if not rows:
        return "There are currently no matching records for this question."

    first = rows[0]
    if len(rows) == 1 and "total_transactions" in first:
        return (
            "The dataset volume is stable enough for trend analysis and "
            "agent validation checks."
        )
    if len(rows) == 1 and "fraud_count" in first:
        return "This helps identify transaction channels with elevated fraud pressure."
    if "fraud_count" in first or "fraud_rows" in first:
        return "Use this split to prioritize policy rules by transaction type."
    if "amount" in first:
        return (
            "High-value transaction lists are useful for secondary review and "
            "investigation triage."
        )
    return "Use this result as a baseline and drill down with a follow-up question."


def _friendly_error_message(error: Exception) -> str:
    message = str(error)
    lowered = message.lower()
    if "only select queries are allowed" in lowered:
        return "Only read-only analytics queries are allowed. Try asking for counts, trends, or top records."
    if "unsafe sql" in lowered:
        return "The generated SQL was blocked for safety. Try a simpler read-only analytics question."
    if "empty sql statement" in lowered:
        return "The model returned an empty query. Try rephrasing with more concrete metrics."
    return "Unable to answer right now. Try a simpler analytics question."


def get_suggested_questions() -> list[str]:
    return list(SUGGESTED_QUESTIONS)


def _fallback_sql_for_common_questions(question: str) -> str | None:
    q = question.lower()
    if "total transaction" in q or "how many transaction" in q:
        return "SELECT COUNT(*) AS total_transactions FROM transactions"
    if "fraud" in q and "type" in q:
        return (
            "SELECT type, SUM(isFraud) AS fraud_count "
            "FROM transactions GROUP BY type ORDER BY fraud_count DESC"
        )
    if "largest transfer" in q or ("top" in q and "transfer" in q):
        return (
            "SELECT step, nameOrig, nameDest, amount FROM transactions "
            "WHERE type = 'TRANSFER' ORDER BY amount DESC LIMIT 10"
        )
    if "user" in q and "fraud" in q and ("highest" in q or "top" in q or "count" in q):
        return (
            "SELECT nameOrig, COUNT(*) AS fraud_transaction_count "
            "FROM transactions WHERE isFraud = 1 "
            "GROUP BY nameOrig ORDER BY fraud_transaction_count DESC LIMIT 10"
        )
    if "block" in q or "blocked" in q:
        return (
            "SELECT type, COUNT(*) AS total_rows, SUM(isFraud) AS fraud_rows "
            "FROM transactions GROUP BY type ORDER BY fraud_rows DESC"
        )
    return None


def _generate_sql_from_question(question: str) -> str:
    schema_text = _get_schema_text()
    system_prompt = (
        "You generate a single SQLite SELECT query for analytics.\n"
        "Return SQL only. Do not use markdown.\n"
        "Never modify data. Never include semicolons.\n"
        "Use only the table and columns provided."
    )
    user_prompt = (
        f"Schema:\n{schema_text}\n\n"
        f"Question: {question}\n\n"
        "Return one SQL SELECT query."
    )

    llm = ChatGroq(
        model=GROQ_MODEL,
        groq_api_key=_get_groq_api_key(),
        temperature=0,
        request_timeout=30,
        max_retries=1,
    )
    response = llm.invoke(
        [SystemMessage(content=system_prompt), HumanMessage(content=user_prompt)]
    )
    return (response.content or "").strip()


def answer_db_question(question: str) -> dict[str, Any]:
    question = (question or "").strip()
    if not question:
        return {
            "answer": "Ask a database question to begin.",
            "sql": "",
            "rows": [],
            "insight": "",
            "error": "Empty question.",
            "safety_note": "Read-only mode: only SELECT and WITH queries are allowed.",
        }

    sql = ""
    safety_note = "Read-only mode: only SELECT and WITH queries are allowed."
    try:
        raw_sql = _generate_sql_from_question(question)
        sql = _sanitize_sql(raw_sql)
        rows = _execute_readonly_query(sql)
        return {
            "answer": _summarize_rows(rows),
            "sql": sql,
            "rows": rows,
            "insight": _explain_result(rows),
            "error": None,
            "safety_note": safety_note,
        }
    except Exception as llm_or_sql_error:
        fallback_sql = _fallback_sql_for_common_questions(question)
        if fallback_sql:
            try:
                sql = _sanitize_sql(fallback_sql)
                rows = _execute_readonly_query(sql)
                return {
                    "answer": (
                        "LLM SQL generation was unavailable, so a built-in analytics query was used."
                    ),
                    "sql": sql,
                    "rows": rows,
                    "insight": _explain_result(rows),
                    "error": str(llm_or_sql_error),
                    "safety_note": safety_note,
                }
            except Exception as fallback_error:
                return {
                    "answer": _friendly_error_message(fallback_error),
                    "sql": sql,
                    "rows": [],
                    "insight": "",
                    "error": str(fallback_error),
                    "safety_note": safety_note,
                }

        return {
            "answer": _friendly_error_message(llm_or_sql_error),
            "sql": sql,
            "rows": [],
            "insight": "",
            "error": str(llm_or_sql_error),
            "safety_note": safety_note,
        }

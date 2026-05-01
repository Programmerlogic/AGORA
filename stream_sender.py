import argparse
import os
import time
from typing import Any

import pandas as pd
import requests
from dotenv import load_dotenv

load_dotenv()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Stream transaction payloads from X_test.csv into AGORA API."
    )
    parser.add_argument("--api-url", default=os.getenv("AGORA_API_URL", "http://localhost:8000"))
    parser.add_argument("--csv", default="X_test.csv")
    parser.add_argument("--sender-prefix", default="SENDER")
    parser.add_argument("--receiver-prefix", default="RECEIVER")
    parser.add_argument("--count", type=int, default=50, help="Number of rows to stream.")
    parser.add_argument(
        "--interval-ms",
        type=int,
        default=400,
        help="Delay between payloads in milliseconds.",
    )
    parser.add_argument("--start-index", type=int, default=0)
    return parser.parse_args()


def make_payload(
    row: pd.Series, idx: int, sender_prefix: str, receiver_prefix: str
) -> dict[str, Any]:
    sender_id = str(row.get("nameOrig", f"{sender_prefix}_{idx % 25:04d}"))
    receiver_id = str(row.get("nameDest", f"{receiver_prefix}_{(idx * 3) % 25:04d}"))
    return {
        "transaction_id": f"txn_stream_{idx}_{int(time.time() * 1000)}",
        "idempotency_key": f"idem_stream_{idx}",
        "sender_id": sender_id,
        "receiver_id": receiver_id,
        "type": str(row["type"]),
        "amount": float(row["amount"]),
        "step": int(row["step"]),
        "metadata": {"source": "stream_sender", "row_index": idx},
    }


def ensure_account(
    api_url: str, api_key: str, account_id: str, initial_balance: float = 500000.0
) -> None:
    headers = {"Content-Type": "application/json"}
    if api_key:
        headers["X-AGORA-API-Key"] = api_key
    body = {
        "account_id": account_id,
        "available_balance": float(initial_balance),
        "held_balance": 0.0,
    }
    response = requests.post(f"{api_url}/accounts", json=body, headers=headers, timeout=20)
    if response.status_code >= 300:
        raise RuntimeError(
            f"Failed to upsert account {account_id}: "
            f"{response.status_code} {response.text}"
        )


def main() -> None:
    args = parse_args()
    api_key = os.getenv("AGORA_API_KEY", "")

    df = pd.read_csv(args.csv)
    if args.start_index >= len(df):
        raise ValueError("start-index is beyond the CSV length.")

    stop = min(len(df), args.start_index + args.count)
    work_df = df.iloc[args.start_index:stop].reset_index(drop=True)

    headers = {"Content-Type": "application/json"}
    if api_key:
        headers["X-AGORA-API-Key"] = api_key

    print(
        f"[INFO] Streaming {len(work_df)} payloads to {args.api_url}/transactions "
        f"(interval={args.interval_ms}ms)"
    )

    for local_idx, (_, row) in enumerate(work_df.iterrows()):
        idx = args.start_index + local_idx
        payload = make_payload(row, idx, args.sender_prefix, args.receiver_prefix)
        ensure_account(args.api_url, api_key, payload["sender_id"], initial_balance=500000.0)
        ensure_account(args.api_url, api_key, payload["receiver_id"], initial_balance=250000.0)

        response = requests.post(
            f"{args.api_url}/transactions",
            json=payload,
            headers=headers,
            timeout=60,
        )
        if response.status_code >= 300:
            print(
                f"[ERROR] idx={idx} status={response.status_code} payload={payload}"
                f" body={response.text}"
            )
        else:
            data = response.json()
            print(
                f"[OK] idx={idx} tx={data.get('transaction_id')} "
                f"status={data.get('status')} verdict={data.get('final_verdict')}"
            )
        time.sleep(max(0, args.interval_ms) / 1000.0)

    print("[INFO] Stream sender completed.")


if __name__ == "__main__":
    main()

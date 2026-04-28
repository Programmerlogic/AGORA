# AGORA: Autonomous Gateway for Orchestrating Risk Analysis

AGORA is a hybrid fraud monitoring system that combines machine learning with an agentic AI risk-investigation layer, human analyst review, and read-only database insights. The project is designed around a real-time payment-risk workflow: a CatBoost model performs fast transaction classification, and a LangChain/Groq agentic AI investigator performs a second-look review for suspicious transactions by using tools to inspect transaction history and risk patterns.

## Project Overview

The system uses a layered fraud detection architecture:

- **CatBoost fraud classifier** for the first-pass transaction decision.
- **Agentic AI investigation layer** built with LangChain/Groq for second-look risk analysis.
- **SQLite transaction database** for transaction history and audit storage.
- **Streamlit dashboard** for live fraud monitoring, investigation details, and analyst review.
- **DB Insights chatbot** for read-only natural-language analytics over the transaction table.

This design keeps the fast ML model in the critical path while adding explainable investigation and human oversight for high-risk cases.

## Dataset Description

The project uses a PaySim-style transaction dataset stored locally as `log.csv`.

Dataset shape:

- Rows: `6,362,620`
- Columns: `11`

Columns:

| Column | Description |
| --- | --- |
| `step` | Simulation time step for the transaction. |
| `type` | Transaction type such as `PAYMENT`, `TRANSFER`, `CASH_OUT`, `CASH_IN`, or `DEBIT`. |
| `amount` | Transaction amount. |
| `nameOrig` | Origin account/customer identifier. |
| `oldbalanceOrg` | Origin account balance before the transaction. |
| `newbalanceOrig` | Origin account balance after the transaction. |
| `nameDest` | Destination account/customer identifier. |
| `oldbalanceDest` | Destination account balance before the transaction. |
| `newbalanceDest` | Destination account balance after the transaction. |
| `isFraud` | Ground-truth fraud label. |
| `isFlaggedFraud` | Rule-based fraud flag present in the dataset. |

Class distribution:

| Class | Meaning | Count |
| --- | --- | ---: |
| `0` | Non-fraud | `6,354,407` |
| `1` | Fraud | `8,213` |

Fraud rate:

```text
0.129%
```

The dataset is highly imbalanced, which reflects a common real-world fraud detection challenge: fraudulent transactions are rare compared with normal transaction volume.

`X_test.csv` is generated from the notebook and contains the feature columns used by the Streamlit live-monitoring flow:

```text
step, type, amount, oldbalanceOrg, newbalanceOrig, oldbalanceDest, newbalanceDest
```

## Model Training and Classification Result

The model training workflow is documented in `train.ipynb`.

Training approach:

- Loaded the full transaction dataset from `log.csv`.
- Addressed extreme class imbalance through majority-class downsampling.
- Downsampled normal transactions to `82,000`.
- Combined downsampled normal transactions with all `8,213` fraud transactions.
- Final training dataset size after balancing: `90,213` rows.
- Used `CatBoostClassifier` for classification.
- Treated `type` as the categorical feature.
- Used Optuna for hyperparameter optimization.

Train/test split:

| Split | Rows |
| --- | ---: |
| Training | `72,170` |
| Test | `18,043` |

Best Optuna result:

```text
Best score: 0.9780237770593856
Best parameters:
iterations = 252
learning_rate = 0.0950128967024424
depth = 5
```

Final classification report from `train.ipynb`:

| Class | Precision | Recall | F1-score | Support |
| --- | ---: | ---: | ---: | ---: |
| `0` Non-fraud | `1.00` | `0.99` | `1.00` | `16,400` |
| `1` Fraud | `0.94` | `1.00` | `0.97` | `1,643` |

Overall result:

| Metric | Value |
| --- | ---: |
| Accuracy | `0.99` |
| Macro avg F1-score | `0.98` |
| Weighted avg F1-score | `0.99` |

The final model is saved as:

```text
agora_fraud_model.cbm
```

## Detailed Workflow

AGORA follows an end-to-end risk monitoring workflow.

1. **Data ingestion**

   `populate_db.py` reads `log.csv` in chunks and loads the records into `agora_transactions.db`.

   It also creates indexes on important lookup fields such as `nameOrig`, `nameDest`, and `(type, amount)` so investigation tools and analytics queries can run faster.

2. **Model training**

   `train.ipynb` preprocesses the dataset, handles class imbalance, trains the CatBoost model, evaluates classification performance, saves the model artifact, and exports `X_test.csv` for live dashboard simulation.

3. **Live transaction stream**

   `dashboard.py` loads `agora_fraud_model.cbm` and reads transactions from `X_test.csv`.

   The dashboard simulates a live stream and sends each transaction through the CatBoost classifier.

4. **First-pass ML decision**

   CatBoost predicts whether the transaction is clean or suspicious.

   Clean transactions are allowed directly in the dashboard flow.

5. **Agentic second-look investigation**

   If CatBoost predicts fraud, `risk_agent.py` runs a LangChain/Groq investigation agent.

   The agent reviews transaction context and uses database tools to inspect historical patterns before producing a final risk verdict.

6. **Investigation event logging**

   Investigation outputs are stored in SQLite with structured fields such as verdict, confidence, risk score, reason code, reason tags, evidence, and investigation steps.

7. **Reroute command and backend acknowledgment**

   If the agent marks a case as requiring human verification, the dashboard creates a `reroute` JSON control command for the `manual_review_queue`.

   A local SQLite-backed simulated backend stores the command and returns a `success` acknowledgment, making the control loop bidirectional: AGORA sends the command and records the backend response.

8. **Analyst review**

   A human analyst can review a selected investigation case in the dashboard.

   The analyst can choose:

   - `CONFIRM_BLOCK`
   - `MARK_FALSE_POSITIVE`
   - `ESCALATE`

   The analyst can also add a manual note. Reviews are saved in the `analyst_reviews` table for auditability.

9. **DB insights**

   `db_chat.py` powers a read-only analytics chatbot.

   Users can ask natural-language questions about the transaction database, and the system converts supported questions into safe SQLite `SELECT` queries.

## Agentic AI Layer

The risk investigation layer is implemented in `risk_agent.py`.

The agent is not just generating text; it can use tools to inspect data before returning a decision. Its toolset includes:

- Sender transaction history lookup.
- Recipient transaction history lookup.
- Liquidation-pattern detection.
- Repetitive-payment detection.
- Similar transaction search by type and amount.

The agent returns a structured investigation result containing:

- `final_verdict`
- `ml_correction`
- `reasoning`
- `confidence_score`
- `risk_score`
- `control_action`
- `control_destination`
- `human_review_required`
- `evidence`
- `tool_trace`
- `latency_s`
- `fallback_used`

The dashboard converts technical tool traces into plain-language investigation steps so the reasoning is understandable to non-technical users.

## Dashboard Features

The Streamlit dashboard in `dashboard.py` provides:

- Live transaction monitoring.
- CatBoost anomaly counts.
- Agent-blocked transaction counts.
- ML override counts.
- Total latency reporting.
- Transaction-type classification charts.
- Risk-score timeline.
- Decision funnel visualization.
- Investigation event log.
- Investigation reason details.
- Reroute-to-human-verification command status.
- Analyst review and manual note capture.
- CSV/JSON export for investigation logs.

The live monitoring section uses Streamlit fragments so the live stream can update without forcing a full app rerun for every transaction.

## DB Insights Chatbot

The DB Insights chatbot is implemented in `db_chat.py`.

It allows read-only natural-language analytics over `agora_transactions.db`.

Safety controls include:

- Only `SELECT` and `WITH` queries are allowed.
- Write operations such as `INSERT`, `UPDATE`, `DELETE`, `DROP`, and `ALTER` are blocked.
- Multiple SQL statements are blocked.
- A default `LIMIT` is added when the generated query does not include one.
- Built-in fallback SQL is available for common analytics questions.

Example questions:

```text
Show fraud count by transaction type
How many transactions were processed in total?
List top 10 largest transfer transactions
Which users have the highest fraud transaction counts?
```

## Project Files

| File | Purpose |
| --- | --- |
| `log.csv` | Full transaction dataset. |
| `populate_db.py` | Loads `log.csv` into SQLite and creates indexes. |
| `agora_transactions.db` | SQLite database for transactions, investigation events, and analyst reviews. |
| `train.ipynb` | Model training, evaluation, and test-stream export notebook. |
| `agora_fraud_model.cbm` | Trained CatBoost model artifact. |
| `X_test.csv` | Test feature rows used for live stream simulation. |
| `risk_agent.py` | Agentic AI risk investigation layer. |
| `dashboard.py` | Streamlit dashboard and live monitoring interface. |
| `db_chat.py` | Read-only database insights chatbot. |
| `requirements.txt` | Python package requirements. |
| `.env` | Environment variables such as `GROQ_API_KEY`. |

## How to Run

Install dependencies:

```bash
pip install -r requirements.txt
```

Set the Groq API key in `.env`:

```text
GROQ_API_KEY=your_groq_api_key_here
```

Populate the SQLite database:

```bash
python populate_db.py
```

Run the dashboard:

```bash
streamlit run dashboard.py
```

Optional model workflow:

```text
Open train.ipynb and run the notebook to retrain the CatBoost model and regenerate X_test.csv.
```

## Notes for Real-Life Deployment

This project demonstrates a practical fraud-risk architecture, but production deployment would require additional controls:

- Real streaming integration instead of CSV-based simulation.
- Stronger model monitoring and drift detection.
- Secure secrets management instead of local `.env` handling.
- Role-based access control for analyst actions.
- Durable audit logging with backup and retention policies.
- Human review policies for high-impact decisions.
- Additional validation before any automated blocking action is applied to real customers.

AGORA is designed as a human-in-the-loop risk analysis system: the ML model provides speed, the agent provides contextual investigation, and the analyst review layer provides operational accountability.

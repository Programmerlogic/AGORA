#!/usr/bin/env bash
# ============================================================
# AGORA — Deploy to EC2 from your local machine
# Usage:  chmod +x deploy.sh && ./deploy.sh <EC2_PUBLIC_IP> <PEM_KEY_PATH>
#
# Example:
#   ./deploy.sh 54.123.45.67 ~/.ssh/agora-key.pem
#
# On Windows (Git Bash / WSL):
#   bash deploy.sh 54.123.45.67 /c/Users/YourName/.ssh/agora-key.pem
# ============================================================
set -euo pipefail

if [ $# -lt 2 ]; then
    echo "Usage: $0 <EC2_PUBLIC_IP> <PEM_KEY_PATH>"
    echo "Example: $0 54.123.45.67 ~/.ssh/agora-key.pem"
    exit 1
fi

EC2_IP="$1"
PEM_KEY="$2"
EC2_USER="ubuntu"
REMOTE_DIR="/home/ubuntu/agora"
SSH_OPTS="-o StrictHostKeyChecking=no -i ${PEM_KEY}"

echo "===== AGORA Deploy to EC2 @ ${EC2_IP} ====="

# ---- 1. Create remote directories ----
echo "[1/5] Creating remote directories..."
ssh ${SSH_OPTS} ${EC2_USER}@${EC2_IP} "mkdir -p ${REMOTE_DIR}/data ${REMOTE_DIR}/deploy ${REMOTE_DIR}/.streamlit"

# ---- 2. Upload application files ----
echo "[2/5] Uploading application files..."
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_DIR="$(dirname "${SCRIPT_DIR}")"

scp ${SSH_OPTS} "${PROJECT_DIR}/dashboard.py" ${EC2_USER}@${EC2_IP}:${REMOTE_DIR}/
scp ${SSH_OPTS} "${PROJECT_DIR}/risk_agent.py" ${EC2_USER}@${EC2_IP}:${REMOTE_DIR}/
scp ${SSH_OPTS} "${PROJECT_DIR}/db_chat.py" ${EC2_USER}@${EC2_IP}:${REMOTE_DIR}/
scp ${SSH_OPTS} "${PROJECT_DIR}/populate_db.py" ${EC2_USER}@${EC2_IP}:${REMOTE_DIR}/
scp ${SSH_OPTS} "${PROJECT_DIR}/requirements.txt" ${EC2_USER}@${EC2_IP}:${REMOTE_DIR}/
scp ${SSH_OPTS} "${PROJECT_DIR}/Dockerfile" ${EC2_USER}@${EC2_IP}:${REMOTE_DIR}/
scp ${SSH_OPTS} "${PROJECT_DIR}/agora_fraud_model.cbm" ${EC2_USER}@${EC2_IP}:${REMOTE_DIR}/
scp ${SSH_OPTS} "${PROJECT_DIR}/X_test.csv" ${EC2_USER}@${EC2_IP}:${REMOTE_DIR}/
scp ${SSH_OPTS} "${PROJECT_DIR}/.env" ${EC2_USER}@${EC2_IP}:${REMOTE_DIR}/
scp ${SSH_OPTS} -r "${PROJECT_DIR}/.streamlit/" ${EC2_USER}@${EC2_IP}:${REMOTE_DIR}/.streamlit/

# Upload deploy files
scp ${SSH_OPTS} "${SCRIPT_DIR}/docker-compose.yml" ${EC2_USER}@${EC2_IP}:${REMOTE_DIR}/deploy/
scp ${SSH_OPTS} "${SCRIPT_DIR}/agora.service" ${EC2_USER}@${EC2_IP}:${REMOTE_DIR}/deploy/

# ---- 3. Upload SQLite database (large — ~840MB, only if not present) ----
echo "[3/5] Checking if database needs uploading..."
DB_EXISTS=$(ssh ${SSH_OPTS} ${EC2_USER}@${EC2_IP} "[ -f ${REMOTE_DIR}/data/agora_transactions.db ] && echo 'yes' || echo 'no'")
if [ "${DB_EXISTS}" = "no" ]; then
    echo "  Uploading agora_transactions.db (~840MB) — this will take a few minutes..."
    scp ${SSH_OPTS} "${PROJECT_DIR}/agora_transactions.db" ${EC2_USER}@${EC2_IP}:${REMOTE_DIR}/data/
    echo "  Database uploaded."
else
    echo "  Database already exists on server, skipping upload."
    echo "  To force re-upload, delete it first: ssh -i ${PEM_KEY} ${EC2_USER}@${EC2_IP} 'rm ${REMOTE_DIR}/data/agora_transactions.db'"
fi

# ---- 4. Build and start ----
echo "[4/5] Building Docker image and starting container..."
ssh ${SSH_OPTS} ${EC2_USER}@${EC2_IP} << 'REMOTE_COMMANDS'
cd /home/ubuntu/agora
docker compose -f deploy/docker-compose.yml down 2>/dev/null || true
docker compose -f deploy/docker-compose.yml up -d --build
echo "Waiting 10s for Streamlit to start..."
sleep 10
docker ps --filter name=agora --format "table {{.Names}}\t{{.Status}}\t{{.Ports}}"
REMOTE_COMMANDS

# ---- 5. Health check ----
echo "[5/5] Running health check..."
sleep 5
HTTP_CODE=$(curl -s -o /dev/null -w "%{http_code}" "http://${EC2_IP}:8501/_stcore/health" 2>/dev/null || echo "000")
if [ "${HTTP_CODE}" = "200" ]; then
    echo ""
    echo "  ✅ AGORA is live at: http://${EC2_IP}:8501"
    echo ""
else
    echo ""
    echo "  ⚠️  Health check returned HTTP ${HTTP_CODE}."
    echo "  The app may still be starting. Check logs with:"
    echo "    ssh -i ${PEM_KEY} ${EC2_USER}@${EC2_IP} 'docker logs agora'"
    echo ""
fi

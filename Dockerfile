FROM python:3.11-slim

WORKDIR /app

# System deps for building native Python packages
RUN apt-get update && \
    apt-get install -y --no-install-recommends gcc g++ libgomp1 && \
    rm -rf /var/lib/apt/lists/*

# Install Python dependencies first (layer caching)
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code and assets
COPY dashboard.py .
COPY risk_agent.py .
COPY db_chat.py .
COPY populate_db.py .
COPY agora_fraud_model.cbm .
COPY X_test.csv .
COPY .streamlit/ .streamlit/

# The SQLite database is mounted as a volume at runtime — not baked into the image.
# GROQ_API_KEY is passed as an environment variable at runtime.

EXPOSE 8501

HEALTHCHECK --interval=30s --timeout=10s --retries=3 \
    CMD python -c "import urllib.request; urllib.request.urlopen('http://localhost:8501/_stcore/health')" || exit 1

ENTRYPOINT ["streamlit", "run", "dashboard.py", \
    "--server.port=8501", \
    "--server.address=0.0.0.0", \
    "--server.headless=true", \
    "--browser.gatherUsageStats=false"]

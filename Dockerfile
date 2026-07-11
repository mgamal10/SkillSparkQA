FROM python:3.11-slim

WORKDIR /app

# ── Python deps ───────────────────────────────────────────────────────────
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# ── App code ──────────────────────────────────────────────────────────────
COPY remote_driver.py scenarios.json ./

# Railway injects a dynamic $PORT at runtime — it is NOT fixed like HF's 7860.
# Shell-form CMD (not the JSON-array/exec form) is required for $PORT to
# actually expand; exec-form would pass the literal string "$PORT" to the
# app. Falls back to 7860 if $PORT isn't set (e.g. running locally).
CMD REMOTE_DRIVER_PORT=${PORT:-7860} python3 remote_driver.py
"""
remote_driver.py — SkillSpark Native (Accessibility-Service) Driver backend
════════════════════════════════════════════════════════════════════════════
Purpose
  Serves scenario/selector-map JSON to the on-device Kotlin Accessibility
  Service (ScenarioExecutor.kt) and relays that device's run reports to n8n.

  This is deliberately NOT the Appium path (test_runner.py / test_mobile.py).
  There is no adb, no udid, no device_endpoint tunnel, and no Appium session
  here. The phone that has SkillSpark installed executes the scenario against
  the target app in-process via its own AccessibilityService, using the
  selector map this service hands it. That phone then POSTs its own result
  back to /report, which this service forwards to n8n.

Endpoints
  GET  /                                    health check
  GET  /scenario/{name}?env=staging|production
                                             returns the selector map + vars
                                             for the executor to run, with
                                             {{var}} placeholders left intact
                                             (Kotlin does the substitution)
  GET  /scenarios                           lightweight listing for a picker UI
  POST /report                              on-device run report → relayed
                                             to n8n with retry + disk fallback
  POST /scenarios/reload                    force scenarios.json reload

Run
  Local:   pip install -r requirements.txt
           python3 remote_driver.py          # listens on $REMOTE_DRIVER_PORT or 7860
  Railway: deployed via the accompanying Dockerfile, which maps Railway's
           dynamic $PORT into REMOTE_DRIVER_PORT automatically.
"""

import os
import re
import json
import time
import threading
from pathlib import Path
from datetime import datetime, timezone
from typing import Optional, Any

import requests
import requests.adapters
import uvicorn
from contextlib import asynccontextmanager
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field

# ══════════════════════════════════════════════════════════════════════════
#  CONFIGURATION
# ══════════════════════════════════════════════════════════════════════════

PORT = int(os.environ.get("REMOTE_DRIVER_PORT", "7860"))

SCENARIOS_FILE = Path(__file__).parent / "scenarios.json"

# Same n8n results webhook used by test_mobile.py / test_runner.py, so runs
# from either path land in the same place downstream. Override via env var
# if you want this service pointed at a different n8n instance.
N8N_RESULTS_URL = os.environ.get(
    "N8N_RESULTS_URL", "https://jimmyyzz-n8n.hf.space/webhook/test_results"
)

# ── Webhook delivery settings — same shape as test_runner.py's resilient
#    sender, sized down since payloads here are small JSON (no image_b64
#    blobs; the device already has its own screenshot story if it wants one).
CONNECT_TIMEOUT = 15
WRITE_TIMEOUT = 20
MAX_RETRIES = 3
BACKOFF_BASE = 3  # 3s, 9s, 27s
FALLBACK_FILE = Path(__file__).parent / "remote_driver_fallback_queue.jsonl"

# ══════════════════════════════════════════════════════════════════════════
#  SCENARIO STORE — hot-reloaded on mtime change, no restart needed to edit
# ══════════════════════════════════════════════════════════════════════════

_scenarios_lock = threading.Lock()
_scenarios_cache: dict = {}
_scenarios_mtime: float = 0.0


def _load_scenarios(force: bool = False) -> dict:
    global _scenarios_cache, _scenarios_mtime
    with _scenarios_lock:
        try:
            mtime = SCENARIOS_FILE.stat().st_mtime
        except FileNotFoundError:
            raise HTTPException(
                status_code=500,
                detail=f"scenarios.json not found at {SCENARIOS_FILE}",
            )
        if force or mtime != _scenarios_mtime:
            with open(SCENARIOS_FILE, "r", encoding="utf-8") as f:
                _scenarios_cache = json.load(f)
            _scenarios_mtime = mtime
            print(f"[REMOTE_DRIVER] scenarios.json reloaded ({mtime})")
        return _scenarios_cache


# ══════════════════════════════════════════════════════════════════════════
#  REPORT MODEL — what the on-device executor POSTs back after a run
# ══════════════════════════════════════════════════════════════════════════


class ScreenshotRef(BaseModel):
    label: str = ""
    image_b64: Optional[str] = None


class RunReport(BaseModel):
    run_id: str
    scenario: str
    status: str = Field(..., description="passed | failed")
    environment: str = "staging"
    app_package: Optional[str] = None
    device_model: Optional[str] = None
    device_label: Optional[str] = None
    os_version: Optional[str] = None
    steps_completed: int = 0
    total_steps: int = 0
    failed_step: Optional[str] = None
    failure_reason: Optional[str] = None
    order_id: Optional[str] = None
    started_at: Optional[str] = None
    ended_at: Optional[str] = None
    duration_seconds: Optional[float] = None
    screenshots: list[ScreenshotRef] = []
    extra: dict[str, Any] = {}


# ══════════════════════════════════════════════════════════════════════════
#  RESILIENT WEBHOOK SENDER — same pattern as test_runner.py
# ══════════════════════════════════════════════════════════════════════════


def _make_session() -> requests.Session:
    s = requests.Session()
    retry = requests.adapters.Retry(total=0, raise_on_status=False)
    s.mount("https://", requests.adapters.HTTPAdapter(max_retries=retry))
    s.mount("http://", requests.adapters.HTTPAdapter(max_retries=retry))
    return s


_SESSION = _make_session()


def _enqueue_fallback(payload: dict, error: str) -> None:
    entry = {
        "queued_at": datetime.now(timezone.utc).isoformat(),
        "last_error": error,
        "payload": payload,
    }
    try:
        with open(FALLBACK_FILE, "a", encoding="utf-8") as fh:
            fh.write(json.dumps(entry) + "\n")
        print(f"[REMOTE_DRIVER] 📥 queued to {FALLBACK_FILE.name}")
    except Exception as e:
        print(f"[REMOTE_DRIVER] ❌ could not write fallback queue: {e}")


def _forward_to_n8n(payload: dict, label: str) -> bool:
    """POST payload to n8n with exponential back-off. Never raises."""
    headers = {"Content-Type": "application/json"}
    last_err = None

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            resp = _SESSION.post(
                N8N_RESULTS_URL,
                json=payload,
                headers=headers,
                timeout=(CONNECT_TIMEOUT, WRITE_TIMEOUT),
            )
            if resp.status_code < 300:
                print(f"[REMOTE_DRIVER] ✅ {label} — HTTP {resp.status_code} (attempt {attempt})")
                return True
            last_err = f"HTTP {resp.status_code}"
            print(f"[REMOTE_DRIVER] ⚠️  {label} — {last_err} (attempt {attempt}/{MAX_RETRIES})")
        except Exception as e:
            last_err = str(e)
            print(f"[REMOTE_DRIVER] ⚠️  {label} — attempt {attempt}/{MAX_RETRIES}: {e}")

        if attempt < MAX_RETRIES:
            wait = BACKOFF_BASE**attempt
            time.sleep(wait)

    print(f"[REMOTE_DRIVER] ❌ {label} — all attempts failed, queuing to disk.")
    _enqueue_fallback(payload, last_err or "unknown error")
    return False


def drain_fallback_queue() -> None:
    """Best-effort replay of anything queued from a prior outage. Called on
    startup and can be triggered again by re-POSTing to /report if desired."""
    if not FALLBACK_FILE.exists():
        return
    lines = FALLBACK_FILE.read_text(encoding="utf-8").splitlines()
    if not lines:
        return
    print(f"[REMOTE_DRIVER] 🔁 draining {len(lines)} queued report(s) …")
    remaining = []
    for line in lines:
        try:
            entry = json.loads(line)
            ok = _forward_to_n8n(entry["payload"], "drain-replay")
            if not ok:
                remaining.append(line)
        except Exception:
            remaining.append(line)
    if remaining:
        FALLBACK_FILE.write_text("\n".join(remaining) + "\n", encoding="utf-8")
    else:
        FALLBACK_FILE.unlink(missing_ok=True)


# ══════════════════════════════════════════════════════════════════════════
#  VARIABLE SUBSTITUTION HELPER
#  (kept here too, server-side, as a convenience for any caller that wants
#   pre-substituted steps instead of doing it on-device — the Kotlin
#   executor does its own {{var}} substitution and can ignore this.)
# ══════════════════════════════════════════════════════════════════════════

_VAR_PATTERN = re.compile(r"\{\{(\w+)\}\}")


def _substitute(obj: Any, variables: dict) -> Any:
    if isinstance(obj, str):
        return _VAR_PATTERN.sub(lambda m: str(variables.get(m.group(1), m.group(0))), obj)
    if isinstance(obj, dict):
        return {k: _substitute(v, variables) for k, v in obj.items()}
    if isinstance(obj, list):
        return [_substitute(v, variables) for v in obj]
    return obj


# ══════════════════════════════════════════════════════════════════════════
#  FASTAPI APP
# ══════════════════════════════════════════════════════════════════════════

@asynccontextmanager
async def _lifespan(_app: FastAPI):
    _load_scenarios(force=True)
    drain_fallback_queue()
    yield


app = FastAPI(title="SkillSpark Remote Driver", version="1.0.0", lifespan=_lifespan)


@app.get("/")
def health():
    return {"status": "online", "service": "SkillSpark Remote Driver (Accessibility path)"}


@app.get("/scenario/{name}")
def get_scenario(name: str, env: str = "staging", substitute: bool = False):
    """
    Returns the selector map + vars for `name` in `env`.

    By default {{var}} placeholders are left intact and `vars` is returned
    separately, so the on-device Kotlin executor can substitute per-step at
    run time (its designed behaviour). Pass ?substitute=true if a caller
    wants the steps pre-substituted server-side instead.
    """
    scenarios = _load_scenarios()
    scenario = scenarios.get(name)
    if scenario is None:
        raise HTTPException(
            status_code=404,
            detail=f"Unknown scenario '{name}'. Available: {list(scenarios.keys())}",
        )

    env_cfg = scenario.get("environments", {}).get(env)
    if env_cfg is None:
        raise HTTPException(
            status_code=404,
            detail=f"Unknown environment '{env}' for scenario '{name}'. "
            f"Available: {list(scenario.get('environments', {}).keys())}",
        )

    steps = scenario["steps"]
    if substitute:
        steps = _substitute(steps, env_cfg.get("vars", {}))

    return {
        "scenario": name,
        "environment": env,
        "feature": scenario.get("feature"),
        "app_package": env_cfg["app_package"],
        "app_activity": env_cfg.get("app_activity"),
        "vars": env_cfg.get("vars", {}),
        "total_steps": scenario.get("total_steps", len(steps)),
        "steps": steps,
    }


@app.get("/scenarios")
def list_scenarios():
    """Lightweight listing for a Flutter picker UI — names + envs, no steps."""
    scenarios = _load_scenarios()
    return {
        name: {
            "feature": s.get("feature"),
            "environments": list(s.get("environments", {}).keys()),
            "total_steps": s.get("total_steps"),
        }
        for name, s in scenarios.items()
    }


@app.post("/report")
def report(payload: RunReport):
    """
    On-device executor calls this when a scenario run finishes (pass or
    fail). Forwarded to n8n with retry; queued to disk if n8n is
    unreachable so nothing is lost.
    """
    body = payload.model_dump()
    body["reported_at"] = datetime.now(timezone.utc).isoformat()
    body["source"] = "accessibility_native_driver"

    forwarded = _forward_to_n8n(body, f"{payload.scenario}[{payload.run_id}]")
    return {"received": True, "forwarded": forwarded}


@app.post("/scenarios/reload")
def reload_scenarios():
    """Force a scenarios.json reload without waiting for the mtime check."""
    _load_scenarios(force=True)
    return {"reloaded": True}


# ══════════════════════════════════════════════════════════════════════════
#  ENTRY POINT
# ══════════════════════════════════════════════════════════════════════════

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=PORT, reload=False)
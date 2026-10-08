# ruff: noqa: INP001
"""Credit card fraud detection: generate transactions and score them live.

A port of ccfraud/streamlit_app.py. The page opens on a status panel: the
connected project, how many merchants, accounts and cards were loaded from the
merchant_details, account_details and card_details feature groups (v1), the
state of the `ccfraud` model deployment (with a button that starts it when it
is not running), and whether the streaming jobs `ccfraud-streaming-aggs` and
`ccfraud-transactions` are running (read only).

A form generates a batch of synthetic transactions (10-500, default 50) for
real cards and merchants: lognormal(3.5, 1.2) amounts, an IP address from the
card's home country, card present with p=0.3, timestamps now (UTC) 1 ms apart,
and t_ids as microseconds since the epoch. The fraud rate (0-10 %, default
0.5 %) is honoured: round(n * rate) transactions are injected as chain attacks
(one card, a foreign IP, card not present, amounts 1-3 then 5-15 then
35-49.99) and kept as the ground-truth column `injected_fraud`. When "write"
is on, the batch is inserted into credit_card_transactions v1 (and so flows
through Kafka into the Spark streaming job) and the injected fraud is labelled
in cc_fraud v1 ("Chain attack (injected by app)"). Every transaction is scored
by the `ccfraud` deployment (8 concurrent calls; a per-row failure shows as
"error"; no simulated predictions when the deployment is down), and each card's
live sliding-window features are read from the online store of
cc_trans_aggs_fg v2. A run is a background job: POST api/runs returns an id,
GET api/runs/{id} reports stage and percent, then the results: metric tiles
(total, predicted fraud, legitimate, predicted fraud rate, injected fraud
caught), the transactions table with predicted fraud highlighted, and a
"Suspected fraud" table.

A custom Hopsworks app: one process serving a JSON API under /api and a static
JavaScript UI, bound to 0.0.0.0:$APP_PORT, with /health for the readiness probe.
The UI calls the API with relative URLs, so the Hopsworks proxy mount
(/hopsworks-api/pythonapp/<project>/<app>/) works without the app knowing it.
The repo's ccfraud package (synth_transactions) is imported from the parent
directory of this app.
"""

from __future__ import annotations

import logging
import math
import os
import sys
import threading
import time
import traceback
import uuid
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta, timezone
from pathlib import Path

from fastapi import FastAPI, HTTPException
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field

APP_DIR = Path(__file__).resolve().parent
STATIC = APP_DIR / "static"
CCFRAUD_DIR = APP_DIR.parent  # mlfs-book/ccfraud, holds the ccfraud package
if str(CCFRAUD_DIR) not in sys.path:
    sys.path.insert(0, str(CCFRAUD_DIR))

DEPLOYMENT = "ccfraud"
JOBS = ["ccfraud-streaming-aggs", "ccfraud-transactions"]
TRANSACTIONS_FG = ("credit_card_transactions", 1)
LABELS_FG = ("cc_fraud", 1)
AGGS_FG = ("cc_trans_aggs_fg", 2)
AGG_COLUMNS = [
    "num_trans_last_10_mins",
    "sum_trans_last_10_mins",
    "num_trans_last_hour",
    "sum_trans_last_hour",
    "max_trans_last_hour",
    "num_ip_addresses_last_hour",
]
FRAUD_EXPLANATION = "Chain attack (injected by app)"
PREDICT_WORKERS = 8
MAX_RUNS_KEPT = 20

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("ccfraud-app")

app = FastAPI(title="Credit card fraud detection", docs_url=None, redoc_url=None)
app.mount("/static", StaticFiles(directory=STATIC), name="static")


# ---------------------------------------------------------------------------
# Hopsworks handles: created on first use, so /health answers immediately.
# ---------------------------------------------------------------------------

_login_lock = threading.Lock()
_handles: dict = {}


def _export_libhdfs_tls() -> None:
    """Point libhdfs (delta-rs offline writes) at the TLS material the SDK wrote on login.

    App pods do not export LIBHDFS_* (jobs and terminals do), so a Delta insert
    from the app fails with "Connection to HopsFS failed". The login writes the
    project user's CA chain, certificate and key to /tmp; this only fills in
    variables that are unset.
    """
    for var, path in [
        ("LIBHDFS_ROOT_CA_BUNDLE", "/tmp/ca_chain.pem"),
        ("LIBHDFS_CLIENT_CERTIFICATE", "/tmp/client_cert.pem"),
        ("LIBHDFS_CLIENT_KEY", "/tmp/client_key.pem"),
    ]:
        if not os.environ.get(var) and Path(path).exists():
            os.environ[var] = path
            log.info("Set %s=%s for HopsFS (Delta) writes", var, path)


def _project():
    with _login_lock:
        if "project" not in _handles:
            import hopsworks

            project = hopsworks.login()
            _export_libhdfs_tls()
            _handles["project"] = project
            _handles["fs"] = project.get_feature_store()
        return _handles["project"]


def _fs():
    _project()
    return _handles["fs"]


def _fg(name: str, version: int):
    key = f"fg:{name}:{version}"
    if key not in _handles:
        _handles[key] = _fs().get_feature_group(name, version=version)
    return _handles[key]


def _deployment():
    if "deployment" not in _handles:
        _handles["deployment"] = _project().get_model_serving().get_deployment(DEPLOYMENT)
    return _handles["deployment"]


def _st():
    """The repo's synthetic transaction helpers (polars, numpy, hsfs; Faker stays unloaded)."""
    from ccfraud import synth_transactions as st

    return st


def utcnow() -> datetime:
    """Naive UTC timestamp; hsfs serializes naive datetimes as UTC."""
    return datetime.now(timezone.utc).replace(tzinfo=None)


# ---------------------------------------------------------------------------
# Entities: cards (with home country), accounts, merchants, loaded once.
# ---------------------------------------------------------------------------


class _Entities:
    def __init__(self):
        self.lock = threading.Lock()
        self.ready = threading.Event()
        self.state = "idle"  # idle | loading | ready | error
        self.error: str | None = None
        self.data: dict | None = None

    def ensure_loading(self) -> None:
        with self.lock:
            if self.state in ("loading", "ready"):
                return
            self.state, self.error = "loading", None
            self.ready.clear()
        threading.Thread(target=self._load, name="entities", daemon=True).start()

    def _load(self) -> None:
        try:
            import numpy as np
            import polars as pl

            st = _st()
            fs = _fs()
            t0 = time.monotonic()
            merchants = pl.from_pandas(fs.get_feature_group("merchant_details", version=1).read(dataframe_type="pandas"))
            accounts = pl.from_pandas(fs.get_feature_group("account_details", version=1).read(dataframe_type="pandas"))
            cards = pl.from_pandas(fs.get_feature_group("card_details", version=1).read(dataframe_type="pandas"))
            if "home_country" not in accounts.columns:
                accounts = st.assign_cardholder_home_locations(accounts, seed=42)
            # card_details can hold several versions of a card: keep the latest one
            cards = cards.sort("last_modified").unique(subset=["cc_num"], keep="last")
            cards = cards.join(accounts.select(["account_id", "home_country"]), on="account_id", how="left")
            cards = cards.with_columns(pl.col("home_country").fill_null("United States"))
            # A stable order, so the same seed picks the same cards and merchants after a restart
            cards = cards.sort("cc_num")
            merchants = merchants.sort("merchant_id")
            self.data = {
                "cc_nums": cards["cc_num"].to_numpy(),
                "account_ids": cards["account_id"].to_numpy(),
                "home_countries": cards["home_country"].to_numpy(),
                "merchant_ids": merchants["merchant_id"].to_numpy(),
                "countries": np.array(list(st.COUNTRY_IP_RANGES.keys())),
                "counts": {"merchants": merchants.height, "accounts": accounts.height, "cards": cards.height},
            }
            with self.lock:
                self.state = "ready"
            log.info("Loaded entities %s in %.1fs", self.data["counts"], time.monotonic() - t0)
        except Exception as e:  # noqa: BLE001
            log.exception("Loading entities failed")
            with self.lock:
                self.state, self.error = "error", f"{type(e).__name__}: {e}"
        finally:
            self.ready.set()

    def get(self, timeout: float = 300) -> dict:
        self.ensure_loading()
        self.ready.wait(timeout)
        if self.state != "ready":
            raise RuntimeError(f"Could not load cards, accounts and merchants: {self.error or 'timed out'}")
        return self.data


ENTITIES = _Entities()


# ---------------------------------------------------------------------------
# Deployment start (non-blocking)
# ---------------------------------------------------------------------------

_deploy = {"starting": False, "error": None}


def _start_deployment() -> None:
    try:
        _deployment().start(await_running=900)
    except Exception as e:  # noqa: BLE001
        log.exception("Starting the deployment failed")
        _deploy["error"] = f"{type(e).__name__}: {e}"
    finally:
        _deploy["starting"] = False


def _deployment_status() -> dict:
    try:
        dep = _deployment()
        status = str(dep.get_state().status)
        running = status.lower() == "running"
        if running:
            _deploy["error"] = None
        return {
            "name": DEPLOYMENT,
            "status": status,
            "running": running,
            "starting": _deploy["starting"],
            "error": _deploy["error"],
        }
    except Exception as e:  # noqa: BLE001
        _handles.pop("deployment", None)
        return {"name": DEPLOYMENT, "status": "Unavailable", "running": False, "starting": False,
                "error": f"{type(e).__name__}: {e}"}


def _job_status(name: str) -> dict:
    try:
        key = f"job:{name}"
        if key not in _handles:
            _handles[key] = _project().get_job_api().get_job(name)
        job = _handles[key]
        if job is None:
            _handles.pop(key, None)
            return {"name": name, "state": "NOT FOUND", "running": False}
        try:
            state = str(job.get_state())
        except Exception:  # noqa: BLE001
            state = "NO EXECUTIONS"
        return {"name": name, "state": state, "running": state == "RUNNING"}
    except Exception as e:  # noqa: BLE001
        return {"name": name, "state": "UNKNOWN", "running": False, "error": f"{type(e).__name__}: {e}"}


# ---------------------------------------------------------------------------
# Routes
# ---------------------------------------------------------------------------


@app.get("/health")
def health() -> dict:
    """Readiness: the process serves; the Hopsworks connection is checked by the API routes."""
    return {"status": "ok"}


@app.get("/")
def index() -> FileResponse:
    """The UI."""
    return FileResponse(STATIC / "index.html")


@app.get("/api/status")
def status() -> dict:
    """Project, entity counts (loaded in the background on first call), deployment and job states."""
    try:
        project = _project()
    except Exception as e:  # noqa: BLE001
        raise HTTPException(status_code=503, detail=f"Could not connect to Hopsworks: {e}") from e
    ENTITIES.ensure_loading()
    entities = {"state": ENTITIES.state, "error": ENTITIES.error}
    if ENTITIES.state == "ready":
        entities.update(ENTITIES.data["counts"])
    return {
        "project": project.name,
        "entities": entities,
        "deployment": _deployment_status(),
        "jobs": [_job_status(name) for name in JOBS],
    }


@app.post("/api/deployment/start")
def start_deployment() -> dict:
    """Starts the model deployment in the background; the UI polls api/status."""
    current = _deployment_status()
    if current["running"]:
        return current
    if not _deploy["starting"]:
        _deploy["starting"], _deploy["error"] = True, None
        threading.Thread(target=_start_deployment, name="deployment-start", daemon=True).start()
    return {**current, "starting": True}


class RunRequest(BaseModel):
    n: int = Field(50, ge=10, le=500, description="Number of transactions")
    fraud_rate_pct: float = Field(0.5, ge=0, le=10, description="Injected fraud, percent of the batch")
    seed: int = Field(42, ge=0, le=2**31 - 1)
    write: bool = Field(False, description="Also write the generated transactions to credit_card_transactions")


_runs: dict[str, dict] = {}
_runs_lock = threading.Lock()


@app.post("/api/runs")
def create_run(request: RunRequest) -> dict:
    """Starts a generate-and-predict run in a background thread."""
    run_id = uuid.uuid4().hex[:12]
    run = {
        "id": run_id,
        "status": "running",
        "stage": "Starting",
        "percent": 0,
        "request": request.model_dump(),
        "warnings": [],
        "error": None,
        "result": None,
        "started_at": utcnow().isoformat() + "Z",
    }
    with _runs_lock:
        _runs[run_id] = run
        for old in list(_runs)[:-MAX_RUNS_KEPT]:
            _runs.pop(old, None)
    threading.Thread(target=_execute_run, args=(run, request), name=f"run-{run_id}", daemon=True).start()
    return {"id": run_id}


@app.get("/api/runs/{run_id}")
def get_run(run_id: str) -> dict:
    """Progress of a run and, when done, its results."""
    run = _runs.get(run_id)
    if run is None:
        raise HTTPException(status_code=404, detail=f"No run {run_id} (the app may have restarted).")
    return run


# ---------------------------------------------------------------------------
# A run: generate, write, predict, look up the live aggregates.
# ---------------------------------------------------------------------------


def _progress(run: dict, percent: int, stage: str) -> None:
    run["percent"], run["stage"] = percent, stage


def _generate(data: dict, n: int, fraud_rate: float, seed: int):
    """n transactions for real cards and merchants; round(n * rate) of them chain-attack fraud."""
    import numpy as np
    import pandas as pd

    st = _st()
    rng = np.random.default_rng(seed)
    cc_nums, account_ids, homes = data["cc_nums"], data["account_ids"], data["home_countries"]
    merchant_ids, countries = data["merchant_ids"], data["countries"]

    card_idx = rng.integers(0, len(cc_nums), size=n)
    now = utcnow()
    start = int(time.time() * 1_000_000)  # t_ids: microseconds since the epoch, like the generator job
    rows = []
    for i, c in enumerate(card_idx):
        rows.append({
            "t_id": start + i,
            "cc_num": str(cc_nums[c]),
            "account_id": str(account_ids[c]),
            "merchant_id": str(rng.choice(merchant_ids)),
            "amount": round(float(rng.lognormal(mean=3.5, sigma=1.2)), 2),
            "ip_address": st.generate_ip_for_country(str(homes[c]), seed=int(rng.integers(2**31))),
            "card_present": bool(rng.random() < 0.3),
            "ts": now + timedelta(milliseconds=i),
            "injected_fraud": False,
            "home_country": str(homes[c]),
            "ip_country": str(homes[c]),
        })

    n_fraud = max(0, int(math.floor(n * fraud_rate + 0.5)))
    if n_fraud:
        positions = sorted(rng.choice(n, size=n_fraud, replace=False).tolist())
        k = 0
        while k < len(positions):
            size = min(len(positions) - k, int(rng.integers(3, 9)))
            c = int(rng.integers(len(cc_nums)))
            home = str(homes[c])
            foreign = [x for x in countries if x != home]
            country = str(rng.choice(foreign))
            ip = st.generate_ip_for_country(country, seed=int(rng.integers(2**31)))
            for j in range(size):
                phase = (3 * j) // size  # small "testing" amounts first, then larger ones
                low, high = [(1.0, 3.0), (5.0, 15.0), (35.0, 49.99)][phase]
                rows[positions[k + j]].update({
                    "cc_num": str(cc_nums[c]),
                    "account_id": str(account_ids[c]),
                    "amount": round(float(rng.uniform(low, high)), 2),
                    "ip_address": ip,
                    "card_present": False,
                    "injected_fraud": True,
                    "home_country": home,
                    "ip_country": country,
                })
            k += size

    df = pd.DataFrame(rows)
    df["t_id"] = df["t_id"].astype("int64")
    df["ts"] = pd.to_datetime(df["ts"])
    return df


def _predict_one(dep, row: dict) -> tuple[bool, float]:
    """(prediction, latency in ms) of one transaction. The latency is the round trip of the
    predict request as seen by the app: online feature lookup, transformations and the model."""
    inputs = [[row["cc_num"], float(row["amount"]), row["merchant_id"], row["ip_address"],
               bool(row["card_present"]), int(row["t_id"])]]
    t0 = time.perf_counter()
    result = dep.predict(inputs=inputs)
    latency_ms = (time.perf_counter() - t0) * 1000
    preds = result.get("predictions") if isinstance(result, dict) else None
    if not preds:
        raise ValueError(f"unexpected response: {result!r}"[:300])
    return bool(preds[0]), latency_ms


def _latency_summary(latencies_ms: list[float]) -> dict | None:
    """Percentiles of the prediction round trips, for the run summary."""
    if not latencies_ms:
        return None
    xs = sorted(latencies_ms)
    q = lambda f: xs[min(len(xs) - 1, int(round(f * (len(xs) - 1))))]  # noqa: E731
    return {"count": len(xs), "mean": sum(xs) / len(xs), "p50": q(0.5), "p95": q(0.95),
            "p99": q(0.99), "min": xs[0], "max": xs[-1]}


def _lookup_aggs(cc_nums: list[str]) -> dict:
    fg = _fg(*AGGS_FG)
    df = fg.select_all().filter(fg.cc_num.isin(cc_nums)).read(online=True, dataframe_type="pandas")
    out = {}
    for rec in df.to_dict(orient="records"):
        vals = {}
        for col in AGG_COLUMNS:
            v = rec.get(col)
            vals[col] = None if v is None or (isinstance(v, float) and math.isnan(v)) else float(v)
        et = rec.get("event_time")
        vals["event_time"] = None if et is None or str(et) == "NaT" else str(et)
        out[str(rec["cc_num"])] = vals
    return out


def _insert_with_retry(fg, df, run: dict, what: str, attempts: int = 5) -> None:
    """Insert, retrying Delta commit conflicts.

    The ccfraud-transactions job commits to the same Delta table every minute, so
    an app commit can lose the optimistic-concurrency race ("a concurrent
    transaction added new data"). The insert is an upsert on the primary key,
    so rerunning it is safe.
    """
    import random

    for attempt in range(1, attempts + 1):
        try:
            fg.insert(df)
            return
        except Exception as e:  # noqa: BLE001
            msg = str(e).lower()
            if attempt == attempts or not ("concurrent" in msg or "commit failed" in msg):
                raise
            wait = min(8.0, 1.5 * attempt) + random.uniform(0, 1.5)
            log.warning("%s: commit conflict (attempt %d/%d), retrying in %.1fs", what, attempt, attempts, wait)
            run["stage"] = f"{what}: commit conflict with a concurrent writer, retrying ({attempt}/{attempts - 1})"
            time.sleep(wait)


def _execute_run(run: dict, req: RunRequest) -> None:
    try:
        _progress(run, 3, "Checking the model deployment")
        dep_status = _deployment_status()
        if not dep_status["running"]:
            raise RuntimeError(
                f"Model deployment '{DEPLOYMENT}' is not running (status: {dep_status['status']}). "
                "Start it from the status panel and run again; predictions are never simulated."
            )
        dep = _deployment()

        _progress(run, 8, "Loading cards, accounts and merchants")
        data = ENTITIES.get()

        _progress(run, 20, f"Generating {req.n} transactions")
        df = _generate(data, req.n, req.fraud_rate_pct / 100.0, req.seed)
        n_injected = int(df["injected_fraud"].sum())

        written = False
        if req.write:
            _progress(run, 30, "Writing to credit_card_transactions")
            cols = ["t_id", "cc_num", "account_id", "merchant_id", "amount", "ip_address", "card_present", "ts"]
            try:
                _insert_with_retry(_fg(*TRANSACTIONS_FG), df[cols].copy(), run, "Writing to credit_card_transactions")
                written = True
            except Exception as e:  # noqa: BLE001
                log.exception("Writing transactions failed")
                run["warnings"].append(f"Could not write transactions to credit_card_transactions: {e}")
            if written and n_injected:
                _progress(run, 38, "Writing fraud labels to cc_fraud")
                labels = df.loc[df["injected_fraud"], ["t_id", "cc_num", "ts"]].copy()
                labels.insert(2, "explanation", FRAUD_EXPLANATION)
                try:
                    _insert_with_retry(_fg(*LABELS_FG), labels[["t_id", "cc_num", "explanation", "ts"]], run, "Writing fraud labels to cc_fraud")
                except Exception as e:  # noqa: BLE001
                    log.exception("Writing labels failed")
                    run["warnings"].append(f"Could not write fraud labels to cc_fraud: {e}")

        records = df.to_dict(orient="records")
        total = len(records)
        preds: list = [None] * total
        errors: list = [None] * total
        latencies: list = [None] * total
        _progress(run, 45, f"Predicting 0/{total}")
        done = 0
        with ThreadPoolExecutor(max_workers=PREDICT_WORKERS) as pool:
            futures = {pool.submit(_predict_one, dep, r): i for i, r in enumerate(records)}
            for fut in as_completed(futures):
                i = futures[fut]
                try:
                    preds[i], latencies[i] = fut.result()
                except Exception as e:  # noqa: BLE001
                    errors[i] = f"{type(e).__name__}: {e}"[:300]
                done += 1
                _progress(run, 45 + int(45 * done / total), f"Predicting {done}/{total}")
        n_errors = sum(e is not None for e in errors)
        if n_errors:
            first = next(e for e in errors if e)
            run["warnings"].append(f"{n_errors} of {total} predictions failed; first error: {first}")

        _progress(run, 93, "Reading live aggregates from cc_trans_aggs_fg")
        aggs = {}
        try:
            aggs = _lookup_aggs(sorted({r["cc_num"] for r in records}))
        except Exception as e:  # noqa: BLE001
            log.exception("Online aggregate lookup failed")
            run["warnings"].append(f"Could not read live aggregates from cc_trans_aggs_fg v2: {e}")

        rows = []
        for i, r in enumerate(records):
            a = aggs.get(r["cc_num"], {})
            rows.append({
                "t_id": int(r["t_id"]),
                "cc_num": r["cc_num"],
                "merchant_id": r["merchant_id"],
                "amount": float(r["amount"]),
                "ip_address": r["ip_address"],
                "ip_country": r["ip_country"],
                "home_country": r["home_country"],
                "card_present": bool(r["card_present"]),
                "ts": r["ts"].isoformat() + "Z",
                "injected_fraud": bool(r["injected_fraud"]),
                "prediction": "error" if errors[i] else ("fraud" if preds[i] else "legit"),
                "error": errors[i],
                "latency_ms": latencies[i],
                **{col: a.get(col) for col in AGG_COLUMNS},
                "aggs_event_time": a.get("event_time"),
            })
        n_fraud = sum(r["prediction"] == "fraud" for r in rows)
        n_legit = sum(r["prediction"] == "legit" for r in rows)
        caught = sum(r["injected_fraud"] and r["prediction"] == "fraud" for r in rows)
        run["result"] = {
            "summary": {
                "total": total,
                "predicted_fraud": n_fraud,
                "legitimate": n_legit,
                "errors": n_errors,
                "predicted_fraud_rate": n_fraud / (total - n_errors) if total - n_errors else None,
                "injected_fraud": n_injected,
                "injected_caught": caught,
                "written": written,
                "cards_with_aggregates": sum(1 for c in {r["cc_num"] for r in rows} if c in aggs),
                "cards": len({r["cc_num"] for r in rows}),
                # Round trip of each predict request (ms), as seen by the app; the requests
                # are sent PREDICT_WORKERS at a time
                "latency_ms": _latency_summary([x for x in latencies if x is not None]),
                "predict_workers": PREDICT_WORKERS,
            },
            "rows": rows,
        }
        _progress(run, 100, "Done")
        run["status"] = "done"
    except Exception as e:  # noqa: BLE001
        log.error("Run %s failed: %s\n%s", run["id"], e, traceback.format_exc())
        run["error"] = str(e)
        run["status"] = "error"
    finally:
        run["finished_at"] = utcnow().isoformat() + "Z"


if __name__ == "__main__":
    import uvicorn

    log.info("Serving from %s; ccfraud package dir %s (exists: %s)", APP_DIR, CCFRAUD_DIR,
             (CCFRAUD_DIR / "ccfraud" / "synth_transactions.py").exists())
    uvicorn.run(app, host="0.0.0.0", port=int(os.environ.get("APP_PORT", "8080")))

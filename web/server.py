from fastapi import FastAPI
from fastapi.responses import HTMLResponse, JSONResponse
from apscheduler.schedulers.background import BackgroundScheduler
import subprocess
import sqlite3
from pathlib import Path
from datetime import datetime

from src.predictor.predictor import compute_dashboard_combined_v2

BASE_DIR = Path(__file__).resolve().parent.parent
LOG_DIR = BASE_DIR / "logs"
LOG_DIR.mkdir(exist_ok=True)
UPDATE_LOG = LOG_DIR / "render_update.log"

scheduler = BackgroundScheduler()
app = FastAPI()


def run_update():
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    try:
        result = subprocess.run(
            ["bash", "update_all_traffic.sh"],
            cwd=str(BASE_DIR),
            capture_output=True,
            text=True,
            timeout=300,
        )

        with open(UPDATE_LOG, "a", encoding="utf-8") as f:
            f.write(f"\n===== {now} =====\n")
            f.write(f"returncode: {result.returncode}\n")
            f.write("STDOUT:\n")
            f.write(result.stdout or "")
            f.write("\nSTDERR:\n")
            f.write(result.stderr or "")
            f.write("\n")

    except Exception as e:
        with open(UPDATE_LOG, "a", encoding="utf-8") as f:
            f.write(f"\n===== {now} =====\n")
            f.write(f"EXCEPTION: {e}\n")


@app.on_event("startup")
def startup_event():
    run_update()
    if not scheduler.running:
        scheduler.add_job(
            run_update,
            "interval",
            minutes=5,
            id="flight_update",
            replace_existing=True,
        )
        scheduler.start()


@app.on_event("shutdown")
def shutdown_event():
    if scheduler.running:
        scheduler.shutdown(wait=False)


@app.get("/api/dashboard")
def dashboard():
    try:
        return compute_dashboard_combined_v2()
    except Exception as e:
        return JSONResponse(
            status_code=200,
            content={
                "updated": "--",
                "current": {
                    "probability": 0,
                    "color": "green",
                    "label": "אין נתונים",
                    "flights": 0,
                    "window": "45 הדקות הקרובות",
                    "quality": "נמוכה",
                    "strong_count": 0
                },
                "top_windows": [],
                "next_attention_window": None,
                "daily": [],
                "error": str(e)
            }
        )


@app.get("/api/db_check")
def db_check():
    conn = sqlite3.connect(str(BASE_DIR / "data" / "flights.db"))
    c = conn.cursor()
    flights_current_rows = c.execute("select count(*) from flights_current").fetchone()[0]
    departures_current_rows = c.execute("select count(*) from departures_current").fetchone()[0]
    conn.close()
    return {
        "flights_current_rows": flights_current_rows,
        "departures_current_rows": departures_current_rows
    }


@app.get("/api/debug_db")
def debug_db():
    import os

    db = str(BASE_DIR / "data" / "flights.db")

    try:
        conn = sqlite3.connect(db)
        c = conn.cursor()

        tables = [
            row[0]
            for row in c.execute(
                "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name"
            ).fetchall()
        ]

        out = {
            "db_path": os.path.abspath(db),
            "tables": tables,
        }

        for table_name in [
            "flights_current",
            "departures_current",
            "traffic_windows_history",
            "flights_ben_gurion_raw",
            "flights_ben_gurion_departures_raw"
        ]:
            try:
                count = c.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
                out[table_name] = count
            except Exception as e:
                out[table_name] = f"ERROR: {e}"

        try:
            out["latest_arrivals_scraped_at"] = c.execute(
                "SELECT MAX(scraped_at) FROM flights_ben_gurion_raw"
            ).fetchone()[0]
        except Exception as e:
            out["latest_arrivals_scraped_at"] = f"ERROR: {e}"

        try:
            out["latest_departures_scraped_at"] = c.execute(
                "SELECT MAX(scraped_at) FROM flights_ben_gurion_departures_raw"
            ).fetchone()[0]
        except Exception as e:
            out["latest_departures_scraped_at"] = f"ERROR: {e}"

        conn.close()
        return out

    except Exception as e:
        return {
            "error": str(e),
            "db_path": os.path.abspath(db)
        }


@app.get("/api/update_log")
def update_log():
    if UPDATE_LOG.exists():
        return {"log": UPDATE_LOG.read_text(encoding="utf-8")[-12000:]}
    return {"log": "no log yet"}


@app.get("/", response_class=HTMLResponse)
def index():
    with open(BASE_DIR / "web" / "templates" / "index.html", "r", encoding="utf-8") as f:
        return f.read()

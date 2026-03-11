from fastapi import FastAPI
from fastapi.responses import HTMLResponse, JSONResponse
from apscheduler.schedulers.background import BackgroundScheduler
import subprocess
import sqlite3
from pathlib import Path

from src.predictor.predictor import compute_dashboard_combined

BASE_DIR = Path(__file__).resolve().parent.parent
scheduler = BackgroundScheduler()

app = FastAPI()


def run_update():
    try:
        subprocess.run(
            ["bash", "update_all_traffic.sh"],
            cwd=str(BASE_DIR),
            check=False,
        )
    except Exception as e:
        print("update job failed:", e)


@app.on_event("startup")
def startup_event():
    if not scheduler.running:
        scheduler.add_job(run_update, "interval", minutes=5, id="flight_update", replace_existing=True)
        scheduler.start()


@app.on_event("shutdown")
def shutdown_event():
    if scheduler.running:
        scheduler.shutdown(wait=False)


@app.get("/api/dashboard")
def dashboard():
    try:
        return compute_dashboard_combined()
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


@app.get("/", response_class=HTMLResponse)
def index():
    with open(BASE_DIR / "web" / "templates" / "index.html", "r", encoding="utf-8") as f:
        return f.read()



@app.get("/api/debug_db")
def debug_db():
    import sqlite3, os

    db = "data/flights.db"

    conn = sqlite3.connect(db)
    c = conn.cursor()

    flights_current = c.execute(
        "select count(*) from flights_current"
    ).fetchone()[0]

    departures_current = c.execute(
        "select count(*) from departures_current"
    ).fetchone()[0]

    flights_history = c.execute(
        "select count(*) from flights_history"
    ).fetchone()[0]

    conn.close()

    return {
        "db_path": os.path.abspath(db),
        "flights_current": flights_current,
        "departures_current": departures_current,
        "flights_history": flights_history
    }

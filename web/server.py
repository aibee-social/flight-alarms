from fastapi import FastAPI
from fastapi.responses import HTMLResponse, JSONResponse
from src.predictor.predictor import compute_dashboard, compute_dashboard_combined
from apscheduler.schedulers.background import BackgroundScheduler
import subprocess
app = FastAPI()
 
def run_update():
    try:
        subprocess.run(["bash", "update_all_traffic.sh"], check=True)
    except Exception as e:
        print("update job failed:", e)

scheduler = BackgroundScheduler()
scheduler.add_job(run_update, "interval", minutes=5)
scheduler.start()


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

@app.get("/", response_class=HTMLResponse)
def index():
    with open("web/templates/index.html", "r", encoding="utf-8") as f:
        return f.read()

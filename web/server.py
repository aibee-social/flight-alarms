from fastapi import FastAPI
from fastapi.responses import HTMLResponse, JSONResponse
from src.predictor.predictor import compute_dashboard

app = FastAPI()

@app.get("/api/dashboard")
def dashboard():
    try:
        return compute_dashboard()
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

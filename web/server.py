from fastapi import FastAPI
from fastapi.responses import HTMLResponse
from src.predictor.predictor import compute_dashboard

app = FastAPI()

@app.get("/api/dashboard")
def dashboard():
    return compute_dashboard()

@app.get("/", response_class=HTMLResponse)
def index():
    with open("web/templates/index.html", "r", encoding="utf-8") as f:
        return f.read()

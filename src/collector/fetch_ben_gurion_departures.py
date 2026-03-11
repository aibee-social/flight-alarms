import json
import sqlite3
import time
from pathlib import Path
from typing import List, Dict

from playwright.sync_api import sync_playwright

URL = "https://www.iaa.gov.il/airports/ben-gurion/flight-board/?flightType=departures"
DB_PATH = Path("data/flights.db")
RAW_JSON_PATH = Path("debug/ben_gurion_departures_raw.json")


def ensure_table(conn: sqlite3.Connection) -> None:
    conn.execute("""
    CREATE TABLE IF NOT EXISTS flights_ben_gurion_departures_raw (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        scraped_at TEXT,
        airline TEXT,
        flight_number TEXT,
        origin_city TEXT,
        terminal TEXT,
        scheduled_time TEXT,
        scheduled_date TEXT,
        updated_time TEXT,
        gate_info TEXT
    )
    """)
    conn.commit()


def save_rows(rows: List[Dict]) -> None:
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(DB_PATH)
    ensure_table(conn)

    scraped_at = time.strftime("%Y-%m-%d %H:%M:%S")


    for row in rows:
        conn.execute("""
        INSERT INTO flights_ben_gurion_departures_raw (
            scraped_at, airline, flight_number, origin_city, terminal,
            scheduled_time, scheduled_date, updated_time, gate_info
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            scraped_at,
            row.get("airline"),
            row.get("flight_number"),
            row.get("origin_city"),
            row.get("terminal"),
            row.get("scheduled_time"),
            row.get("scheduled_date"),
            row.get("updated_time"),
            row.get("gate_info"),
        ))

    conn.commit()
    conn.close()


def scrape_rows() -> List[Dict]:
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_page(viewport={"width": 1440, "height": 2200})
        page.goto(URL, wait_until="networkidle", timeout=120000)

        for text in ["אישור", "הבנתי", "סגור", "Accept", "OK"]:
            try:
                page.get_by_text(text, exact=False).click(timeout=1500)
            except Exception:
                pass

        for _ in range(15):
            try:
                btn = page.get_by_text("הצגת תוצאות נוספות", exact=False)
                btn.wait_for(timeout=2000)
                btn.click()
                page.wait_for_timeout(1200)
            except Exception:
                break

        Path("debug").mkdir(exist_ok=True)
        Path("debug/ben_gurion_page.html").write_text(page.content(), encoding="utf-8")

        rows: List[Dict] = []

        all_rows = page.locator("[role='row']")
        count = all_rows.count()

        for i in range(count):
            row = all_rows.nth(i)
            txt = row.inner_text().strip()
            if not txt:
                continue
            if "חברת תעופה" in txt and "סטאטוס" in txt:
                continue

            cells = [c.strip() for c in row.locator("[role='cell']").all_inner_texts()]
            if len(cells) >= 8:
                # הסדר בפועל בהמראות:
                # [airline, flight_number, destination_city, terminal, scheduled_time, scheduled_date, updated_time, gate_info]
                rows.append({
                    "airline": cells[0],
                    "flight_number": cells[1],
                    "origin_city": cells[2],
                    "terminal": cells[3],
                    "scheduled_time": cells[4],
                    "scheduled_date": cells[5],
                    "updated_time": cells[6],
                    "gate_info": cells[7],
                })

        if not rows:
            trs = page.locator("tr")
            tr_count = trs.count()
            for i in range(tr_count):
                txt = trs.nth(i).inner_text().strip()
                if not txt or "חברת תעופה" in txt:
                    continue
                parts = [x.strip() for x in txt.split("\n") if x.strip()]
                if len(parts) >= 8:
                    rows.append({
                        "airline": parts[0],
                        "flight_number": parts[1],
                        "origin_city": parts[2],
                        "terminal": parts[3],
                        "scheduled_time": parts[4],
                        "scheduled_date": parts[5],
                        "updated_time": parts[6],
                        "gate_info": parts[7],
                    })

        browser.close()
        return rows


def main() -> None:
    rows = scrape_rows()
    RAW_JSON_PATH.parent.mkdir(exist_ok=True, parents=True)
    RAW_JSON_PATH.write_text(json.dumps(rows, ensure_ascii=False, indent=2), encoding="utf-8")
    save_rows(rows)
    print(f"Scraped {len(rows)} Ben Gurion arrival rows")
    print(f"Saved raw JSON to {RAW_JSON_PATH}")


if __name__ == "__main__":
    main()

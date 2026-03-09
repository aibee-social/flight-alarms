import sqlite3
import pandas as pd
from datetime import datetime, timedelta
import pytz

DB = "data/flights.db"
ISRAEL = pytz.timezone("Asia/Jerusalem")
WINDOW_MINUTES = 12

STATUS_BASE_WEIGHTS = {
    "estimated": 1.00,
    "delayed": 0.90,
    "scheduled": 0.40,
    "landed": 0.0,
    "unknown": 0.0
}

PASSENGER_AIRLINES = {
    "EL AL", "EL AL ISRAEL AIRLINES",
    "ISRAIR AIRLINES", "ישראייר",
    "ARKIA", "ARKIA ISRAELI AIRLINES",
    "AIR HAIFA",
    "WIZZ AIR", "WIZZ AIR MALTA",
    "ETIHAD AIRWAYS"
}

CARGO_HINTS = ("CHALLENGE", "CARGO", "ICL", "CAL", "DHL", "UPS", "FEDEX", "5C")
CHARTER_HINTS = ("SMARTWINGS", "TRAVAIR", "QS", "TVR")

def airline_weight(airline_name, flight_number):
    airline = (airline_name or "").upper()
    flight = (flight_number or "").upper()

    if any(h in airline for h in CARGO_HINTS) or flight.startswith("5C") or flight.startswith("ICL"):
        return 0.45, "cargo"
    if any(h in airline for h in CHARTER_HINTS) or flight.startswith("QS") or flight.startswith("TVR"):
        return 0.60, "charter"
    if airline in PASSENGER_AIRLINES:
        return 1.00, "passenger"
    return 0.70, "other"

def flight_confidence(row):
    base = STATUS_BASE_WEIGHTS.get((row["status"] or "").lower(), 0.30)
    if base == 0:
        return 0.0

    score = base
    aw, _ = airline_weight(row.get("airline"), row.get("flight_number"))
    score *= aw

    if pd.notna(row.get("eta_utc")):
        score += 0.22
    if pd.notna(row.get("estimated_arrival_utc")):
        score += 0.18
    if pd.notna(row.get("real_arrival_utc")):
        score += 0.15
    if pd.notna(row.get("callsign")) and str(row.get("callsign")).strip():
        score += 0.10
    if pd.notna(row.get("registration")) and str(row.get("registration")).strip():
        score += 0.12

    cnt_updates = int(row.get("cnt_updates", 0) or 0)
    cnt_snapshots = int(row.get("cnt_snapshots", 0) or 0)

    if cnt_updates >= 1:
        score += 0.10
    if cnt_updates >= 2:
        score += 0.08
    if cnt_snapshots >= 3:
        score += 0.05
    if cnt_snapshots >= 8:
        score += 0.05

    weak = (
        (row.get("status") == "scheduled") and
        pd.isna(row.get("estimated_arrival_utc")) and
        pd.isna(row.get("real_arrival_utc")) and
        pd.isna(row.get("eta_utc")) and
        (pd.isna(row.get("callsign")) or not str(row.get("callsign")).strip()) and
        (pd.isna(row.get("registration")) or not str(row.get("registration")).strip()) and
        cnt_updates == 0
    )
    if weak:
        score *= 0.35

    return max(0.0, min(1.35, score))

def load_flights():
    import sqlite3
    from pathlib import Path
    import pandas as pd

    candidates = [
        Path("data/flights.db"),
        Path("src/data/flights.db"),
    ]

    db_path = None
    for candidate in candidates:
        if candidate.exists():
            db_path = candidate
            break

    if db_path is None:
        return pd.DataFrame(columns=[
            "flight_number", "airline", "origin_airport", "origin_city", "status",
            "callsign", "registration",
            "scheduled_arrival_utc", "scheduled_arrival_israel",
            "estimated_arrival_utc", "estimated_arrival_israel",
            "eta_utc", "eta_israel",
            "real_arrival_utc", "real_arrival_israel",
            "effective_arrival_utc", "effective_arrival_israel",
            "confidence"
        ])

    conn = sqlite3.connect(str(db_path))

    query = """
    SELECT
        flight_number,
        airline,
        origin_airport,
        origin_city,
        status,
        callsign,
        registration,
        scheduled_arrival_utc,
        scheduled_arrival_israel,
        estimated_arrival_utc,
        estimated_arrival_israel,
        eta_utc,
        eta_israel,
        real_arrival_utc,
        real_arrival_israel
    FROM flights_current
    WHERE COALESCE(eta_utc, estimated_arrival_utc, scheduled_arrival_utc) IS NOT NULL
    """

    try:
        df = pd.read_sql_query(query, conn)
    finally:
        conn.close()

    if df.empty:
        df["effective_arrival_utc"] = []
        df["effective_arrival_israel"] = []
        df["confidence"] = []
        return df

    df["effective_arrival_utc"] = df[
        ["eta_utc", "estimated_arrival_utc", "scheduled_arrival_utc"]
    ].bfill(axis=1).iloc[:, 0]

    df["effective_arrival_israel"] = df[
        ["eta_israel", "estimated_arrival_israel", "scheduled_arrival_israel"]
    ].bfill(axis=1).iloc[:, 0]

    def _extract_confidence(value):
        import pandas as pd

        if isinstance(value, pd.Series):
            if "confidence" in value.index:
                try:
                    return float(value["confidence"])
                except Exception:
                    return 0.0
            if len(value) > 0:
                try:
                    return float(value.iloc[0])
                except Exception:
                    return 0.0
            return 0.0

        if isinstance(value, dict):
            if "confidence" in value:
                try:
                    return float(value["confidence"])
                except Exception:
                    return 0.0
            if value:
                try:
                    return float(next(iter(value.values())))
                except Exception:
                    return 0.0
            return 0.0

        if isinstance(value, (list, tuple)):
            if len(value) > 0:
                try:
                    return float(value[0])
                except Exception:
                    return 0.0
            return 0.0

        try:
            return float(value)
        except Exception:
            return 0.0

    df["confidence"] = df.apply(
        lambda row: _extract_confidence(flight_confidence(row)),
        axis=1
    )

    return df.sort_values("effective_arrival_utc").reset_index(drop=True)


def color_from_prob(prob):
    if prob < 30:
        return "green"
    elif prob < 60:
        return "orange"
    return "red"

def quality_from_avg_conf(avg_conf):
    if avg_conf >= 0.95:
        return "גבוהה"
    if avg_conf >= 0.7:
        return "בינונית"
    return "נמוכה"

def best_cluster_from_sorted(sub, max_window_minutes=15):
    if sub.empty:
        return {
            "count": 0, "weighted": 0.0, "avg_conf": 0.0, "min_gap": None,
            "start_ts": None, "end_ts": None, "count10": 0
        }

    times = sub["effective_arrival_utc"].tolist()
    confs = sub["confidence"].tolist()

    best = None
    j = 0

    for i in range(len(times)):
        while times[i] - times[j] > max_window_minutes * 60:
            j += 1

        cluster = sub.iloc[j:i+1].copy()
        count = len(cluster)
        weighted = float(cluster["confidence"].sum())
        avg_conf = float(cluster["confidence"].mean())
        start_ts = int(cluster["effective_arrival_utc"].min())
        end_ts = int(cluster["effective_arrival_utc"].max())

        min_gap = None
        if count >= 2:
            ctimes = cluster["effective_arrival_utc"].tolist()
            gaps = [ctimes[k+1] - ctimes[k] for k in range(len(ctimes)-1)]
            if gaps:
                min_gap = round(min(gaps) / 60.0, 1)

        # כמה יש בתוך 10 דקות בתוך אותו cluster
        count10 = 0
        ctimes = cluster["effective_arrival_utc"].tolist()
        left = 0
        for right in range(len(ctimes)):
            while ctimes[right] - ctimes[left] > 10 * 60:
                left += 1
            count10 = max(count10, right - left + 1)

        score = weighted + (count * 0.7) + (count10 * 1.2)
        if min_gap is not None and min_gap <= 5:
            score += 0.8

        candidate = {
            "score": score,
            "count": count,
            "weighted": weighted,
            "avg_conf": avg_conf,
            "min_gap": min_gap,
            "start_ts": start_ts,
            "end_ts": end_ts,
            "count10": count10
        }

        if best is None or candidate["score"] > best["score"]:
            best = candidate

    return best

def probability_from_cluster(count, weighted, min_gap, avg_conf, count10, minutes_ahead=0):
    if count <= 0:
        prob = 8
    elif count == 1:
        prob = 10 + round(weighted * 4)
    elif count == 2:
        prob = 26 + round(weighted * 11)
    elif count == 3:
        prob = 42 + round(weighted * 11)
    elif count == 4:
        prob = 55 + round(weighted * 9)
    else:
        prob = 62 + round(weighted * 7)

    if min_gap is not None:
        if min_gap <= 2:
            prob += 8
        elif min_gap <= 4:
            prob += 5
        elif min_gap <= 6:
            prob += 3

    if count10 >= 2:
        prob = max(prob, 58)
    if count10 >= 3:
        prob = max(prob, 74)
    if count10 >= 4:
        prob = max(prob, 84)

    if avg_conf < 0.55:
        prob -= 4
    elif avg_conf < 0.75:
        prob -= 1
    elif avg_conf > 1.0:
        prob += 3

    if count < 2 and count10 < 2:
        prob = min(prob, 28)

    if minutes_ahead > 0:
        decay = max(0.90, 1 - (minutes_ahead / 360) * 0.06)
        prob = round(prob * decay)

    return max(8, min(88, prob))

def window_probability(df, start_ts, window_minutes=45, minutes_ahead=0):
    sub = df[
        (df["effective_arrival_utc"] >= start_ts) &
        (df["effective_arrival_utc"] < start_ts + window_minutes * 60)
    ].copy()

    best = best_cluster_from_sorted(sub, 15)
    if not best or best["count"] == 0:
        return {
            "start_ts": start_ts,
            "label_start_ts": start_ts,
            "label_end_ts": start_ts + 12*60,
            "probability": 8,
            "flights": 0,
            "gap": None,
            "avg_conf": 0.0,
            "strong_count": 0,
            "count10": 0
        }

    prob = probability_from_cluster(
        best["count"],
        best["weighted"],
        best["min_gap"],
        best["avg_conf"],
        best["count10"],
        minutes_ahead
    )

    strong_count = int((sub["confidence"] >= 0.75).sum()) if not sub.empty else 0

    return {
        "start_ts": start_ts,
        "label_start_ts": best["start_ts"],
        "label_end_ts": best["end_ts"],
        "probability": prob,
        "flights": best["count"],
        "gap": best["min_gap"],
        "avg_conf": best["avg_conf"],
        "strong_count": strong_count,
        "count10": best["count10"]
    }

def best_window_in_horizon(df, from_ts, horizon_minutes):
    starts = [from_ts + i * 60 for i in range(0, horizon_minutes, 5)]
    best = None

    for s in starts:
        result = window_probability(df, s, 45, s - from_ts)
        if best is None or result["probability"] > best["probability"]:
            best = result

    return best or {
        "start_ts": from_ts, "label_start_ts": from_ts, "label_end_ts": from_ts + 12*60,
        "probability": 8, "flights": 0, "gap": None, "avg_conf": 0.0, "strong_count": 0, "count10": 0
    }

def select_non_overlapping_top_windows(windows, max_items=5, min_separation_minutes=15):
    selected = []
    for w in sorted(windows, key=lambda x: x["probability"], reverse=True):
        if all(abs(w["label_start_ts"] - s["label_start_ts"]) >= min_separation_minutes * 60 for s in selected):
            selected.append(w)
        if len(selected) >= max_items:
            break
    return sorted(selected, key=lambda x: x["label_start_ts"])

def smooth_probs(values):
    if len(values) < 3:
        return values
    out = []
    for i in range(len(values)):
        left = values[i-1] if i > 0 else values[i]
        mid = values[i]
        right = values[i+1] if i < len(values)-1 else values[i]
        out.append(round(left * 0.10 + mid * 0.80 + right * 0.10))
    return out

def compute_dashboard():
    df = load_flights()
    now_israel = datetime.now(ISRAEL)
    now_ts = int(now_israel.timestamp())

    current_res = best_window_in_horizon(df, now_ts, 45)
    current_probability = current_res["probability"]

    current = {
        "probability": current_probability,
        "color": color_from_prob(current_probability),
        "label": "נמוך" if current_probability < 30 else ("בינוני" if current_probability < 60 else "גבוה"),
        "flights": current_res["flights"],
        "window": "45 הדקות הקרובות",
        "quality": quality_from_avg_conf(current_res["avg_conf"]),
        "strong_count": current_res["strong_count"]
    }

    upcoming_raw = []
    for i in range(0, 6 * 60, 5):
        start_ts = now_ts + i * 60
        result = window_probability(df, start_ts, 45, i)

        start_dt = datetime.fromtimestamp(result["label_start_ts"], ISRAEL)
        end_dt = datetime.fromtimestamp(result["label_end_ts"], ISRAEL)

        upcoming_raw.append({
            "start_ts": start_ts,
            "label_start_ts": result["label_start_ts"],
            "label_end_ts": result["label_end_ts"],
            "label": f"{start_dt.strftime('%H:%M')}–{end_dt.strftime('%H:%M')}",
            "probability": result["probability"],
            "flights": result["flights"],
            "color": color_from_prob(result["probability"]),
            "best10": result["count10"],
            "best15": "-",
            "best30": "-",
            "gap10": result["gap"],
            "strong_count": result["strong_count"]
        })

    top_windows = select_non_overlapping_top_windows(upcoming_raw, max_items=5, min_separation_minutes=15)

    strong_windows = [w for w in top_windows if w["probability"] >= 50]
    medium_windows = [w for w in top_windows if w["probability"] >= 35]

    if strong_windows:
        top_windows = strong_windows
    elif medium_windows:
        top_windows = medium_windows[:3]
    else:
        top_windows = []

    next_attention_window = None
    candidates = [w for w in upcoming_raw if w["probability"] >= 30]
    if candidates:
        next_attention_window = sorted(candidates, key=lambda x: x["label_start_ts"])[0]

    daily_raw = []
    for h in range(24):
        hour_start_dt = now_israel.replace(minute=0, second=0, microsecond=0) + timedelta(hours=h)
        hour_start_ts = int(hour_start_dt.timestamp())

        hour_sub = df[
            (df["effective_arrival_utc"] >= hour_start_ts) &
            (df["effective_arrival_utc"] < hour_start_ts + 3600)
        ].copy()

        best = best_cluster_from_sorted(hour_sub, 15)
        if not best:
            p = 8
            flights = 0
        else:
            p = probability_from_cluster(
                best["count"], best["weighted"], best["min_gap"],
                best["avg_conf"], best["count10"], h * 60
            )
            flights = best["count"]

        daily_raw.append({
            "hour": hour_start_dt.strftime("%H:00"),
            "probability": p,
            "flights": flights
        })

    smoothed = smooth_probs([x["probability"] for x in daily_raw])

    daily = []
    for i, row in enumerate(daily_raw):
        p = smoothed[i]
        daily.append({
            "hour": row["hour"],
            "probability": p,
            "flights": row["flights"],
            "color": color_from_prob(p)
        })

    return {
        "updated": now_israel.strftime("%Y-%m-%d %H:%M:%S"),
        "current": current,
        "top_windows": top_windows,
        "next_attention_window": next_attention_window,
        "daily": daily
    }

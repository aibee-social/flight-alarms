import sqlite3
import pandas as pd
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
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
    conn = sqlite3.connect(DB)

    df = pd.read_sql("""
        SELECT
            fc.flight_number,
            fc.callsign,
            fc.registration,
            fc.airline,
            fc.origin_airport,
            fc.origin_city,
            fc.scheduled_arrival_utc,
            fc.scheduled_arrival_israel,
            fc.estimated_arrival_utc,
            fc.estimated_arrival_israel,
            fc.real_arrival_utc,
            fc.real_arrival_israel,
            fc.eta_utc,
            fc.eta_israel,
            fc.status,
            fc.last_collected_utc,
            COALESCE(u.cnt_updates, 0) AS cnt_updates,
            COALESCE(s.cnt_snapshots, 0) AS cnt_snapshots
        FROM flights_current fc
        LEFT JOIN (
            SELECT flight_number, COUNT(*) AS cnt_updates
            FROM flight_updates
            GROUP BY flight_number
        ) u
        ON fc.flight_number = u.flight_number
        LEFT JOIN (
            SELECT flight_number, COUNT(*) AS cnt_snapshots
            FROM flight_snapshots
            GROUP BY flight_number
        ) s
        ON fc.flight_number = s.flight_number
    """, conn)

    conn.close()

    if df.empty:
        df["effective_arrival_utc"] = pd.Series(dtype="float64")
        df["effective_arrival_israel"] = pd.Series(dtype="object")
        df["confidence"] = pd.Series(dtype="float64")
        return df

    israel_tz = "Asia/Jerusalem"

    # אם אין UTC, נבנה אותו מהזמן בישראל
    for local_col, utc_col in [
        ("scheduled_arrival_israel", "scheduled_arrival_utc"),
        ("estimated_arrival_israel", "estimated_arrival_utc"),
        ("eta_israel", "eta_utc"),
    ]:
        if local_col in df.columns and utc_col in df.columns:
            mask = df[utc_col].isna() & df[local_col].notna()
            if mask.any():
                local_dt = pd.to_datetime(df.loc[mask, local_col], errors="coerce")
                local_dt = local_dt.dt.tz_localize(
                    israel_tz,
                    nonexistent="shift_forward",
                    ambiguous="NaT"
                )
                df.loc[mask, utc_col] = (
                    local_dt.dt.tz_convert("UTC").astype("int64") // 10**9
                )

    df["effective_arrival_utc"] = (
        df["eta_utc"]
        .fillna(df["estimated_arrival_utc"])
        .fillna(df["scheduled_arrival_utc"])
    )

    df["effective_arrival_israel"] = (
        df["eta_israel"]
        .fillna(df["estimated_arrival_israel"])
        .fillna(df["scheduled_arrival_israel"])
    )

    df = df[df["effective_arrival_utc"].notna()].copy()

    now_ts = int(datetime.now(ISRAEL).timestamp())
    df["minutes_ahead"] = (df["effective_arrival_utc"] - now_ts) / 60.0

    df = df[
        (df["effective_arrival_utc"] >= now_ts - 10 * 60) &
        (df["effective_arrival_utc"] <= now_ts + 24 * 3600)
    ].copy()

    df = df[~df["status"].fillna("").str.lower().isin(["unknown"])].copy()

    bucket5 = ((df["effective_arrival_utc"] // 300) * 300).astype("Int64").astype(str)
    fallback_key = df["origin_city"].fillna("UNK") + "_" + bucket5 + "_" + df["status"].fillna("UNK")

    df["dedupe_key"] = (
        df["registration"]
        .replace("", pd.NA)
        .fillna(df["callsign"].replace("", pd.NA))
        .fillna(fallback_key)
        .fillna(df["flight_number"])
    )

    df = df.sort_values("last_collected_utc", ascending=False).drop_duplicates(subset=["dedupe_key"])

    def _extract_confidence(value):
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

    confidence_values = []
    for _, row in df.iterrows():
        try:
            confidence_values.append(float(_extract_confidence(flight_confidence(row))))
        except Exception:
            confidence_values.append(0.6)
    df["confidence"] = confidence_values

    type_info = df.apply(lambda r: airline_weight(r.get("airline"), r.get("flight_number")), axis=1)
    df["flight_type"] = [x[1] for x in type_info]

    strong_live = (
        (df["status"].isin(["estimated", "delayed"])) |
        (df["eta_utc"].notna()) |
        (df["estimated_arrival_utc"].notna()) |
        (df["real_arrival_utc"].notna()) |
        (df["callsign"].notna() & (df["callsign"] != "")) |
        (df["registration"].notna() & (df["registration"] != "")) |
        (df["cnt_updates"] > 0)
    )

    local_scheduled = (
        (df["status"] == "scheduled") &
        (df["airline"].fillna("").str.upper().isin(PASSENGER_AIRLINES)) &
        (df["minutes_ahead"] <= 180)
    )

    df = df[strong_live | local_scheduled].copy()
    df = df[~((df["flight_type"] == "cargo") & ~(strong_live))].copy()
    df = df[~((df["flight_type"] == "charter") & ~(strong_live))].copy()
    df = df[df["confidence"] >= 0.22].copy()

    return df.sort_values("effective_arrival_utc").reset_index(drop=True)












def _color_from_probability_pct(prob_pct: int) -> str:
    if prob_pct >= 70:
        return "red"
    if prob_pct >= 35:
        return "orange"
    return "green"


def _label_from_probability_pct(prob_pct: int) -> str:
    if prob_pct >= 70:
        return "גבוה"
    if prob_pct >= 35:
        return "בינוני"
    return "נמוך"


def _neighbor_bonus(current_window, clusters_df) -> int:
    if clusters_df.empty:
        return 0

    prev_w = current_window - pd.Timedelta(minutes=10)
    next_w = current_window + pd.Timedelta(minutes=10)

    bonus = 0

    prev_row = clusters_df[clusters_df["window"] == prev_w]
    next_row = clusters_df[clusters_df["window"] == next_w]

    if not prev_row.empty:
        prev_traffic = int(prev_row.iloc[0]["traffic"])
        if prev_traffic >= 2:
            bonus += 3
        if prev_traffic >= 3:
            bonus += 2

    if not next_row.empty:
        next_traffic = int(next_row.iloc[0]["traffic"])
        if next_traffic >= 2:
            bonus += 3
        if next_traffic >= 3:
            bonus += 2

    return min(bonus, 8)

def _traffic_probability(traffic: int, updated_count: int, avg_weight: float, max_cluster: int) -> int:
    if traffic <= 0:
        return 12

    base = 12

    # כמות טיסות – החלק המרכזי
    count_term = min(42, max(0, traffic - 1) * 18)

    # בונוס לטיסות עם מידע מעודכן
    updated_term = min(12, updated_count * 4)

    # בונוס מתון לאיכות/משקל
    quality_term = int(round(max(0.0, avg_weight - 0.6) * 25))
    quality_term = min(10, quality_term)

    # בונוס קטן אם זה מהקלאסטרים הכי חזקים של היום
    relative_term = 0
    if max_cluster >= 3:
        relative_term = int(round(((traffic - 1) / max(1, max_cluster - 1)) * 12))
        relative_term = min(12, max(0, relative_term))

    prob = base + count_term + updated_term + quality_term + relative_term
    return int(max(12, min(88, prob)))



def compute_dashboard_combined():
    import pandas as pd
    from datetime import datetime
    from zoneinfo import ZoneInfo

    df = load_all_traffic().copy()
    if df.empty:
        return {
            "updated": "--",
            "updated_at": "--",
            "current": {
                "probability": 0,
                "color": "green",
                "label": "אין נתונים",
                "flights": 0,
                "arrivals": 0,
                "departures": 0,
                "updated_count": 0,
                "window": "45 הדקות הקרובות",
                "quality": "נמוכה",
                "strong_count": 0
            },
            "top_windows": [],
            "next_attention_window": None,
            "daily": []
        }

    df["dt"] = pd.to_datetime(df["dt"], errors="coerce")
    df = df.dropna(subset=["dt"]).copy()
    df["window"] = df["dt"].dt.floor("10min")

    grouped = (
        df.groupby("window")
          .agg(
              flights=("flight_number", "size"),
              arrivals=("type", lambda x: int((x == "arrival").sum())),
              departures=("type", lambda x: int((x == "departure").sum())),
              updated_count=("is_updated", "sum"),
              avg_weight=("weight", "mean")
          )
          .reset_index()
          .sort_values("window")
    )

    if grouped.empty:
        return {
            "updated": "--",
            "updated_at": "--",
            "current": {
                "probability": 0,
                "color": "green",
                "label": "אין נתונים",
                "flights": 0,
                "arrivals": 0,
                "departures": 0,
                "updated_count": 0,
                "window": "45 הדקות הקרובות",
                "quality": "נמוכה",
                "strong_count": 0
            },
            "top_windows": [],
            "next_attention_window": None,
            "daily": []
        }

    max_cluster = int(grouped["flights"].max()) if not grouped.empty else 1

    # צפיפות סמוכה: כמה טיסות יש גם בחלון הקודם והבא (עד 20 דק)
    grouped["neighbor_flights"] = (
        grouped["flights"].shift(1).fillna(0) +
        grouped["flights"].shift(-1).fillna(0)
    )

    # בונוס אם יש יותר מאשכול אחד באותה שעה
    grouped["hour_bucket"] = grouped["window"].dt.floor("1h")
    hour_counts = grouped.groupby("hour_bucket").size().to_dict()
    grouped["clusters_in_hour"] = grouped["hour_bucket"].map(hour_counts).fillna(1)

    probs = []
    for _, row in grouped.iterrows():
        base = _traffic_probability(
            int(row["flights"]),
            int(row["updated_count"]),
            float(row["avg_weight"]),
            max_cluster
        )

        bonus = 0
        bonus += min(12, int(row["neighbor_flights"]) * 3)
        bonus += min(10, max(0, int(row["clusters_in_hour"]) - 1) * 4)

        prob = min(85, max(8, base + bonus))
        probs.append(int(prob))

    grouped["probability"] = probs

    def _color_from_probability_pct(prob):
        if prob < 30:
            return "green"
        if prob < 60:
            return "orange"
        return "red"

    def _label_from_probability_pct(prob):
        if prob < 30:
            return "נמוך"
        if prob < 60:
            return "בינוני"
        return "גבוה"

    now = datetime.now(ZoneInfo("Asia/Jerusalem")).replace(tzinfo=None)
    now_ts = pd.Timestamp(now)

    current_sub = grouped[
        (grouped["window"] >= now_ts.floor("10min")) &
        (grouped["window"] < now_ts + pd.Timedelta(minutes=45))
    ].copy()

    if current_sub.empty:
        current_prob = 0
        current_flights = 0
        current_arrivals = 0
        current_departures = 0
        current_updated = 0
        current_window = "45 הדקות הקרובות"
        current_color = "green"
        current_label = "נמוך"
        quality = "נמוכה"
        strong_count = 0
    else:
        first_current = current_sub.iloc[0]
        current_prob = int(first_current["probability"])
        current_flights = int(first_current["flights"])
        current_arrivals = int(first_current["arrivals"])
        current_departures = int(first_current["departures"])
        current_updated = int(first_current["updated_count"])
        start = pd.Timestamp(first_current["window"])
        end = start + pd.Timedelta(minutes=10)
        current_window = f"{start.strftime('%H:%M')}–{end.strftime('%H:%M')}"
        current_color = _color_from_probability_pct(current_prob)
        current_label = _label_from_probability_pct(current_prob)
        strong_count = current_updated
        quality = "גבוהה" if current_updated >= 1 else "בינונית"

    # הצגה: קודם חלונות קרובים בזמן, אבל נציג רק רלוונטיים
    future_windows = grouped[grouped["window"] >= now_ts.floor("10min")].copy()
    future_windows = future_windows[future_windows["probability"] >= 35].copy()
    future_windows = future_windows.sort_values(["window", "probability"], ascending=[True, False]).head(5)

    top_windows = []
    for _, row in future_windows.iterrows():
        start = pd.Timestamp(row["window"])
        end = start + pd.Timedelta(minutes=10)
        top_windows.append({
            "start_ts": int(start.timestamp()),
            "label_start_ts": int(start.timestamp()),
            "label_end_ts": int(end.timestamp()),
            "label": f"{start.strftime('%H:%M')}–{end.strftime('%H:%M')}",
            "probability": int(row["probability"]),
            "flights": int(row["flights"]),
            "arrivals": int(row["arrivals"]),
            "departures": int(row["departures"]),
            "color": _color_from_probability_pct(int(row["probability"])),
            "best10": int(row["flights"]),
            "best15": "-",
            "best30": "-",
            "gap10": 0.0,
            "strong_count": int(row["updated_count"])
        })

    next_attention_window = top_windows[0] if top_windows else None

    grouped["hour"] = grouped["window"].dt.floor("1h")
    hourly = (
        grouped.groupby("hour")
          .agg(
              probability=("probability", "max"),
              flights=("flights", "sum")
          )
          .reset_index()
          .sort_values("hour")
    )

    daily = []
    for _, row in hourly.iterrows():
        daily.append({
            "hour": pd.Timestamp(row["hour"]).strftime("%H:%M"),
            "probability": int(row["probability"]),
            "flights": int(row["flights"]),
            "color": _color_from_probability_pct(int(row["probability"]))
        })

    updated = now.strftime("%Y-%m-%d %H:%M:%S")

    return {
        "updated": updated,
        "updated_at": updated,
        "current": {
            "probability": current_prob,
            "color": current_color,
            "label": current_label,
            "flights": current_flights,
            "arrivals": current_arrivals,
            "departures": current_departures,
            "updated_count": current_updated,
            "window": current_window,
            "quality": quality,
            "strong_count": strong_count
        },
        "top_windows": top_windows,
        "next_attention_window": next_attention_window,
        "daily": daily
    }

def compute_traffic_clusters():
    import numpy as np

    df = load_all_traffic()

    if df.empty:
        return df

    df["window"] = df["dt"].dt.floor("10min")

    clusters = (
        df.groupby("window")
          .size()
          .reset_index(name="traffic")
    )

    mean = clusters["traffic"].mean()
    std = clusters["traffic"].std()

    if std == 0:
        clusters["probability"] = 0.5
        return clusters

    z = (clusters["traffic"] - mean) / std

    clusters["probability"] = 1 / (1 + np.exp(-z))

    return clusters.sort_values("window")



def load_all_traffic():
    arr = load_flights().copy()
    dep = load_departures().copy()

    if not arr.empty:
        arr["type"] = "arrival"
        arr["dt"] = pd.to_datetime(arr["effective_arrival_israel"], errors="coerce")
        arr["is_updated"] = (
            arr["eta_israel"].notna() |
            arr["estimated_arrival_israel"].notna()
        ).astype(int)
        arr["weight"] = arr.get("confidence", 0.6).fillna(0.6).clip(0.35, 1.0)
        arr = arr[["flight_number", "dt", "type", "is_updated", "weight"]]

    if not dep.empty:
        dep["type"] = "departure"
        dep["dt"] = pd.to_datetime(dep["effective_departure_israel"], errors="coerce")
        dep["is_updated"] = (
            dep["estimated_departure_israel"].notna() &
            (dep["estimated_departure_israel"] != dep["scheduled_departure_israel"])
        ).astype(int)
        dep["weight"] = dep["is_updated"].map({1: 0.9, 0: 0.65}).astype(float)
        dep = dep[["flight_number", "dt", "type", "is_updated", "weight"]]

    df = pd.concat([arr, dep], ignore_index=True) if (not arr.empty or not dep.empty) else pd.DataFrame(
        columns=["flight_number", "dt", "type", "is_updated", "weight"]
    )

    if df.empty:
        return df

    df = df.dropna(subset=["dt"]).sort_values("dt").reset_index(drop=True)
    return df

def load_departures():
    conn = sqlite3.connect(DB)

    df = pd.read_sql("""
        SELECT
            flight_number,
            airline,
            destination_city,
            terminal,
            scheduled_date,
            scheduled_departure_israel,
            estimated_departure_israel,
            gate_info,
            scraped_at
        FROM departures_current
    """, conn)

    conn.close()

    if df.empty:
        df["effective_departure_israel"] = pd.Series(dtype="object")
        return df

    df["effective_departure_israel"] = (
        df["estimated_departure_israel"]
        .fillna(df["scheduled_departure_israel"])
    )

    df["effective_dt"] = pd.to_datetime(df["effective_departure_israel"], errors="coerce")
    df = df.dropna(subset=["effective_dt"]).copy()

    return df.sort_values("effective_dt").reset_index(drop=True)


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

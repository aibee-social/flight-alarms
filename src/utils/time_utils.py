from datetime import datetime
import pytz

UTC = pytz.utc
ISRAEL = pytz.timezone("Asia/Jerusalem")

def utc_now():
    return int(datetime.utcnow().timestamp())

def israel_now():
    return datetime.now(ISRAEL).strftime("%Y-%m-%d %H:%M:%S")

def utc_to_israel(ts):
    if ts is None or ts == "None":
        return None
    dt = datetime.utcfromtimestamp(int(ts))
    dt = UTC.localize(dt)
    return dt.astimezone(ISRAEL).strftime("%Y-%m-%d %H:%M:%S")

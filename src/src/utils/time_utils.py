from datetime import datetime
import pytz

utc = pytz.utc
israel = pytz.timezone("Asia/Jerusalem")


def utc_now():

    return int(datetime.utcnow().timestamp())


def israel_now():

    return datetime.now(israel).strftime("%Y-%m-%d %H:%M:%S")


def utc_to_israel(ts):

    if ts is None:
        return None

    dt = datetime.utcfromtimestamp(ts)
    dt = utc.localize(dt)

    return dt.astimezone(israel).strftime("%Y-%m-%d %H:%M:%S")


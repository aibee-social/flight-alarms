import sqlite3
from pyflightdata import FlightData

from src.utils.time_utils import utc_now, israel_now, utc_to_israel

DB_PATH = "data/flights.db"

fd = FlightData()


def run():

    arrivals = fd.get_airport_arrivals("TLV")

    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()

    now_utc = utc_now()
    now_israel = israel_now()

    for f in arrivals:

        try:

            flight = f["flight"]

            flight_number = flight["identification"]["number"]["default"]

            airline = flight["airline"]["name"]

            origin_airport = flight["airport"]["origin"]["code"]["iata"]

            origin_city = flight["airport"]["origin"]["position"]["region"]["city"]

            status = flight["status"]["generic"]["status"]["text"]

            if status == "canceled":
                continue

            scheduled = flight["time"]["scheduled"]["arrival"]

            real = flight["time"]["real"]["arrival"]

            scheduled_israel = utc_to_israel(scheduled)
            real_israel = utc_to_israel(real)

            existing = c.execute(
                "SELECT status, scheduled_arrival_utc FROM flights_active WHERE flight_number=?",
                (flight_number,)
            ).fetchone()

            if existing:

                old_status, old_time = existing

                if old_status != status or old_time != scheduled:

                    c.execute("""
                    INSERT INTO flights_updates (
                        flight_number,
                        old_status,
                        new_status,
                        old_time,
                        new_time,
                        updated_at_utc,
                        updated_at_israel
                    )
                    VALUES (?,?,?,?,?,?,?)
                    """, (
                        flight_number,
                        old_status,
                        status,
                        old_time,
                        scheduled,
                        now_utc,
                        now_israel
                    ))

                    c.execute("""
                    UPDATE flights_active
                    SET status=?,
                        scheduled_arrival_utc=?,
                        scheduled_arrival_israel=?,
                        real_arrival_utc=?,
                        real_arrival_israel=?,
                        last_updated_utc=?,
                        last_updated_israel=?
                    WHERE flight_number=?
                    """, (
                        status,
                        scheduled,
                        scheduled_israel,
                        real,
                        real_israel,
                        now_utc,
                        now_israel,
                        flight_number
                    ))

            else:

                c.execute("""
                INSERT INTO flights_active (
                    flight_number,
                    airline,
                    origin_airport,
                    origin_city,

                    scheduled_arrival_utc,
                    scheduled_arrival_israel,

                    real_arrival_utc,
                    real_arrival_israel,

                    status,

                    first_seen_utc,
                    last_updated_utc,

                    first_seen_israel,
                    last_updated_israel
                )
                VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)
                """, (
                    flight_number,
                    airline,
                    origin_airport,
                    origin_city,

                    scheduled,
                    scheduled_israel,

                    real,
                    real_israel,

                    status,

                    now_utc,
                    now_utc,

                    now_israel,
                    now_israel
                ))

            if status == "landed":

                c.execute("""
                INSERT INTO flights_landed (
                    flight_number,
                    airline,
                    origin_airport,
                    origin_city,

                    scheduled_arrival_utc,
                    scheduled_arrival_israel,

                    real_arrival_utc,
                    real_arrival_israel,

                    collected_at_utc,
                    collected_at_israel
                )
                VALUES (?,?,?,?,?,?,?,?,?,?)
                """, (
                    flight_number,
                    airline,
                    origin_airport,
                    origin_city,
                    scheduled,
                    scheduled_israel,
                    real,
                    real_israel,
                    now_utc,
                    now_israel
                ))

                c.execute(
                    "DELETE FROM flights_active WHERE flight_number=?",
                    (flight_number,)
                )

        except:
            continue

    conn.commit()
    conn.close()


if __name__ == "__main__":
    run()

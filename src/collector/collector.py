import sqlite3
from pyflightdata import FlightData
from src.utils.time_utils import utc_now, israel_now, utc_to_israel

DB_PATH = "data/flights.db"
fd = FlightData()

VALID_STATUSES = {"scheduled", "estimated", "delayed", "landed"}

def norm(v):
    if v == "None" or v == "":
        return None
    return v

def run():
    arrivals = fd.get_airport_arrivals("TLV")
    print(f"\nFetched raw arrivals: {len(arrivals)}")

    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()

    now_utc = utc_now()
    now_israel = israel_now()

    inserted_snapshots = 0
    inserted_updates = 0
    inserted_landed = 0
    skipped_canceled = 0
    skipped_invalid = 0
    errors = 0

    for i, f in enumerate(arrivals):
        try:
            flight = f["flight"]

            flight_number = norm(flight["identification"]["number"]["default"])
            callsign = norm(flight["identification"].get("callsign"))
            airline = norm(flight["airline"]["name"])

            aircraft = flight.get("aircraft", {})
            registration = norm(aircraft.get("registration"))

            origin_airport = norm(flight["airport"]["origin"]["code"]["iata"])
            origin_city = norm(flight["airport"]["origin"]["position"]["region"]["city"])

            status = norm(flight["status"]["generic"]["status"]["text"])

            if status == "canceled":
                skipped_canceled += 1
                continue

            if status not in VALID_STATUSES or not flight_number:
                skipped_invalid += 1
                continue

            scheduled = norm(flight["time"]["scheduled"]["arrival"])
            estimated = norm(flight["time"]["estimated"]["arrival"])
            real = norm(flight["time"]["real"]["arrival"])
            eta = norm(flight["time"]["other"].get("eta") if flight["time"].get("other") else None)

            if scheduled is not None:
                scheduled = int(scheduled)
            if estimated is not None:
                estimated = int(estimated)
            if real is not None:
                real = int(real)
            if eta is not None:
                eta = int(eta)

            scheduled_israel = utc_to_israel(scheduled)
            estimated_israel = utc_to_israel(estimated)
            real_israel = utc_to_israel(real)
            eta_israel = utc_to_israel(eta)

            c.execute("""
            INSERT INTO flight_snapshots (
                flight_number, callsign, registration, airline, origin_airport, origin_city,
                scheduled_arrival_utc, scheduled_arrival_israel,
                estimated_arrival_utc, estimated_arrival_israel,
                real_arrival_utc, real_arrival_israel,
                eta_utc, eta_israel,
                status, collected_at_utc, collected_at_israel
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                flight_number, callsign, registration, airline, origin_airport, origin_city,
                scheduled, scheduled_israel,
                estimated, estimated_israel,
                real, real_israel,
                eta, eta_israel,
                status, now_utc, now_israel
            ))
            inserted_snapshots += 1

            existing = c.execute("""
            SELECT status, scheduled_arrival_utc, estimated_arrival_utc, real_arrival_utc, eta_utc
            FROM flights_current
            WHERE flight_number = ?
            """, (flight_number,)).fetchone()

            if existing:
                old_status, old_scheduled, old_estimated, old_real, old_eta = existing
                changed = (
                    old_status != status or
                    old_scheduled != scheduled or
                    old_estimated != estimated or
                    old_real != real or
                    old_eta != eta
                )

                if changed:
                    c.execute("""
                    INSERT INTO flight_updates (
                        flight_number,
                        old_status, new_status,
                        old_scheduled_utc, new_scheduled_utc,
                        old_estimated_utc, new_estimated_utc,
                        old_real_utc, new_real_utc,
                        updated_at_utc, updated_at_israel
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """, (
                        flight_number,
                        old_status, status,
                        old_scheduled, scheduled,
                        old_estimated, estimated,
                        old_real, real,
                        now_utc, now_israel
                    ))
                    inserted_updates += 1

            c.execute("""
            INSERT INTO flights_current (
                flight_number, callsign, registration, airline, origin_airport, origin_city,
                scheduled_arrival_utc, scheduled_arrival_israel,
                estimated_arrival_utc, estimated_arrival_israel,
                real_arrival_utc, real_arrival_israel,
                eta_utc, eta_israel,
                status,
                first_seen_utc, first_seen_israel,
                last_collected_utc, last_collected_israel,
                last_changed_utc, last_changed_israel
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(flight_number) DO UPDATE SET
                callsign = excluded.callsign,
                registration = excluded.registration,
                airline = excluded.airline,
                origin_airport = excluded.origin_airport,
                origin_city = excluded.origin_city,
                scheduled_arrival_utc = excluded.scheduled_arrival_utc,
                scheduled_arrival_israel = excluded.scheduled_arrival_israel,
                estimated_arrival_utc = excluded.estimated_arrival_utc,
                estimated_arrival_israel = excluded.estimated_arrival_israel,
                real_arrival_utc = excluded.real_arrival_utc,
                real_arrival_israel = excluded.real_arrival_israel,
                eta_utc = excluded.eta_utc,
                eta_israel = excluded.eta_israel,
                status = excluded.status,
                last_collected_utc = excluded.last_collected_utc,
                last_collected_israel = excluded.last_collected_israel,
                last_changed_utc = CASE
                    WHEN flights_current.status != excluded.status
                      OR COALESCE(flights_current.scheduled_arrival_utc, -1) != COALESCE(excluded.scheduled_arrival_utc, -1)
                      OR COALESCE(flights_current.estimated_arrival_utc, -1) != COALESCE(excluded.estimated_arrival_utc, -1)
                      OR COALESCE(flights_current.real_arrival_utc, -1) != COALESCE(excluded.real_arrival_utc, -1)
                      OR COALESCE(flights_current.eta_utc, -1) != COALESCE(excluded.eta_utc, -1)
                    THEN excluded.last_changed_utc
                    ELSE flights_current.last_changed_utc
                END,
                last_changed_israel = CASE
                    WHEN flights_current.status != excluded.status
                      OR COALESCE(flights_current.scheduled_arrival_utc, -1) != COALESCE(excluded.scheduled_arrival_utc, -1)
                      OR COALESCE(flights_current.estimated_arrival_utc, -1) != COALESCE(excluded.estimated_arrival_utc, -1)
                      OR COALESCE(flights_current.real_arrival_utc, -1) != COALESCE(excluded.real_arrival_utc, -1)
                      OR COALESCE(flights_current.eta_utc, -1) != COALESCE(excluded.eta_utc, -1)
                    THEN excluded.last_changed_israel
                    ELSE flights_current.last_changed_israel
                END
            """, (
                flight_number, callsign, registration, airline, origin_airport, origin_city,
                scheduled, scheduled_israel,
                estimated, estimated_israel,
                real, real_israel,
                eta, eta_israel,
                status,
                now_utc, now_israel,
                now_utc, now_israel,
                now_utc, now_israel
            ))

            if status == "landed" or real is not None:
                c.execute("""
                INSERT INTO flights_landed (
                    flight_number, callsign, registration, airline, origin_airport, origin_city,
                    scheduled_arrival_utc, scheduled_arrival_israel,
                    estimated_arrival_utc, estimated_arrival_israel,
                    real_arrival_utc, real_arrival_israel,
                    eta_utc, eta_israel,
                    landed_detected_at_utc, landed_detected_at_israel
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(flight_number) DO UPDATE SET
                    callsign = excluded.callsign,
                    registration = excluded.registration,
                    airline = excluded.airline,
                    origin_airport = excluded.origin_airport,
                    origin_city = excluded.origin_city,
                    scheduled_arrival_utc = excluded.scheduled_arrival_utc,
                    scheduled_arrival_israel = excluded.scheduled_arrival_israel,
                    estimated_arrival_utc = excluded.estimated_arrival_utc,
                    estimated_arrival_israel = excluded.estimated_arrival_israel,
                    real_arrival_utc = excluded.real_arrival_utc,
                    real_arrival_israel = excluded.real_arrival_israel,
                    eta_utc = excluded.eta_utc,
                    eta_israel = excluded.eta_israel,
                    landed_detected_at_utc = excluded.landed_detected_at_utc,
                    landed_detected_at_israel = excluded.landed_detected_at_israel
                """, (
                    flight_number, callsign, registration, airline, origin_airport, origin_city,
                    scheduled, scheduled_israel,
                    estimated, estimated_israel,
                    real, real_israel,
                    eta, eta_israel,
                    now_utc, now_israel
                ))
                inserted_landed += 1
                c.execute("DELETE FROM flights_current WHERE flight_number = ?", (flight_number,))

        except Exception as e:
            errors += 1
            print(f"Error on flight index {i}: {e}")

    stale_threshold = now_utc - 15 * 60
    c.execute("""
    DELETE FROM flights_current
    WHERE status != 'landed'
      AND COALESCE(eta_utc, estimated_arrival_utc, scheduled_arrival_utc) IS NOT NULL
      AND COALESCE(eta_utc, estimated_arrival_utc, scheduled_arrival_utc) < ?
    """, (stale_threshold,))

    conn.commit()
    conn.close()

    print("\nCollector summary:")
    print("inserted_snapshots =", inserted_snapshots)
    print("inserted_updates =", inserted_updates)
    print("inserted_landed =", inserted_landed)
    print("skipped_canceled =", skipped_canceled)
    print("skipped_invalid =", skipped_invalid)
    print("errors =", errors)

if __name__ == "__main__":
    run()

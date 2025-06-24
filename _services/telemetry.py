from asyncio.log import logger
from itertools import batched
import fastf1
from sqlalchemy import MetaData, Table, select
from sqlalchemy.dialects.postgresql import insert
import pandas as pd

from _repository.engine import postgres
from _services.session_type_selector import get_session_type


def store_practice_telemetry(season: int, identifier: int, round_number: int):
    telemetries = []
    laps_table = Table("laps", MetaData(), autoload_with=postgres)

    with postgres.connect() as pg_con:
        session = fastf1.get_session(
            year=season, gp=round_number, identifier=identifier
        )
        # uncomment if previously loaded
        session.load(laps=True, telemetry=True, weather=False, messages=False)
        drivers = session.results[["BroadcastName", "DriverNumber"]].values
        for driver in drivers:
            driver_laps = session.laps.pick_drivers(driver[1])
            lap_numbers = driver_laps["LapNumber"].values
            for lap_number in lap_numbers:
                lap = driver_laps.pick_laps(lap_number)
                try:
                    telemetry = (
                        lap.get_car_data()[
                            [
                                "Speed",
                                "RPM",
                                "nGear",
                                "Throttle",
                                "Brake",
                                "Time",
                            ]
                        ]
                        .add_distance()
                        .rename(
                            {
                                "Speed": "speed",
                                "RPM": "rpm",
                                "Throttle": "throttle",
                                "Distance": "distance",
                                "Brake": "brake",
                                "nGear": "gear",
                                "Time": "laptime_at",
                            }
                        )
                    )
                    lap_id = (
                        pg_con.execute(
                            select(laps_table).where(
                                laps_table.c.lap_number == int(lap_number),
                                laps_table.c.driver_id == driver[0],
                                laps_table.c.season_year == season,
                                laps_table.c.session_type_id
                                == f"Practice {identifier}",
                                laps_table.c.event_name == session.event.EventName,
                            )
                        )
                        .fetchone()
                        ._tuple()[0]
                    )
                    telemetry[["lap_id"]] = lap_id
                    telemetry.Time = telemetry.Time.transform(
                        lambda x: x.total_seconds() if pd.notna(x) else None
                    )
                    telemetries.extend(telemetry.to_dict(orient="records"))
                except:
                    logger.warning(
                        "Unable to load session: %s %s %s",
                        identifier,
                        driver,
                        lap_number,
                    )
                    continue

        telemetry_table = Table(
            "telemetry_measurements", MetaData(), autoload_with=postgres
        )
        for batch in batched(telemetries, 10000):
            pg_con.execute(insert(table=telemetry_table).values(batch))
        pg_con.commit()


def store_race_telemetry(season: int, round_number: int, identifier: int):
    telemetries = []
    laps_table = Table("laps", MetaData(), autoload_with=postgres)
    with postgres.connect() as pg_con:
        session = fastf1.get_session(
            year=season, gp=round_number, identifier=identifier
        )
        # uncomment if previously loaded
        session.load(laps=True, telemetry=True, weather=False, messages=False)
        drivers = session.results[["BroadcastName", "DriverNumber"]].values
        for driver in drivers:
            driver_laps = session.laps.pick_drivers(driver[1])
            lap_numbers = driver_laps["LapNumber"].values
            for lap_number in lap_numbers:
                lap = driver_laps.pick_laps(lap_number)
                try:
                    telemetry = (
                        lap.get_car_data()[
                            [
                                "Speed",
                                "RPM",
                                "nGear",
                                "Throttle",
                                "Brake",
                                "Time",
                            ]
                        ]
                        .add_distance()
                        .rename(
                            columns={
                                "Speed": "speed",
                                "RPM": "rpm",
                                "Throttle": "throttle",
                                "Distance": "distance",
                                "Brake": "brake",
                                "nGear": "gear",
                                "Time": "laptime_at",
                            }
                        )
                    )
                    lap_id = (
                        pg_con.execute(
                            select(laps_table).where(
                                laps_table.c.lap_number == int(lap_number),
                                laps_table.c.driver_id == driver[0],
                                laps_table.c.season_year == season,
                                laps_table.c.session_type_id
                                == ("Race" if identifier == 5 else "Sprint"),
                                laps_table.c.event_name == session.event.EventName,
                            )
                        )
                        .fetchone()
                        ._tuple()[0]
                    )
                    telemetry[["lap_id"]] = lap_id
                    telemetry.laptime_at = telemetry.laptime_at.transform(
                        lambda x: x.total_seconds() if pd.notna(x) else None
                    )
                    telemetry.brake = telemetry.brake.transform(lambda x: int(x))
                    telemetries.extend(telemetry.to_dict(orient="records"))
                except:
                    logger.warning(
                        "Unable to load session: %s %s %s %s %s",
                        int(lap_number),
                        driver[0],
                        season,
                        "Race" if identifier == 5 else "Sprint",
                        session.event.EventName,
                    )
                    continue

        telemetry_table = Table(
            "telemetry_measurements", MetaData(), autoload_with=postgres
        )
        for batch in batched(telemetries, 10000):
            pg_con.execute(insert(table=telemetry_table).values(batch))
        pg_con.commit()


def store_quali_telemetry(season: int, round_number: int, identifier: int):
    telemetries = []
    laps_table = Table("laps", MetaData(), autoload_with=postgres)

    with postgres.connect() as pg_con:
        session = fastf1.get_session(
            year=season, gp=round_number, identifier=identifier
        )
        # uncomment if previously loaded
        session.load(laps=True, telemetry=True, weather=False, messages=True)
        drivers = session.results[["BroadcastName", "DriverNumber"]].values
        for driver in drivers:
            driver_laps = session.laps.pick_drivers(driver[1])
            lap_numbers = driver_laps["LapNumber"].values
            for lap_number in lap_numbers:
                lap = driver_laps.pick_laps(lap_number)
                try:
                    telemetry = (
                        lap.get_car_data()[
                            [
                                "Speed",
                                "RPM",
                                "nGear",
                                "Throttle",
                                "Brake",
                                "Time",
                            ]
                        ]
                        .add_distance()
                        .rename(
                            columns={
                                "Speed": "speed",
                                "RPM": "rpm",
                                "Throttle": "throttle",
                                "Distance": "distance",
                                "Brake": "brake",
                                "nGear": "gear",
                                "Time": "laptime_at",
                            }
                        )
                    )
                    lap_id = (
                        pg_con.execute(
                            select(laps_table).where(
                                laps_table.c.lap_number == int(lap_number),
                                laps_table.c.driver_id == driver[0],
                                laps_table.c.season_year == season,
                                laps_table.c.session_type_id
                                == (
                                    "Qualifying"
                                    if identifier == 4
                                    else "Sprint Qualifying"
                                ),
                                laps_table.c.event_name == session.event.EventName,
                            )
                        )
                        .fetchone()
                        ._tuple()[0]
                    )
                    telemetry[["lap_id"]] = lap_id
                    telemetry.laptime_at = telemetry.laptime_at.transform(
                        lambda x: x.total_seconds() if pd.notna(x) else None
                    )
                    telemetry.brake = telemetry.brake.transform(lambda x: int(x))
                    telemetries.extend(telemetry.to_dict(orient="records"))
                except:
                    logger.warning(
                        "Unable to load session: %s %s %s %s %s",
                        int(lap_number),
                        driver[0],
                        season,
                        f"Practice {identifier}",
                        session.event.EventName,
                    )
                    continue

        telemetry_table = Table(
            "telemetry_measurements", MetaData(), autoload_with=postgres
        )
        for batch in batched(telemetries, 10000):
            pg_con.execute(insert(table=telemetry_table).values(batch))
        pg_con.commit()


def store_telemetry(season: int, round_number: int, identifier: int):
    session_type = get_session_type(season, round_number, identifier)
    if session_type == "Practice":
        store_practice_telemetry(season, identifier, round_number)
    elif session_type == "Qualilike":
        store_quali_telemetry(season, round_number, identifier)
    elif session_type == "Racelike":
        store_race_telemetry(season, round_number, identifier)

    raise ValueError("Session type not supported")

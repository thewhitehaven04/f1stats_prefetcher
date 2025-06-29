from asyncio.log import logger
from itertools import batched
import fastf1
import pandas as pd

from _repository.engine import postgres
from _repository.repository import Laps, TelemetryMeasurements
from _services.session_type_selector import get_session_type
from sqlalchemy.orm import Session


def store_practice_telemetry(season: int, identifier: int, round_number: int):
    telemetries = []

    with Session(postgres) as s:
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
                        s.query(Laps)
                        .filter(
                            Laps.lap_number == int(lap_number),
                            Laps.driver_id == driver[0],
                            Laps.season_year == season,
                            Laps.session_type_id == f"Practice {identifier}",
                            Laps.event_name == session.event.EventName,
                        )
                        .one()
                        .id
                    )
                    telemetry[["lap_id"]] = lap_id
                    telemetry.brake = telemetry.brake.transform(lambda x: int(x))
                    telemetry.laptime_at = telemetry.laptime_at.transform(
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

        with Session(postgres) as s:
            for batch in batched(telemetries, 1000):
                s.add_all(map(lambda x: TelemetryMeasurements(**x), batch))
                s.commit()


def store_race_telemetry(season: int, round_number: int, identifier: int):
    telemetries = []
    with Session(postgres) as s:
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
                        s.query(Laps)
                        .filter(
                            Laps.lap_number == int(lap_number),
                            Laps.driver_id == driver[0],
                            Laps.season_year == season,
                            Laps.session_type_id
                            == ("Race" if identifier == 5 else "Sprint"),
                            Laps.event_name == session.event.EventName,
                        )
                        .one()
                        .id
                    )
                    telemetry[["lap_id"]] = lap_id
                    telemetry.brake = telemetry.brake.transform(lambda x: int(x))
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

        with Session(postgres) as s:
            for batch in batched(telemetries, 1000):
                s.add_all(map(lambda x: TelemetryMeasurements(**x), batch))
                s.commit()


def store_quali_telemetry(season: int, round_number: int, identifier: int):
    telemetries = []
    with Session(postgres) as s:
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
                        s.query(Laps)
                        .filter(
                            Laps.lap_number == int(lap_number),
                            Laps.driver_id == driver[0],
                            Laps.season_year == season,
                            Laps.session_type_id
                            == (
                                "Qualifying" if identifier == 4 else "Sprint Qualifying"
                            ),
                            Laps.event_name == session.event.EventName,
                        )
                        .one()
                        .id
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
                        identifier,
                        session.event.EventName,
                    )
                    continue

        with Session(postgres) as s:
            for batch in batched(telemetries, 1000):
                s.add_all(map(lambda x: TelemetryMeasurements(**x), batch))
                s.commit()


def store_telemetry(season: int, round_number: int, identifier: int):
    session_type = get_session_type(season, round_number, identifier)
    if session_type == "Practice":
        store_practice_telemetry(season, identifier, round_number)
    elif session_type == "Qualilike":
        store_quali_telemetry(season, round_number, identifier)
    elif session_type == "Racelike":
        store_race_telemetry(season, round_number, identifier)
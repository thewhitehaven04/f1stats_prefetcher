from sqlalchemy import MetaData, Table
from sqlalchemy.dialects.postgresql import insert
import fastf1
import pandas as pd
from _repository.engine import postgres 
from _services.session_type_selector import get_session_type


def store_practice_laps(season: int, round_number: int, identifier: int):
    practice_laps = []
    session = fastf1.get_session(year=season, gp=round_number, identifier=identifier)
    # uncomment if previously loaded
    session.load(laps=True, telemetry=False, weather=False, messages=False)
    laps = session.laps[
        [
            "Sector1Time",
            "Sector2Time",
            "Sector3Time",
            "SpeedI1",
            "SpeedI2",
            "SpeedFL",
            "Stint",
            "Compound",
            "LapNumber",
            "DriverNumber",
        ]
    ].rename(
        columns={
            "Sector1Time": "sector_1_time",
            "Sector2Time": "sector_2_time",
            "Sector3Time": "sector_3_time",
            "SpeedI1": "speedtrap_1",
            "SpeedI2": "speedtrap_2",
            "SpeedFL": "speedtrap_fl",
            "Stint": "stint",
            "Compound": "compound_id",
            "LapNumber": "lap_number",
        }
    )
    results = session.results[["DriverNumber", "BroadcastName"]]
    laps = (
        laps.join(results.set_index("DriverNumber"), on="DriverNumber")
        .rename(columns={"BroadcastName": "driver_id"})
        .drop(columns=["DriverNumber"])
    )
    laps[["event_name"]] = session.event.EventName
    laps[["session_type_id"]] = f"Practice {identifier}"
    laps[["season_year"]] = season
    practice_laps.extend(laps.to_dict(orient="records"))

    def transform(val):
        val["sector_1_time"] = (
            val["sector_1_time"]
            if isinstance(val["sector_1_time"], float)
            else (
                val["sector_1_time"].total_seconds()
                if pd.notna(val["sector_1_time"])
                else None
            )
        )
        val["sector_2_time"] = (
            val["sector_2_time"]
            if isinstance(val["sector_2_time"], float)
            else (
                val["sector_2_time"].total_seconds()
                if pd.notna(val["sector_2_time"])
                else None
            )
        )
        val["sector_3_time"] = (
            val["sector_3_time"]
            if isinstance(val["sector_3_time"], float)
            else (
                val["sector_3_time"].total_seconds()
                if pd.notna(val["sector_3_time"])
                else None
            )
        )
        val["speedtrap_1"] = (
            int(val["speedtrap_1"]) if pd.notna(val["speedtrap_1"]) else None
        )
        val["speedtrap_2"] = (
            int(val["speedtrap_2"]) if pd.notna(val["speedtrap_2"]) else None
        )
        val["speedtrap_fl"] = (
            int(val["speedtrap_fl"]) if pd.notna(val["speedtrap_fl"]) else None
        )
        val["pit_in_time"] = (
            val["pit_in_time"]
            if isinstance(val["pit_in_time"], float)
            else (
                val["pit_in_time"].total_seconds()
                if pd.notna(val["pit_in_time"])
                else None
            )
        )
        val["pit_out_time"] = (
            val["pit_out_time"]
            if isinstance(val["pit_out_time"], float)
            else (
                val["pit_out_time"].total_seconds()
                if pd.notna(val["pit_out_time"])
                else None
            )
        )
        return val

    laps_transformed = list(map(transform, practice_laps))
    with postgres.connect() as pg_con:
        laps_table = Table("laps", MetaData(), autoload_with=postgres)
        pg_con.execute(insert(table=laps_table).values(laps_transformed))
        pg_con.commit()


def store_quali_laps(season: int, round_number: int, identifier: int):
    quali_laps = []
    session = fastf1.get_session(year=season, gp=round_number, identifier=identifier)
    # uncomment if previously loaded
    session.load(laps=True, telemetry=False, weather=False, messages=False)
    laps = session.laps[
        [
            "Sector1Time",
            "Sector2Time",
            "Sector3Time",
            "SpeedI1",
            "SpeedI2",
            "SpeedFL",
            "Stint",
            "Compound",
            "LapNumber",
            "DriverNumber",
            "PitInTime",
            "PitOutTime",
        ]
    ].rename(
        columns={
            "Sector1Time": "sector_1_time",
            "Sector2Time": "sector_2_time",
            "Sector3Time": "sector_3_time",
            "SpeedI1": "speedtrap_1",
            "SpeedI2": "speedtrap_2",
            "SpeedFL": "speedtrap_fl",
            "Stint": "stint",
            "Compound": "compound_id",
            "LapNumber": "lap_number",
            "PitInTime": "pit_in_time",
            "PitOutTime": "pit_out_time",
        }
    )
    results = session.results[["DriverNumber", "BroadcastName"]]
    laps = (
        laps.join(results.set_index("DriverNumber"), on="DriverNumber")
        .rename(columns={"BroadcastName": "driver_id"})
        .drop(columns=["DriverNumber"])
    )
    laps[["event_name"]] = session.event.EventName
    laps[["session_type_id"]] = "Qualifying" if identifier == 4 else "Sprint Qualifying"
    laps[["season_year"]] = season
    quali_laps.extend(laps.to_dict(orient="records"))

    def transform(val):
        val["sector_1_time"] = (
            val["sector_1_time"]
            if isinstance(val["sector_1_time"], float)
            else (
                val["sector_1_time"].total_seconds()
                if pd.notna(val["sector_1_time"])
                else None
            )
        )
        val["sector_2_time"] = (
            val["sector_2_time"]
            if isinstance(val["sector_2_time"], float)
            else (
                val["sector_2_time"].total_seconds()
                if pd.notna(val["sector_2_time"])
                else None
            )
        )
        val["sector_3_time"] = (
            val["sector_3_time"]
            if isinstance(val["sector_3_time"], float)
            else (
                val["sector_3_time"].total_seconds()
                if pd.notna(val["sector_3_time"])
                else None
            )
        )
        val["speedtrap_1"] = (
            int(val["speedtrap_1"]) if pd.notna(val["speedtrap_1"]) else None
        )
        val["speedtrap_2"] = (
            int(val["speedtrap_2"]) if pd.notna(val["speedtrap_2"]) else None
        )
        val["speedtrap_fl"] = (
            int(val["speedtrap_fl"]) if pd.notna(val["speedtrap_fl"]) else None
        )
        val["pit_in_time"] = (
            val["pit_in_time"]
            if isinstance(val["pit_in_time"], float)
            else (
                val["pit_in_time"].total_seconds()
                if pd.notna(val["pit_in_time"])
                else None
            )
        )
        val["pit_out_time"] = (
            val["pit_out_time"]
            if isinstance(val["pit_out_time"], float)
            else (
                val["pit_out_time"].total_seconds()
                if pd.notna(val["pit_out_time"])
                else None
            )
        )
        return val

    laps_transformed = list(map(transform, quali_laps))
    with postgres.connect() as pg_con:
        laps_table = Table("laps", MetaData(), autoload_with=postgres)
        pg_con.execute(insert(table=laps_table).values(laps_transformed))
        pg_con.commit()


def store_race_laps(season: int, round_number: int, identifier: int):
    quali_laps = []
    session = fastf1.get_session(year=season, gp=round_number, identifier=identifier)
    # uncomment if previously loaded
    session.load(laps=True, telemetry=False, weather=False, messages=False)
    laps = session.laps[
        [
            "Sector1Time",
            "Sector2Time",
            "Sector3Time",
            "SpeedI1",
            "SpeedI2",
            "SpeedFL",
            "Stint",
            "Compound",
            "LapNumber",
            "DriverNumber",
            "PitInTime",
            "PitOutTime",
        ]
    ].rename(
        columns={
            "Sector1Time": "sector_1_time",
            "Sector2Time": "sector_2_time",
            "Sector3Time": "sector_3_time",
            "SpeedI1": "speedtrap_1",
            "SpeedI2": "speedtrap_2",
            "SpeedFL": "speedtrap_fl",
            "Stint": "stint",
            "Compound": "compound_id",
            "LapNumber": "lap_number",
            "PitInTime": "pit_in_time",
            "PitOutTime": "pit_out_time",
        }
    )
    results = session.results[["DriverNumber", "BroadcastName"]]
    laps = (
        laps.join(results.set_index("DriverNumber"), on="DriverNumber")
        .rename(columns={"BroadcastName": "driver_id"})
        .drop(columns=["DriverNumber"])
    )
    laps[["event_name"]] = session.event.EventName
    laps[["session_type_id"]] = "Race" if identifier == 5 else "Sprint"
    laps[["season_year"]] = season
    quali_laps.extend(laps.to_dict(orient="records"))

    def transform(val):
        val["sector_1_time"] = (
            val["sector_1_time"]
            if isinstance(val["sector_1_time"], float)
            else (
                val["sector_1_time"].total_seconds()
                if pd.notna(val["sector_1_time"])
                else None
            )
        )
        val["sector_2_time"] = (
            val["sector_2_time"]
            if isinstance(val["sector_2_time"], float)
            else (
                val["sector_2_time"].total_seconds()
                if pd.notna(val["sector_2_time"])
                else None
            )
        )
        val["sector_3_time"] = (
            val["sector_3_time"]
            if isinstance(val["sector_3_time"], float)
            else (
                val["sector_3_time"].total_seconds()
                if pd.notna(val["sector_3_time"])
                else None
            )
        )
        val["speedtrap_1"] = (
            int(val["speedtrap_1"]) if pd.notna(val["speedtrap_1"]) else None
        )
        val["speedtrap_2"] = (
            int(val["speedtrap_2"]) if pd.notna(val["speedtrap_2"]) else None
        )
        val["speedtrap_fl"] = (
            int(val["speedtrap_fl"]) if pd.notna(val["speedtrap_fl"]) else None
        )
        val["pit_in_time"] = (
            val["pit_in_time"]
            if isinstance(val["pit_in_time"], float)
            else (
                val["pit_in_time"].total_seconds()
                if pd.notna(val["pit_in_time"])
                else None
            )
        )
        val["pit_out_time"] = (
            val["pit_out_time"]
            if isinstance(val["pit_out_time"], float)
            else (
                val["pit_out_time"].total_seconds()
                if pd.notna(val["pit_out_time"])
                else None
            )
        )
        return val

    laps_transformed = list(map(transform, quali_laps))
    with postgres.connect() as pg_con:
        laps_table = Table("laps", MetaData(), autoload_with=postgres)
        pg_con.execute(insert(table=laps_table).values(laps_transformed))
        pg_con.commit()


def store_laps(season: int, round_number: int, identifier: int):
    session_type = get_session_type(season, round_number, identifier)
    if session_type == "Practice":
        store_practice_laps(season, round_number, identifier)
    elif session_type == "Qualilike":
        store_quali_laps(season, round_number, identifier)
    else:
        store_race_laps(season, round_number, identifier)

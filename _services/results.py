import pandas as pd
import fastf1
from sqlalchemy.orm import Session
from _repository.repository import (
    PracticeSessionResults,
    QualifyingSessionResults,
    RaceSessionResults,
)

from _repository.engine import postgres
from _services.session_type_selector import get_session_type


def store_sprint_race_results(season: int, round_number: int):
    results_arr = []

    session = fastf1.get_session(year=season, gp=round_number, identifier=3)
    session.load(laps=True, telemetry=False, weather=False, messages=False)

    results = session.results.rename(
        columns={"FullName": "Driver", "Time": "Gap"}
    ).assign(
        Time=pd.Series(
            index=session.results.index,
            data=[
                session.results["Time"].iloc[0],
                *(
                    session.results["Time"]
                    .iloc[1:]
                    .add(session.results["Time"].iloc[0])
                ),
            ],
        )
    )
    for result in results.itertuples():
        time = result.Time.total_seconds()
        gap = result.Gap.total_seconds()
        results_arr.append(
            {
                "event_name": session.event.EventName,
                "season_year": season,
                "session_type_id": f"Sprint",
                "driver_id": result.BroadcastName,
                "total_time": time if pd.notna(time) else None,
                "result_status": result.Status,
                "classified_position": result.Position,
                "points": result.Points,
                "gap": gap if pd.notna(gap) else None,
                "grid_position": result.GridPosition,
            }
        )

    with Session(postgres) as s:
        s.add_all(map(lambda x: RaceSessionResults(**x), results_arr))
        s.commit()


def store_race_results(season: int, round_number: int):
    results_arr = []

    session = fastf1.get_session(year=season, gp=round_number, identifier=5)
    # uncomment if previously loaded
    session.load(laps=True, telemetry=False, weather=False, messages=False)
    results = session.results.rename(
        columns={"FullName": "Driver", "Time": "Gap"}
    ).assign(
        Time=pd.Series(
            index=session.results.index,
            data=[
                session.results["Time"].iloc[0],
                *(
                    session.results["Time"]
                    .iloc[1:]
                    .add(session.results["Time"].iloc[0])
                ),
            ],
        )
    )

    for result in results.itertuples():
        time = result.Time.total_seconds()
        gap = result.Gap.total_seconds()
        results_arr.append(
            {
                "event_name": session.event.EventName,
                "season_year": season,
                "session_type_id": "Race",
                "driver_id": result.BroadcastName,
                "total_time": time if pd.notna(time) else None,
                "result_status": result.Status,
                "classified_position": result.ClassifiedPosition,
                "points": result.Points,
                "gap": gap if pd.notna(gap) else None,
                "grid_position": result.GridPosition,
            }
        )

    with Session(postgres) as s:
        s.add_all(map(lambda x: RaceSessionResults(**x), results_arr))
        s.commit()


def _store_qualifying_results(
    season: int, round_number: int, identifier: int, session_type_name: str
):
    session = fastf1.get_session(year=season, gp=round_number, identifier=identifier)
    session.load(laps=True, telemetry=False, weather=False, messages=True)
    results = session.results

    results_arr = []
    for result in results.itertuples():
        q1 = result.Q1.total_seconds() 
        q2 = result.Q2.total_seconds()
        q3 = result.Q3.total_seconds()
        results_arr.append(
            {
                "event_name": session.event.EventName,
                "season_year": season,
                "session_type_id": session_type_name,
                "driver_id": result.BroadcastName,
                "q1_laptime": q1 if pd.notna(q1) else None,
                "q2_laptime": q2 if pd.notna(q2) else None,
                "q3_laptime": q3 if pd.notna(q3) else None,
                "position": result.Position,
            }
        )

    with Session(postgres) as s:
        s.add_all(map(lambda x: QualifyingSessionResults(**x), results_arr))
        s.commit()


def store_sprint_quali_results(season: int, round_number: int):
    _store_qualifying_results(season, round_number, 2, "Sprint Qualifying")


def store_quali_results(season: int, round_number: int):
    _store_qualifying_results(season, round_number, 4, "Qualifying")


def store_practice_results(season: int, round_number: int, identifier: int):
    results_arr = []
    session = fastf1.get_session(year=season, gp=round_number, identifier=identifier)
    # uncomment if previously loaded
    session.load(laps=True, telemetry=False, weather=False, messages=False)
    laps = session.laps
    results = (
        session.results.assign(
            Time_=laps.groupby("DriverNumber").agg({"LapTime": "min"})
        )
        .sort_values(by=["Time_"], ascending=True)
        .assign(Gap=lambda x: x["Time_"].sub(x["Time_"].iloc[0]))
        .dropna(subset=["BroadcastName"])
    )
    for result in results.itertuples():
        results_arr.append(
            {
                "event_name": session.event.EventName,
                "season_year": season,
                "session_type_id": f"Practice {identifier}",
                "driver_id": result.BroadcastName,
                "laptime": result.Time_.total_seconds(),
                "gap": result.Gap.total_seconds(),
            }
        )

    with Session(postgres) as s:
        s.add_all(map(lambda x: PracticeSessionResults(**x), results_arr))
        s.commit()


def store_results(season: int, round_number: int, identifier: int):
    session_type = get_session_type(season, round_number, identifier)
    if session_type == "Practice":
        store_practice_results(season, round_number, identifier)

    elif session_type == "Qualilike":
        if identifier == 4:
            store_quali_results(season, round_number)
        else:
            store_sprint_quali_results(season, round_number)

    elif session_type == "Racelike":
        if identifier == 5:
            store_race_results(season, round_number)
        else:
            store_sprint_race_results(season, round_number)

    else:
        raise ValueError("Session type not supported")


# def store_testing_sessions(season: int):
#     sessions = []

#     for session in range(1, 3):
#         try:
#             session = fastf1.get_testing_session(
#                 year=season, test_number=1, session_number=session
#             )
#             session.load(laps=False, telemetry=False, weather=False, messages=False)
#             session_info = session.session_info
#             sessions.append(
#                 {
#                     "start_time": session_info["StartDate"],
#                     "end_time": session_info["EndDate"],
#                     "season_year": season,
#                     "event_name": session.event.EventName,
#                     "session_type_id": session.event[f"Session{session}"],
#                 }
#             )
#         finally:
#             continue

#     with open(f"/testing_sessions_{season}.json", "w") as file:
#         file.write(json.dumps(sessions))


# def insert_testing_sessions(season: int):
#     with open(f"/sessions_{season}.json", "r") as file:
#         sessions = json.loads(file.read())
#         with con as pg_con:
#             events_sessions_table = Table(
#                 "event_sessions", MetaData(), autoload_with=postgres
#             )
#             pg_con.execute(insert(table=events_sessions_table).values(sessions))
#             pg_con.commit()

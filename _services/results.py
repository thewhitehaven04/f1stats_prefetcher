import pandas as pd
import fastf1
from sqlalchemy.orm import Session
from _repository.repository import (
    PracticeSessionResults,
    QualifyingSessionResults,
    RaceSessionResults,
    SessionResults,
)

from _repository.engine import postgres
from _services.session_type_selector import get_session_type


def store_sprint_race_results(season: int, round_number: int):
    results_arr = []
    sprint_results = []

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
    for result in results.iterrows():
        results_arr.append(
            {
                "event_name": session.event.EventName,
                "season_year": season,
                "session_type_id": f"Sprint",
                "driver_id": result[1][1],
            }
        )
        time = result[1][21].total_seconds()
        gap = result[1][18].total_seconds()
        sprint_results.append(
            {
                "total_time": time if pd.notna(time) else None,
                "result_status": result[1][19],
                "classified_position": result[1][13],
                "points": result[1][20],
                "gap": gap if pd.notna(gap) else None,
            }
        )

    with Session(postgres) as s:
        for result, sprint_result in zip(results_arr, sprint_results):
            orm_result = SessionResults(**result)
            s.add(orm_result)
            s.flush()
            s.add(
                RaceSessionResults(
                    **result,
                    **sprint_result,
                    season_year=orm_result.season_year,
                    event_name=orm_result.event_name,
                    session_type_id=orm_result.session_type_id,
                    driver_id=orm_result.driver_id,
                )
            )
        s.commit()


def store_race_results(season: int, round_number: int):
    results_arr = []
    race_results = []

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

    for result in results.iterrows():
        results_arr.append(
            {
                "event_name": session.event.EventName,
                "season_year": season,
                "session_type_id": "Race",
                "driver_id": result[1][1],
            }
        )
        time = result[1][21].total_seconds()
        gap = result[1][18].total_seconds()
        race_results.append(
            {
                "total_time": time if pd.notna(time) else None,
                "result_status": result[1][19],
                "classified_position": result[1][13],
                "points": result[1][20],
                "gap": gap if pd.notna(gap) else None,
            }
        )

    with Session(postgres) as s:
        for result, race_result in zip(results_arr, race_results):
            orm_result = SessionResults(**result)
            s.add(orm_result)
            s.flush()
            s.add(
                RaceSessionResults(
                    **race_result,
                    id=orm_result.id,
                    season_year=orm_result.season_year,
                    event_name=orm_result.event_name,
                    session_type_id=orm_result.session_type_id,
                    driver_id=orm_result.driver_id,
                )
            )
        s.commit()


def store_sprint_quali_results(season: int, round_number: int):
    results_arr = []
    sprint_quali_results = []
    session = fastf1.get_session(year=season, gp=round_number, identifier=2)
    session.load(laps=True, telemetry=False, weather=False, messages=True)
    results = session.results

    for result in results.iterrows():
        results_arr.append(
            {
                "event_name": session.event.EventName,
                "season_year": season,
                "session_type_id": f"Sprint Qualifying",
                "driver_id": result[1][1],
            }
        )
        sprint_quali_results.append(
            {
                "q1_laptime": result[1][15].total_seconds(),
                "q2_laptime": result[1][16].total_seconds(),
                "q3_laptime": result[1][17].total_seconds(),
                "position": result[1][12],
            }
        )

    with Session(postgres) as s:
        for result, quali_result in zip(results_arr, sprint_quali_results):
            orm_result = SessionResults(**result)
            s.add(orm_result)
            s.flush()
            s.add(
                QualifyingSessionResults(
                    **quali_result,
                    id=orm_result.id,
                    season_year=orm_result.season_year,
                    event_name=orm_result.event_name,
                    session_type_id=orm_result.session_type_id,
                    driver_id=orm_result.driver_id,
                )
            )
        s.commit()


def store_quali_results(season: int, round_number: int):
    results_arr = []
    quali_results = []

    session = fastf1.get_session(year=season, gp=round_number, identifier=4)
    # uncomment if previously loaded
    session.load(laps=True, telemetry=False, weather=False, messages=False)
    results = session.results
    for result in results.iterrows():
        results_arr.append(
            {
                "event_name": session.event.EventName,
                "season_year": season,
                "session_type_id": f"Qualifying",
                "driver_id": result[1][1],
            }
        )
        quali_results.append(
            {
                "q1_laptime": result[1][15].total_seconds(),
                "q2_laptime": result[1][16].total_seconds(),
                "q3_laptime": result[1][17].total_seconds(),
                "position": result[1][12],
            }
        )

    with Session(postgres) as s:
        for result, quali_result in zip(results_arr, quali_results):
            orm_result = SessionResults(**result)
            s.add(orm_result)
            s.flush()
            s.add(
                QualifyingSessionResults(
                    **quali_result,
                    id=orm_result.id,
                    season_year=orm_result.season_year,
                    event_name=orm_result.event_name,
                    session_type_id=orm_result.session_type_id,
                    driver_id=orm_result.driver_id,
                )
            )
        s.commit()


def store_practice_results(season: int, round_number: int, identifier: int):
    results_arr = []
    practice_results = []
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
    for result in results.iterrows():
        results_arr.append(
            {
                "event_name": session.event.EventName,
                "season_year": season,
                "session_type_id": f"Practice {identifier}",
                "driver_id": result[1][1],
            }
        )
        practice_results.append(
            {
                "laptime": result[1][21].total_seconds(),
                "gap": result[1][22].total_seconds(),
            }
        )

    with Session(postgres) as s:
        for result, practice_result in zip(results_arr, practice_results):
            s.add(
                PracticeSessionResults(
                    **result,
                    **practice_result,
                )
            )
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

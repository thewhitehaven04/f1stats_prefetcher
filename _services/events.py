import pandas as pd
import fastf1
from sqlalchemy import MetaData, Table
from sqlalchemy.dialects.postgresql import insert

from _repository.engine import postgres


def store_events(year: int):
    schedules = []
    schedule = fastf1.get_event_schedule(year=year)[
        ["EventName", "OfficialEventName", "EventDate", "Country", "EventFormat"]
    ].rename(
        columns={
            "EventName": "event_name",
            "OfficialEventName": "event_official_name",
            "EventDate": "date_start",
            "Country": "country",
            "EventFormat": "event_format_name",
        }
    )
    schedule[["season_year"]] = year
    schedules.append(schedule)

    schedule = pd.concat(schedules)

    with postgres.connect() as pg_con:
        events_table = Table("events", MetaData(), autoload_with=postgres)
        pg_con.execute(
            insert(table=events_table).values(schedule.to_dict(orient="records"))
        )
        pg_con.commit()


def store_event_sessions(year: int):
    sessions = []
    schedule = fastf1.get_event_schedule(year=year, include_testing=False)

    for i in range(len(schedule.index)):
        for identifier in range(1, 6):
            try:
                session = fastf1.get_session(year=year, gp=i, identifier=identifier)
                session.load(laps=False, telemetry=False, weather=False, messages=False)
                session_info = session.session_info
                sessions.append(
                    {
                        "start_time": session_info["StartDate"],
                        "end_time": session_info["EndDate"],
                        "season_year": year,
                        "event_name": session.event.EventName,
                        "session_type_id": session.event[f"Session{identifier}"],
                    }
                )
            finally:
                continue

    with postgres.connect() as pg_con:
        events_sessions_table = Table(
            "event_sessions", MetaData(), autoload_with=postgres
        )
        pg_con.execute(insert(table=events_sessions_table).values(sessions))
        pg_con.commit()

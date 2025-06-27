import pandas as pd
import fastf1
from pycountry import countries
from sqlalchemy.orm import Session

from _repository.engine import postgres
from _repository.repository import EventSessions, Events
from _services.circuits import get_season_data


def store_events(year: int):
    schedule = fastf1.get_event_schedule(year=year, include_testing=False)[
        [
            "EventName",
            "OfficialEventName",
            "EventDate",
            "Country",
            "Location",
            "EventFormat",
        ]
    ].rename(
        columns={
            "EventName": "event_name",
            "OfficialEventName": "event_official_name",
            "EventDate": "date_start",
            "Location": "location",
            "Country": "country",
            "EventFormat": "event_format_name",
        }
    )
    schedule["country"] = schedule["country"].map(
        lambda x: countries.search_fuzzy(x)[0].alpha_3
    )
    schedule[["season_year"]] = year

    season_data = pd.DataFrame(get_season_data(str(year)), columns=["id", "location"])
    schedule = schedule.join(
        season_data.set_index("location").rename(columns={"id": "circuit_id"}),
        on="location",
        how="right",
        validate="m:1",
    ).drop(labels=["location"], axis=1)

    with Session(postgres) as s:
        s.add_all(map(lambda x: Events(**x), schedule.to_dict(orient="records")))
        s.commit()


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

    with Session(postgres) as s:
        s.add_all(map(lambda x: EventSessions(**x), sessions))
        s.commit()

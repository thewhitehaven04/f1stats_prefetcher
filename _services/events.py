import pandas as pd
import fastf1
from pycountry import countries
from sqlalchemy.orm import Session

from _repository.engine import postgres
from _repository.repository import EventSessions, Events, SessionWeatherMeasurements
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


def store_weather_data(year: int, event: int, identifier: int):
    session = fastf1.get_session(year=year, gp=event, identifier=identifier)
    session.load(laps=False, telemetry=False, weather=True, messages=False)
    weather = session.weather_data
    if weather is not None:
        weather_data = weather[
            ["AirTemp", "TrackTemp", "Pressure", "Humidity", "Time"]
        ].rename(
            columns={
                "AirTemp": "air_temp",
                "TrackTemp": "track_temp",
                "Pressure": "air_pressure",
                "Humidity": "humidity",
                "Time": "time_at",
            }
        )
        weather_data.time_at = weather_data.time_at.transform(
            lambda x: x.total_seconds() * 1000
        )
        weather_data[["event_name"]] = session.event.EventName
        weather_data[["season_year"]] = year
        weather_data[["session_type_id"]] = session.name
        weather_data = weather_data.to_dict(orient="records")
        first_measurement = weather_data[0] 
        last_measurement = weather_data[-1] 
    else:
        raise ValueError(f"Weather data not loaded for {year}, {event}, {identifier}")

    with Session(postgres) as s:
        s.add(SessionWeatherMeasurements(**first_measurement))
        s.add(SessionWeatherMeasurements(**last_measurement))
        s.commit()


def store_event_session(year: int, event: int, identifier: int):
    session = fastf1.get_session(year=year, gp=event, identifier=identifier)
    session.load(laps=False, telemetry=False, weather=True, messages=False)
    session_info = session.session_info

    with Session(postgres) as s:
        s.add(
            EventSessions(
                **{
                    "start_time": session_info["StartDate"],
                    "end_time": session_info["EndDate"],
                    "season_year": year,
                    "event_name": session.event.EventName,
                    "session_type_id": session.event[f"Session{identifier}"],
                }
            )
        )
        s.commit()


def store_event_sessions(year: int):
    schedule = fastf1.get_event_schedule(year=year, include_testing=False)

    for i in range(len(schedule.index)):
        for identifier in range(1, 6):
            store_weather_data(year, schedule["RoundNumber"].iloc[i], identifier)
            store_event_session(year, schedule["RoundNumber"].iloc[i], identifier)

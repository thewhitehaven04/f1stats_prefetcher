from _services.circuits import store_circuit_data
from _services.drivers import (
    store_driver_data,
    store_driver_numbers,
    store_team_changes,
)
from _services.enums import init_compounds, init_event_formats, init_session_types
from _services.events import store_event_session, store_events, store_weather_data
from _services.laps import store_laps
from _services.results import store_results
from _services.teams import store_team_colors, store_teams
from _services.telemetry import store_telemetry


def store_session_data_to_db(season: str, round_number: int, session_type: int, is_store_session: bool = False):
    if is_store_session:
        store_event_session(int(season), round_number, session_type)
        store_weather_data(int(season), round_number, session_type)
    store_results(int(season), round_number, session_type)
    store_laps(int(season), round_number, session_type)
    store_telemetry(int(season), round_number, session_type)


def init_enums():
    init_session_types()
    init_event_formats()
    init_compounds()


def init_year_data(season: str):
    init_enums()
    store_driver_data(int(season), 1)
    store_driver_numbers(int(season), 24)
    store_teams(int(season))
    store_team_colors(int(season))

    store_driver_data(int(season), 24)
    store_driver_data(int(season), 1)
    store_driver_numbers(int(season), 24)

    store_team_changes(int(season))
    store_circuit_data(season)
    store_events(int(season))

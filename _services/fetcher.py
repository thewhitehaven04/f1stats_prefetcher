from _services.drivers import (
    store_driver_data,
    store_driver_numbers,
    store_team_changes,
)
from _services.events import store_event_sessions, store_events
from _services.laps import store_laps
from _services.results import store_results
from _services.teams import store_team_colors, store_teams
from _services.telemetry import store_telemetry


def store_session_data_to_db(season: str, round_number: int, session_type: int):
    store_results(int(season), round_number, session_type)
    store_laps(int(season), round_number, session_type)
    store_telemetry(int(season), round_number, session_type)


def init_year_data(season: str):
    store_teams(int(season))
    store_team_colors(int(season))
    store_driver_data(int(season))
    store_driver_numbers(int(season))
    store_team_changes(int(season))

    store_events(int(season))
    store_event_sessions(int(season))

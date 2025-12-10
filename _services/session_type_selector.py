from typing import Literal

import fastf1


SessionType = Literal["Practice", "Qualilike", "Racelike"]


def get_session_type(
    season: int, round_number: int, session_identifier: int
) -> SessionType:
    if session_identifier == 1:
        return "Practice"
    schedule = fastf1.get_event_schedule(year=season, include_testing=False)
    format_ = schedule[schedule["RoundNumber"] == round_number]["EventFormat"].iloc[0]

    if format_ == "conventional":
        if session_identifier in [2, 3]:
            return "Practice"
        elif session_identifier == 4:
            return "Qualilike"
        return "Racelike"
    elif format_ == "sprint_qualifying":
        if session_identifier in [2, 4]:
            return "Qualilike"
        return "Racelike"
    elif format_ == "testing":
        return "Practice"

    raise ValueError(f"Unsupported session format {format_}")

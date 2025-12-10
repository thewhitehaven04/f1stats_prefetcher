from itertools import product
from typing import Tuple
import fastf1
from sqlalchemy import Row, select, update
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.orm import Session

from _repository.engine import postgres
from _repository.repository import DriverNumbers, DriverTeamChanges, Drivers, Teams


def store_session_driver_data(season: int, event: int, identifier: int):
    session = fastf1.get_session(year=season, gp=event, identifier=identifier)
    session.load(laps=False, telemetry=False, weather=False, messages=False)
    drivers = session.results[
        ["BroadcastName", "Abbreviation", "FirstName", "LastName", "CountryCode"]
    ].rename(
        columns={
            "BroadcastName": "id",
            "Abbreviation": "abbreviation",
            "FirstName": "first_name",
            "LastName": "last_name",
            "CountryCode": "country_alpha3",
        }
    )
    with Session(postgres) as s:
        s.execute(
            statement=insert(Drivers)
            .values(drivers.to_dict(orient="records"))
            .on_conflict_do_nothing()
        )
        s.commit()


def store_driver_data(season: int, event: int):
    for event in range(1, 25):
        store_session_driver_data(season, event, 1)


def store_team_changes(season: int):
    schedule = fastf1.get_event_schedule(year=season, include_testing=False)
    sessions = product(schedule["RoundNumber"].values, range(1, 6))

    prev_chg = None

    def store_first_outing_for_team(
        s: Session, team: Row[Tuple[Teams]], driver_id: str
    ):
        s.execute(
            insert(DriverTeamChanges)
            .values(
                {
                    "driver_id": driver_id,
                    "timestamp_start": date,
                    "timestamp_end": None,
                    "team_id": team.tuple()[0].id,
                }
            )
            .on_conflict_do_nothing()
        )
        s.commit()

    def store_last_outing_for_team(
        s: Session,
        driver_id: str,
        new_team: Row[Tuple[Teams]],
        old_team_change: Row[Tuple[DriverTeamChanges]],
        previous_date,
    ):
        s.execute(
            update(DriverTeamChanges)
            .values({"timestamp_end": previous_date})
            .where(
                DriverTeamChanges.driver_id == driver_id,
                DriverTeamChanges.team_id == old_team_change.tuple()[0].team_id,
            )
        )
        s.execute(
            insert(DriverTeamChanges)
            .values(
                {
                    "driver_id": driver_id,
                    "timestamp_start": date,
                    "timestamp_end": None,
                    "team_id": new_team.tuple()[0].id,
                }
            )
            .on_conflict_do_nothing()
        )
        s.commit()

    with Session(postgres) as s:
        previous_date = None
        for curr in sessions:
            session = fastf1.get_session(year=season, identifier=curr[1], gp=curr[0])
            session.load(laps=False, telemetry=False, weather=False, messages=False)
            results = session.results
            for result in results.itertuples():
                driver = result.BroadcastName
                team_name = result.TeamName
                date = session.date

                prev_chg = s.execute(
                    select(DriverTeamChanges)
                    .where(
                        DriverTeamChanges.driver_id == driver,
                        DriverTeamChanges.timestamp_end == None,
                    )
                    .order_by(DriverTeamChanges.timestamp_start.desc())
                    .fetch(1)
                ).fetchone()

                team_record = s.execute(
                    select(Teams).where(Teams.team_display_name == team_name).fetch(1)
                ).fetchone()

                if team_record:
                    if not prev_chg:
                        store_first_outing_for_team(s, team_record, driver)
                        continue

                    elif prev_chg.tuple()[0].team_id == team_record.tuple()[0].id:
                        continue

                    if previous_date:
                        store_last_outing_for_team(
                            s, driver, team_record, prev_chg, previous_date
                        )

                    previous_date = date


def store_driver_numbers(season: int, event: int):
    session = fastf1.get_session(year=season, gp=event, identifier=1)
    session.load(laps=False, weather=False, messages=False, telemetry=False)
    numbers = session.results[["DriverNumber", "BroadcastName"]].rename(
        columns={"DriverNumber": "driver_number", "BroadcastName": "driver_id"}
    )
    with Session(postgres) as s:
        s.execute(
            insert(DriverNumbers)
            .values(
                [
                    {**x, "season_year": season}
                    for x in numbers.to_dict(orient="records")
                ]
            )
            .on_conflict_do_nothing()
        )
        s.commit()

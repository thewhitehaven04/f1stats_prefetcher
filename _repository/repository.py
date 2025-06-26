from typing import List, Optional

from sqlalchemy import (
    BigInteger,
    Boolean,
    CHAR,
    Column,
    Computed,
    Date,
    DateTime,
    ForeignKeyConstraint,
    Index,
    Integer,
    PrimaryKeyConstraint,
    REAL,
    SmallInteger,
    String,
    Table,
    Text,
    text,
)
from sqlalchemy.dialects.postgresql import JSONB, TIMESTAMP
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship, MappedAsDataclass
import datetime


class Base(DeclarativeBase, MappedAsDataclass):
    pass


class PrismaMigrations(Base):
    __tablename__ = "_prisma_migrations"
    __table_args__ = (PrimaryKeyConstraint("id", name="_prisma_migrations_pkey"),)

    id: Mapped[str] = mapped_column(String(36), primary_key=True)
    checksum: Mapped[str] = mapped_column(String(64))
    migration_name: Mapped[str] = mapped_column(String(255))
    started_at: Mapped[datetime.datetime] = mapped_column(
        DateTime(True), server_default=text("now()")
    )
    applied_steps_count: Mapped[int] = mapped_column(Integer, server_default=text("0"))
    finished_at: Mapped[Optional[datetime.datetime]] = mapped_column(DateTime(True))
    logs: Mapped[Optional[str]] = mapped_column(Text)
    rolled_back_at: Mapped[Optional[datetime.datetime]] = mapped_column(DateTime(True))


class Circuits(Base):
    __tablename__ = "circuits"
    __table_args__ = (PrimaryKeyConstraint("id", name="circuits_pkey"),)

    id: Mapped[str] = mapped_column(String(8), primary_key=True)
    name: Mapped[str] = mapped_column(Text)
    geojson: Mapped[dict] = mapped_column(JSONB)

    events: Mapped[List["Events"]] = relationship("Events", back_populates="circuit")


class Compounds(Base):
    __tablename__ = "compounds"
    __table_args__ = (PrimaryKeyConstraint("id", name="compounds_pkey"),)

    id: Mapped[str] = mapped_column(String(16), primary_key=True)

    laps: Mapped[List["Laps"]] = relationship("Laps", back_populates="compound")


t_driver_laps_with_analytics = Table(
    "driver_laps_with_analytics",
    Base.metadata,
    Column("driver_id", String(64)),
    Column("event_name", Text),
    Column("session_type_id", String(32)),
    Column("season_year", SmallInteger),
    Column("pb_s1", REAL),
    Column("pb_s2", REAL),
    Column("pb_s3", REAL),
    Column("pb_st1", SmallInteger),
    Column("pb_st2", SmallInteger),
    Column("pb_stfl", SmallInteger),
    Column("pb_laptime", REAL),
)


class Drivers(Base):
    __tablename__ = "drivers"
    __table_args__ = (PrimaryKeyConstraint("id", name="drivers_pkey"),)

    id: Mapped[str] = mapped_column(String(64), primary_key=True)
    country_alpha3: Mapped[str] = mapped_column(String(3))
    abbreviation: Mapped[str] = mapped_column(String(3))
    first_name: Mapped[str] = mapped_column(Text)
    last_name: Mapped[str] = mapped_column(Text)

    driver_numbers: Mapped[List["DriverNumbers"]] = relationship(
        "DriverNumbers", back_populates="driver"
    )
    driver_team_changes: Mapped[List["DriverTeamChanges"]] = relationship(
        "DriverTeamChanges", back_populates="driver"
    )
    laps: Mapped[List["Laps"]] = relationship("Laps", back_populates="driver")
    session_results: Mapped[List["SessionResults"]] = relationship(
        "SessionResults", back_populates="driver"
    )


class EventFormats(Base):
    __tablename__ = "event_formats"
    __table_args__ = (
        PrimaryKeyConstraint("event_format_name", name="event_formats_pkey"),
    )

    event_format_name: Mapped[str] = mapped_column(String(32), primary_key=True)

    events: Mapped[List["Events"]] = relationship(
        "Events", back_populates="event_formats"
    )


t_laps_with_analytics = Table(
    "laps_with_analytics",
    Base.metadata,
    Column("event_name", Text),
    Column("session_type_id", String(32)),
    Column("season_year", SmallInteger),
    Column("min_s1", REAL),
    Column("min_s2", REAL),
    Column("min_s3", REAL),
    Column("best_st1", SmallInteger),
    Column("best_st2", SmallInteger),
    Column("best_stfl", SmallInteger),
    Column("best_laptime", REAL),
)


class Seasons(Base):
    __tablename__ = "seasons"
    __table_args__ = (PrimaryKeyConstraint("season_year", name="seasons_pkey"),)

    season_year: Mapped[int] = mapped_column(SmallInteger, primary_key=True)
    description_text: Mapped[Optional[str]] = mapped_column(Text)

    driver_numbers: Mapped[List["DriverNumbers"]] = relationship(
        "DriverNumbers", back_populates="seasons"
    )
    events: Mapped[List["Events"]] = relationship("Events", back_populates="seasons")
    team_season_colors: Mapped[List["TeamSeasonColors"]] = relationship(
        "TeamSeasonColors", back_populates="seasons"
    )


class SessionTypes(Base):
    __tablename__ = "session_types"
    __table_args__ = (PrimaryKeyConstraint("id", name="session_types_pkey"),)

    id: Mapped[str] = mapped_column(String(32), primary_key=True)

    event_sessions: Mapped[List["EventSessions"]] = relationship(
        "EventSessions", back_populates="session_type"
    )
    laps: Mapped[List["Laps"]] = relationship("Laps", back_populates="session_type")


class Teams(Base):
    __tablename__ = "teams"
    __table_args__ = (
        PrimaryKeyConstraint("id", name="teams_pkey"),
        Index("teams_team_display_name_key", "team_display_name", unique=True),
    )

    id: Mapped[int] = mapped_column(SmallInteger, primary_key=True)
    team_display_name: Mapped[Optional[str]] = mapped_column(String(64))

    driver_team_changes: Mapped[List["DriverTeamChanges"]] = relationship(
        "DriverTeamChanges", back_populates="team"
    )
    team_season_colors: Mapped[List["TeamSeasonColors"]] = relationship(
        "TeamSeasonColors", back_populates="team"
    )


class DriverNumbers(Base):
    __tablename__ = "driver_numbers"
    __table_args__ = (
        ForeignKeyConstraint(
            ["driver_id"], ["drivers.id"], ondelete="RESTRICT", name="fk_driver_id"
        ),
        ForeignKeyConstraint(
            ["season_year"],
            ["seasons.season_year"],
            ondelete="RESTRICT",
            name="fk_season_year",
        ),
        PrimaryKeyConstraint("driver_id", "season_year", name="driver_numbers_pkey"),
    )

    driver_id: Mapped[str] = mapped_column(String(64), primary_key=True)
    season_year: Mapped[int] = mapped_column(SmallInteger, primary_key=True)
    driver_number: Mapped[int] = mapped_column(SmallInteger)

    driver: Mapped["Drivers"] = relationship("Drivers", back_populates="driver_numbers")
    seasons: Mapped["Seasons"] = relationship(
        "Seasons", back_populates="driver_numbers"
    )


class DriverTeamChanges(Base):
    __tablename__ = "driver_team_changes"
    __table_args__ = (
        ForeignKeyConstraint(
            ["driver_id"], ["drivers.id"], ondelete="RESTRICT", name="fk_driver_id"
        ),
        ForeignKeyConstraint(
            ["team_id"], ["teams.id"], ondelete="RESTRICT", name="fk_team_id"
        ),
        PrimaryKeyConstraint(
            "driver_id", "timestamp_start", name="driver_team_changes_pkey"
        ),
    )

    driver_id: Mapped[str] = mapped_column(String(64), primary_key=True)
    timestamp_start: Mapped[datetime.datetime] = mapped_column(
        TIMESTAMP(precision=6), primary_key=True
    )
    team_id: Mapped[int] = mapped_column(SmallInteger)
    timestamp_end: Mapped[Optional[datetime.datetime]] = mapped_column(
        TIMESTAMP(precision=6)
    )

    driver: Mapped["Drivers"] = relationship(
        "Drivers", back_populates="driver_team_changes"
    )
    team: Mapped["Teams"] = relationship("Teams", back_populates="driver_team_changes")


class Events(Base):
    __tablename__ = "events"
    __table_args__ = (
        ForeignKeyConstraint(
            ["circuit_id"], ["circuits.id"], ondelete="RESTRICT", name="fk_circuit_id"
        ),
        ForeignKeyConstraint(
            ["event_format_name"],
            ["event_formats.event_format_name"],
            ondelete="RESTRICT",
            name="fk_event_format_name",
        ),
        ForeignKeyConstraint(
            ["season_year"],
            ["seasons.season_year"],
            ondelete="RESTRICT",
            name="fk_season_year",
        ),
        PrimaryKeyConstraint("event_name", "season_year", name="events_pkey"),
    )

    event_name: Mapped[str] = mapped_column(Text, primary_key=True)
    event_official_name: Mapped[str] = mapped_column(Text)
    date_start: Mapped[datetime.date] = mapped_column(Date)
    country: Mapped[str] = mapped_column(String(3))
    season_year: Mapped[int] = mapped_column(SmallInteger, primary_key=True)
    event_format_name: Mapped[str] = mapped_column(String(32))
    circuit_id: Mapped[str] = mapped_column(String(8))

    circuit: Mapped["Circuits"] = relationship("Circuits", back_populates="events")
    event_formats: Mapped["EventFormats"] = relationship(
        "EventFormats", back_populates="events"
    )
    seasons: Mapped["Seasons"] = relationship("Seasons", back_populates="events")
    event_sessions: Mapped[List["EventSessions"]] = relationship(
        "EventSessions", back_populates="events"
    )
    laps: Mapped[List["Laps"]] = relationship("Laps", back_populates="events")


class TeamSeasonColors(Base):
    __tablename__ = "team_season_colors"
    __table_args__ = (
        ForeignKeyConstraint(
            ["season_year"],
            ["seasons.season_year"],
            ondelete="RESTRICT",
            name="fk_seasons_season_year",
        ),
        ForeignKeyConstraint(
            ["team_id"], ["teams.id"], ondelete="RESTRICT", name="fk_teams_id"
        ),
        PrimaryKeyConstraint("team_id", "season_year", name="team_season_colors_pkey"),
    )

    team_id: Mapped[int] = mapped_column(SmallInteger, primary_key=True)
    season_year: Mapped[int] = mapped_column(SmallInteger, primary_key=True)
    color: Mapped[str] = mapped_column(CHAR(7))

    seasons: Mapped["Seasons"] = relationship(
        "Seasons", back_populates="team_season_colors"
    )
    team: Mapped["Teams"] = relationship("Teams", back_populates="team_season_colors")


class EventSessions(Base):
    __tablename__ = "event_sessions"
    __table_args__ = (
        ForeignKeyConstraint(
            ["event_name", "season_year"],
            ["events.event_name", "events.season_year"],
            ondelete="RESTRICT",
            name="fk_event_name_season_year",
        ),
        ForeignKeyConstraint(
            ["session_type_id"],
            ["session_types.id"],
            ondelete="RESTRICT",
            name="fk_session_type_id",
        ),
        PrimaryKeyConstraint(
            "event_name", "season_year", "session_type_id", name="event_sessions_pkey"
        ),
    )

    event_name: Mapped[str] = mapped_column(Text, primary_key=True)
    season_year: Mapped[int] = mapped_column(SmallInteger, primary_key=True)
    session_type_id: Mapped[str] = mapped_column(String(32), primary_key=True)
    start_time: Mapped[datetime.datetime] = mapped_column(TIMESTAMP(precision=6))
    end_time: Mapped[datetime.datetime] = mapped_column(TIMESTAMP(precision=6))

    events: Mapped["Events"] = relationship("Events", back_populates="event_sessions")
    session_type: Mapped["SessionTypes"] = relationship(
        "SessionTypes", back_populates="event_sessions"
    )
    session_results: Mapped[List["SessionResults"]] = relationship(
        "SessionResults", back_populates="event_sessions"
    )
    session_weather_measurements: Mapped[List["SessionWeatherMeasurements"]] = (
        relationship("SessionWeatherMeasurements", back_populates="event_sessions")
    )


class Laps(Base):
    __tablename__ = "laps"
    __table_args__ = (
        ForeignKeyConstraint(
            ["compound_id"],
            ["compounds.id"],
            ondelete="RESTRICT",
            name="fk_compound_id",
        ),
        ForeignKeyConstraint(
            ["driver_id"], ["drivers.id"], ondelete="RESTRICT", name="fk_driver_id"
        ),
        ForeignKeyConstraint(
            ["event_name", "season_year"],
            ["events.event_name", "events.season_year"],
            ondelete="RESTRICT",
            name="fk_event_name_season_year",
        ),
        ForeignKeyConstraint(
            ["session_type_id"],
            ["session_types.id"],
            ondelete="RESTRICT",
            name="fk_session_type_id",
        ),
        PrimaryKeyConstraint("id", name="laps_pkey"),
        Index(
            "session_type_id_event_name_season_year_driver_id_lap_number_uni",
            "session_type_id",
            "season_year",
            "event_name",
            "driver_id",
            "lap_number",
            unique=True,
        ),
    )

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    lap_number: Mapped[int] = mapped_column(SmallInteger)
    stint: Mapped[int] = mapped_column(SmallInteger)
    event_name: Mapped[str] = mapped_column(Text)
    compound_id: Mapped[str] = mapped_column(String(16))
    driver_id: Mapped[str] = mapped_column(String(64))
    session_type_id: Mapped[str] = mapped_column(String(32))
    sector_1_time: Mapped[Optional[float]] = mapped_column(REAL)
    sector_2_time: Mapped[Optional[float]] = mapped_column(REAL)
    sector_3_time: Mapped[Optional[float]] = mapped_column(REAL)
    speedtrap_1: Mapped[Optional[int]] = mapped_column(SmallInteger)
    speedtrap_2: Mapped[Optional[int]] = mapped_column(SmallInteger)
    speedtrap_fl: Mapped[Optional[int]] = mapped_column(SmallInteger)
    season_year: Mapped[Optional[int]] = mapped_column(SmallInteger)
    laptime: Mapped[Optional[float]] = mapped_column(
        REAL,
        Computed("((sector_1_time + sector_2_time) + sector_3_time)", persisted=True),
    )
    pit_in_time: Mapped[Optional[float]] = mapped_column(REAL)
    pit_out_time: Mapped[Optional[float]] = mapped_column(REAL)
    is_inlap: Mapped[Optional[bool]] = mapped_column(
        Boolean, Computed("(pit_in_time IS NOT NULL)", persisted=True)
    )
    is_outlap: Mapped[Optional[bool]] = mapped_column(
        Boolean, Computed("(pit_out_time IS NOT NULL)", persisted=True)
    )

    compound: Mapped["Compounds"] = relationship("Compounds", back_populates="laps")
    driver: Mapped["Drivers"] = relationship("Drivers", back_populates="laps")
    events: Mapped[Optional["Events"]] = relationship("Events", back_populates="laps")
    session_type: Mapped["SessionTypes"] = relationship(
        "SessionTypes", back_populates="laps"
    )
    telemetry_measurements: Mapped[List["TelemetryMeasurements"]] = relationship(
        "TelemetryMeasurements", back_populates="lap"
    )


class SessionResults(Base):
    __tablename__ = "session_results"
    __table_args__ = (
        ForeignKeyConstraint(
            ["driver_id"], ["drivers.id"], ondelete="RESTRICT", name="fk_driver_id"
        ),
        ForeignKeyConstraint(
            ["event_name", "season_year", "session_type_id"],
            [
                "event_sessions.event_name",
                "event_sessions.season_year",
                "event_sessions.session_type_id",
            ],
            ondelete="RESTRICT",
            name="fk_event_sessions_event_name_season_year_session_type_id",
        ),
        PrimaryKeyConstraint("id", name="session_results_pkey"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    event_name: Mapped[Optional[str]] = mapped_column(Text)
    season_year: Mapped[Optional[int]] = mapped_column(SmallInteger)
    session_type_id: Mapped[Optional[str]] = mapped_column(String(32))
    driver_id: Mapped[Optional[str]] = mapped_column(String(64))

    driver: Mapped[Optional["Drivers"]] = relationship(
        "Drivers", back_populates="session_results"
    )
    event_sessions: Mapped[Optional["EventSessions"]] = relationship(
        "EventSessions", back_populates="session_results"
    )


class SessionWeatherMeasurements(Base):
    __tablename__ = "session_weather_measurements"
    __table_args__ = (
        ForeignKeyConstraint(
            ["event_name", "season_year", "session_type_id"],
            [
                "event_sessions.event_name",
                "event_sessions.season_year",
                "event_sessions.session_type_id",
            ],
            ondelete="RESTRICT",
            name="fk_event_name_season_year_session_type_id_event_sessions",
        ),
        PrimaryKeyConstraint(
            "time_at",
            "event_name",
            "season_year",
            "session_type_id",
            name="session_weather_measurements_pkey",
        ),
    )

    event_name: Mapped[str] = mapped_column(Text, primary_key=True)
    season_year: Mapped[int] = mapped_column(SmallInteger, primary_key=True)
    session_type_id: Mapped[str] = mapped_column(String(32), primary_key=True)
    time_at: Mapped[float] = mapped_column(REAL, primary_key=True)
    humidity: Mapped[int] = mapped_column(SmallInteger)
    air_pressure: Mapped[int] = mapped_column(SmallInteger)
    track_temp: Mapped[int] = mapped_column(SmallInteger)
    air_temp: Mapped[int] = mapped_column(SmallInteger)

    event_sessions: Mapped["EventSessions"] = relationship(
        "EventSessions", back_populates="session_weather_measurements"
    )


class TelemetryMeasurements(Base):
    __tablename__ = "telemetry_measurements"
    __table_args__ = (
        ForeignKeyConstraint(
            ["lap_id"], ["laps.id"], ondelete="RESTRICT", name="fk_lap_id"
        ),
        PrimaryKeyConstraint(
            "lap_id", "laptime_at", name="telemetry_measurements_pkey"
        ),
    )

    lap_id: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    laptime_at: Mapped[float] = mapped_column(REAL, primary_key=True)
    speed: Mapped[float] = mapped_column(REAL)
    rpm: Mapped[int] = mapped_column(SmallInteger)
    brake: Mapped[int] = mapped_column(SmallInteger)
    throttle: Mapped[int] = mapped_column(SmallInteger)
    distance: Mapped[float] = mapped_column(REAL)
    gear: Mapped[int] = mapped_column(SmallInteger)

    lap: Mapped["Laps"] = relationship("Laps", back_populates="telemetry_measurements")


class PracticeSessionResults(SessionResults):
    __tablename__ = "practice_session_results"
    __table_args__ = (
        ForeignKeyConstraint(
            ["id"],
            ["session_results.id"],
            ondelete="RESTRICT",
            name="fk_session_result_id",
        ),
        PrimaryKeyConstraint("id", name="practice_session_results_pkey"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    laptime: Mapped[Optional[float]] = mapped_column(REAL)
    gap: Mapped[Optional[float]] = mapped_column(REAL)


class QualifyingSessionResults(SessionResults):
    __tablename__ = "qualifying_session_results"
    __table_args__ = (
        ForeignKeyConstraint(
            ["id"],
            ["session_results.id"],
            ondelete="RESTRICT",
            name="fk_session_result_id",
        ),
        PrimaryKeyConstraint("id", name="qualifying_session_results_pkey"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    q1_laptime: Mapped[Optional[float]] = mapped_column(REAL)
    q2_laptime: Mapped[Optional[float]] = mapped_column(REAL)
    q3_laptime: Mapped[Optional[float]] = mapped_column(REAL)
    position: Mapped[Optional[int]] = mapped_column(SmallInteger)


class RaceSessionResults(SessionResults):
    __tablename__ = "race_session_results"
    __table_args__ = (
        ForeignKeyConstraint(
            ["id"],
            ["session_results.id"],
            ondelete="RESTRICT",
            name="fk_session_result_id",
        ),
        PrimaryKeyConstraint("id", name="race_session_results_pkey"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    total_time: Mapped[Optional[float]] = mapped_column(REAL)
    result_status: Mapped[Optional[str]] = mapped_column(Text)
    classified_position: Mapped[Optional[str]] = mapped_column(Text)
    points: Mapped[Optional[int]] = mapped_column(SmallInteger)
    gap: Mapped[Optional[float]] = mapped_column(REAL)
    grid_position: Mapped[Optional[int]] = mapped_column(
        SmallInteger, server_default=text("0")
    )

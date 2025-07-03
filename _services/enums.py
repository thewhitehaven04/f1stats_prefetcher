from typing import Sequence

from _repository.engine import postgres
from _repository.repository import Compounds, EventFormats, SessionTypes
from sqlalchemy.orm import Session


EVENT_FORMATS: Sequence[EventFormats] = [
    EventFormats("conventional"),
    EventFormats("sprint"),
    EventFormats("sprint_qualifying"),
    EventFormats("sprint_shootout"),
    EventFormats("testing"),
]

SESSION_TYPE_IDS: Sequence[SessionTypes] = [
    SessionTypes("Practice 1"),
    SessionTypes("Practice 2 "),
    SessionTypes("Practice 3"),
    SessionTypes("Qualifying"),
    SessionTypes("Race"),
    SessionTypes("Sprint"),
    SessionTypes("Sprint Qualifying"),
    SessionTypes("Sprint Shootout"),
]

COMPOUNDS: Sequence[Compounds] = [
    Compounds('HARD'),
    Compounds('MEDIUM'),
    Compounds('SOFT'),
    Compounds('WET'),
    Compounds('INTERMEDIATE'),
    Compounds('UNKNOWN'),
    Compounds('TEST_UNKNOWN')
]

def init_event_formats():
    with Session(postgres) as s:
        s.query(EventFormats)
        if s.query(EventFormats).count() == 0:
            s.add_all(EVENT_FORMATS)
            s.commit()

def init_session_types():
    with Session(postgres) as s:
        s.query(SessionTypes)
        if s.query(SessionTypes).count() == 0:
            s.add_all(SESSION_TYPE_IDS)
            s.commit()
        

def init_compounds():
    with Session(postgres) as s:
        s.query(Compounds)
        if s.query(Compounds).count() == 0:
            s.add_all(COMPOUNDS)
            s.commit()
from typing import Sequence

from _repository.engine import postgres
from _repository.repository import EventFormats
from sqlalchemy.orm import Session


EVENT_FORMATS: Sequence[EventFormats] = [
    EventFormats("conventional"),
    EventFormats("sprint"),
    EventFormats("sprint_qualifying"),
    EventFormats("sprint_shootout"),
    EventFormats("testing"),
]


def init_event_formats():
    with Session(postgres) as s:
        s.query(EventFormats)
        if s.query(EventFormats).count() == 0:
            s.add_all(EVENT_FORMATS)
            s.commit()

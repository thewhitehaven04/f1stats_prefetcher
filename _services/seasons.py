from sqlalchemy.orm import Session
from _repository.engine import postgres
from _repository.repository import Seasons

def init_season(season: int):
    with Session(postgres) as s:
        s.add(
            Seasons(season_year=season, descripton_text='text')
        )
        s.commit()
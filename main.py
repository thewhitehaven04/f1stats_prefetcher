from sanic import Sanic, json

from _services.fetcher import init_year_data, store_session_data_to_db
from _repository.engine import postgres


app = Sanic("app")
app.ctx.engine = postgres


@app.post("/season/<season>/event/<event:int>/session/<session:int>/populate")
async def populate_session_data(request, season: str, event: int, session: int):
    store_session_data_to_db(season, event, session)
    return json({"success": True})


@app.post("/season/<season:str>/initialize")
async def initialize_season_data(request, season: str):
    init_year_data(season)
    return json({"success": True})

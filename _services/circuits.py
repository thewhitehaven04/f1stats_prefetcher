import json
from os import getcwd
from typing import TypedDict
from sqlalchemy import update
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.orm import Session
from _repository.engine import postgres
from _repository.repository import Circuits


CircuitData = TypedDict(
    "CircuitData",
    {"lon": float, "lat": float, "zoom": int, "location": str, "name": str, "id": str},
)


def get_season_data(season: str) -> list[CircuitData]:
    with open(getcwd() + f"/static/championships/f1-locations-{season}.json", "r") as f:
        return json.loads(f.read())


def get_geojson_data(circuit_id: str) -> str:
    with open(getcwd() + f"/static/circuits/{circuit_id}.geojson", "r") as f:
        return f.read()


def store_circuit_data(season: str):
    try:
        circuits = get_season_data(season)
    except:
        raise ValueError("Invalid season")

    entries = []
    for circuit in circuits:
        circuit_id = circuit["id"]
        geojson = get_geojson_data(circuit_id)
        name = circuit["name"]
        entries.append({"name": name, "geojson": geojson, "id": circuit_id})

    with postgres.connect() as pg_con:
        pg_con.execute(insert(Circuits).values(entries).on_conflict_do_nothing())
        pg_con.commit()


def update_circuit_data(season: str):
    try:
        circuits = get_season_data(season)
    except:
        raise ValueError("Invalid season")

    entries = [
        {
            "name": circuit["name"],
            "id": circuit["id"],
            "geojson": get_geojson_data(circuit["id"]),
        }
        for circuit in circuits
    ]
    with Session(postgres) as s:
        s.execute(update(Circuits), entries)
        s.commit()

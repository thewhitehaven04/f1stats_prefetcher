import json
from os import getcwd
from typing import TypedDict
from sqlalchemy import MetaData, Table
from sqlalchemy.dialects.postgresql import insert
from _repository.engine import postgres


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
        circuits_table = Table("circuits", MetaData(), autoload_with=postgres)
        pg_con.execute(
            insert(table=circuits_table).values(entries).on_conflict_do_nothing()
        )
        pg_con.commit()

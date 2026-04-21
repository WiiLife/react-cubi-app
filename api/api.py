from api.db.repository import repository
from fastapi import APIRouter, Body, Response
from pydantic import BaseModel
from typing import List


class SelectBody(BaseModel):
    columns: List[str]


class PivotBody(BaseModel):
    columns: List[str]
    group_columns: List[str]

router = APIRouter(prefix="/api")

@router.get("/tables")
async def home():
    tables = await repository.tables()
    return {"tables": tables}

@router.get("/tables/{table_name}")
async def table(table_name: str):
    df = await repository.get_table(table_name)
    return Response(content=df.to_json(orient="records"), media_type="application/json")

@router.post("/tables/{table_name}")
async def select(table_name: str, payload: SelectBody = Body(...)):
    df = await repository.select(table_name, payload.columns)
    return Response(content=df.to_json(orient="records"), media_type="application/json")

@router.post("/tables/{table_name}/pivot")
async def pivot(table_name: str, payload: PivotBody = Body(...)):
    df = await repository.pivot(
        table=table_name, 
        columns=payload.columns, 
        operation_column="valore", 
        group_by_columns=payload.group_columns
    )
    return Response(content=df.to_json(orient="records"), media_type="application/json")

@router.get("/tables/{table_name}/columns")
async def table_columns(table_name: str):
    res = await repository.get_table_columns(table_name)
    return res

@router.get("/tables/{table_name}/columns-values")
async def table_columns_unique_values(table_name: str):
    res = await repository.get_table_columns_and_unique_values(table_name)
    return res
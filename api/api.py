from api.db.repository import repository, SQLOperation
from fastapi import APIRouter, Body, Response, HTTPException, status
from fastapi.responses import JSONResponse
from pydantic import BaseModel
from typing import List, Literal
import pandas as pd
import json


class SelectBody(BaseModel):
    columns: List[str]


class PivotBody(BaseModel):
    columns: dict[str, list[str]]
    operation: Literal["SUM", "COUNT", "AVG"] = "SUM"
    column_variables: list[str] | None = None
    row_limit: int
    row_offset: int

router = APIRouter(prefix="/api")

@router.get("/tables")
async def home():
    tables = await repository.tables()
    return tables

# not currently used in gui
@router.get("/tables/{table_name}")
async def table(table_name: str):
    df = await repository.get_table(table_name)
    return Response(content=df.to_json(orient="records"), media_type="application/json")

# not currently used in gui
@router.post("/tables/{table_name}")
async def select(table_name: str, payload: SelectBody = Body(...)):
    df = await repository.select(table_name, payload.columns)
    return Response(content=df.to_json(orient="records"), media_type="application/json")

@router.post("/tables/{table_name}/pivot")
async def pivot(table_name: str, payload: PivotBody = Body(...)):
    df, n_rows = await repository.pivot(
        table=table_name,
        columns=payload.columns,
        column_variables=payload.column_variables,
        operation=SQLOperation(payload.operation),
        row_limit=payload.row_limit,
        row_offset=payload.row_offset,
    )

    # implement limit also for columns (if we want to keep efficiency we can simply have [first_few_cols...last_few_cols])

    df = df.fillna('null')  
    return JSONResponse({
        "data": df.to_dict(orient="records"), 
        "n_rows": n_rows
    })

@router.get("/tables/{table_name}/columns")
async def table_columns(table_name: str):
    res = await repository.get_table_columns(table_name)
    return res

@router.get("/tables/{table_name}/columns-values")
async def table_columns_unique_values(table_name: str):
    res = await repository.get_table_columns_and_unique_values(table_name)
    return res

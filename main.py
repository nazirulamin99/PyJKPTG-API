from datetime import date, timedelta

import duckdb
from fastapi import FastAPI, Query
from pydantic import BaseModel
from collections import defaultdict
from typing import Optional


app = FastAPI(title="JKPTG API")


# --- Schemas ---


class Ship(BaseModel):
    fileNo: Optional[str] = None
    company: Optional[str] = None
    shipName: Optional[str] = None
    shipCapacity: Optional[float] = None
    licenseNo: Optional[float] = None
    concession: Optional[str] = None
    licensePeriod: Optional[str] = None
    project: Optional[str] = None
    allowOptPeriod: Optional[str] = None
    optTime: Optional[str] = None
    royalty: Optional[str] = None
    official: Optional[str] = None
    licensePeriodStart: Optional[str] = None
    licensePeriodEnd: Optional[str] = None
    allowOptStart: Optional[str] = None
    allowOptEnd: Optional[str] = None
    optTimeStart: Optional[str] = None
    optimeEnd: Optional[str] = None


class ShipExpiring(Ship):
    daysRemaining: Optional[int] = None


class Coordinate(BaseModel):
    latitude: Optional[str] = None
    longitude: Optional[str] = None


class License(BaseModel):
    BilNo: Optional[int] = None
    licenseNo: Optional[str] = None
    company: Optional[str] = None
    licenseDate: Optional[str] = None
    expiredDate: Optional[str] = None
    zone: Optional[str] = None
    area_km2: Optional[str] = None
    geometryPoint: Optional[str] = None
    coordinate: Optional[Coordinate] = None
    lat: Optional[str] = None
    lng: Optional[str] = None
    status: Optional[str] = None
    remark: Optional[str] = None
    noFile: Optional[str] = None
    region: Optional[str] = None


class LicenseExpiring(License):
    daysRemaining: Optional[int] = None

SHIPLIST_PATH = "./data/Shiplist-20260112.parquet"
LICENSELIST_PATH = "./data/Licenselist-20260122-testing.parquet"


def query_parquet(path: str, where_clause: str = "", params: list = []) -> list[dict]:
    sql = f"SELECT * FROM '{path}'"
    if where_clause:
        sql += f" WHERE {where_clause}"
    result = duckdb.execute(sql, params)
    columns = [desc[0] for desc in result.description]
    rows = result.fetchall()
    return [dict(zip(columns, row)) for row in rows]

# --- Shiplist Endpoints ---

@app.get("/shiplist", tags=["Shiplist"], response_model=list[Ship])
def get_shiplist(
    concession: Optional[str] = Query(None, description="Filter by concession (e.g. Perak, Selangor)"),
    company: Optional[str] = Query(None, description="Filter by company name (partial match)"),
    file_no: Optional[str] = Query(None, description="Filter by file number (e.g. SK 561)"),
    license_no: Optional[int] = Query(None, description="Filter by license number"),
):
    conditions = []
    params = []

    if concession:
        conditions.append("concession ILIKE '%' || $1 || '%'")
        params.append(concession)
    if company:
        conditions.append(f"company ILIKE '%' || ${len(params) + 1} || '%'")
        params.append(company)
    if file_no:
        conditions.append(f"fileNo = ${len(params) + 1}")
        params.append(file_no)
    if license_no:
        conditions.append(f"licenseNo = ${len(params) + 1}")
        params.append(license_no)

    where = " AND ".join(conditions)
    return query_parquet(SHIPLIST_PATH, where, params)


@app.get("/shiplist/expiring", tags=["Shiplist"], response_model=list[ShipExpiring])
def get_expiring_licenses():
    result = duckdb.execute(f"""
        SELECT *,
            CAST(licensePeriodEnd AS DATE) - CURRENT_DATE AS daysRemaining
        FROM '{SHIPLIST_PATH}'
        ORDER BY licensePeriodEnd ASC
    """)
    columns = [desc[0] for desc in result.description]
    return [dict(zip(columns, row)) for row in result.fetchall()]


# @app.get("/companies")
# def get_companies():
#     result = duckdb.execute(f"""
#         SELECT DISTINCT fileNo, company
#         FROM '{PARQUET_PATH}'
#         ORDER BY fileNo
#     """)
#     columns = [desc[0] for desc in result.description]
#     return [dict(zip(columns, row)) for row in result.fetchall()]


@app.get("/companies/{file_no}", tags=["Shiplist"], response_model=list[Ship])
def get_company(file_no: str):
    return query_parquet(SHIPLIST_PATH, "fileNo = $1", [file_no])


@app.get("/concessions", tags=["Shiplist"], response_model=list[str])
def get_concessions():
    result = duckdb.execute(f"""
        SELECT DISTINCT concession
        FROM '{SHIPLIST_PATH}'
        ORDER BY concession
    """)
    return [row[0] for row in result.fetchall()]


@app.get("/concessions/{name}", tags=["Shiplist"], response_model=list[Ship])
def get_concession(name: str):
    return query_parquet(SHIPLIST_PATH, "concession ILIKE '%' || $1 || '%'", [name])

# --- Licenselist Endpoints ---


def nest_coordinates(rows: list[dict]) -> list[dict]:
    for row in rows:
        row["coordinate"] = {
            "latitude": row.pop("latitude", None),
            "longitude": row.pop("longitude", None),
        }
        # row.pop("lat", None)
        # row.pop("lng", None)
    return rows


@app.get("/licenselist", tags=["Licenselist"], response_model=dict[str, list[License]])
def get_licenselist(
    region: Optional[str] = Query(None, description="Filter by region (e.g. PAHANG, JOHOR)"),
    company: Optional[str] = Query(None, description="Filter by company name (partial match)"),
    license_no: Optional[str] = Query(None, description="Filter by license number"),
    status: Optional[str] = Query(None, description="Filter by status (partial match)"),
    no_file: Optional[str] = Query(None, description="Filter by file number (e.g. SK 338)"),
):
    conditions = []
    params = []

    if region:
        conditions.append(f"region ILIKE '%' || ${len(params) + 1} || '%'")
        params.append(region)
    if company:
        conditions.append(f"company ILIKE '%' || ${len(params) + 1} || '%'")
        params.append(company)
    if license_no:
        conditions.append(f"licenseNo = ${len(params) + 1}")
        params.append(license_no)
    if status:
        conditions.append(f"status ILIKE '%' || ${len(params) + 1} || '%'")
        params.append(status)
    if no_file:
        conditions.append(f"noFile = ${len(params) + 1}")
        params.append(no_file)

    where = " AND ".join(conditions)
    rows = nest_coordinates(query_parquet(LICENSELIST_PATH, where, params))
    grouped = defaultdict(list)
    for row in rows:
        key = row.pop("licenseNo")
        grouped[key].append(row)
    return grouped


@app.get("/licenselist/expiring", tags=["Licenselist"], response_model=dict[str, list[LicenseExpiring]])
def get_expiring_licenselist():
    result = duckdb.execute(f"""
        SELECT *,
            CAST(strptime(expiredDate, '%d/%m/%Y') AS DATE) - CURRENT_DATE AS daysRemaining
        FROM '{LICENSELIST_PATH}'
        ORDER BY expiredDate ASC
    """)
    columns = [desc[0] for desc in result.description]
    rows = nest_coordinates([dict(zip(columns, row)) for row in result.fetchall()])
    grouped = defaultdict(list)
    for row in rows:
        key = row.pop("licenseNo")
        grouped[key].append(row)
    return grouped


@app.get("/regions", tags=["Licenselist"], response_model=list[str])
def get_regions():
    result = duckdb.execute(f"""
        SELECT DISTINCT region
        FROM '{LICENSELIST_PATH}'
        ORDER BY region
    """)
    return [row[0] for row in result.fetchall()]


@app.get("/regions/{name}", tags=["Licenselist"], response_model=dict[str, list[License]])
def get_region(name: str):
    rows = nest_coordinates(query_parquet(LICENSELIST_PATH, "region ILIKE '%' || $1 || '%'", [name]))
    grouped = defaultdict(list)
    for row in rows:
        key = row.pop("licenseNo")
        grouped[key].append(row)
    return grouped

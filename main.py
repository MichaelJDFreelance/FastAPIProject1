from fastapi import FastAPI
import csv, json, pathlib
from typing import Optional
from functools import lru_cache
from rapidfuzz import fuzz, process
from pydantic import BaseModel
from slowapi import Limiter
from slowapi.util import get_remote_address
from slowapi.errors import RateLimitExceeded
from slowapi.middleware import SlowAPIMiddleware
from fastapi import Query, Response
from fastapi import HTTPException
from fastapi import Request

in_path = pathlib.Path("./data/cities15000.txt")
out_path = pathlib.Path("./data/cities15000.json")

class City(BaseModel):
    name: str
    lat: float
    lng: float
    tz: str
    pop: int
    loc: str

    class Config:
        schema_extra = {
            "example": {
                "name": "London",
                "lat": 51.50853,
                "lng": -0.12574,
                "tz": "Europe/London",
                "pop": 7556900,
                "loc": "GB"
            }
        }

limiter = Limiter(key_func=get_remote_address)

app = FastAPI(
    title="Cities API",
    description="""
A feature-rich API for searching and filtering world cities.

### Features
- Fuzzy search (RapidFuzz)
- Filter by timezone and population
- Pagination
- Sorting
- Input validation
- Rate limiting ready
""",
    version="1.0.0",
)
app.state.limiter = limiter
app.add_exception_handler(RateLimitExceeded, lambda r, e: Response("Too Many Requests", status_code=429))
app.add_middleware(SlowAPIMiddleware)

@lru_cache()
def load_cities():
    records = []
    with open(in_path, "r", encoding="utf-8") as fin:
        reader = csv.reader(fin, delimiter="\t")
        for row in reader:
            name = row[1]
            lat = float(row[4])
            lng = float(row[5])
            tz = row[-2]
            pop = int(row[14]) if row[14] else 0
            loc = row[8]
            records.append({"name": name, "lat": lat, "lng": lng, "tz": tz, "pop": pop, "loc":loc})

    return records

@app.get(
    "/cities",
    response_model=list[City],
    tags=["Cities"],
    summary="Search and filter cities",
    description="""
Returns a list of cities filtered by:

- **q** — fuzzy text search  
- **tz** — timezone (exact match)  
- **min_pop** — minimum population  
- **pagination** — limit/offset  
- **sorting** — sort by name, pop, etc.

Uses **RapidFuzz** for matching.
"""
)
@limiter.limit("10/minute")
async def get_cities(
    request: Request,  # <-- REQUIRED BY SLOWAPI
    q: Optional[str] = Query(
        None,
        example="York",
        description="Fuzzy search by city name"
    ),
    tz: Optional[str] = Query(
        None,
        example="Europe/London",
        description="Timezone exact match"
    ),
    min_pop: Optional[int] = Query(
        None,
        example=500000,
        description="Minimum population"
    ),
    limit: int = Query(
        50,
        example=10,
        description="Pagination limit"
    ),
    offset: int = Query(
        0,
        example=0,
        description="Pagination offset"
    ),
    sort: Optional[str] = Query(
        None,
        example="pop",
        description="Sort key (name, pop, tz)"
    ),
    order: str = Query(
        "asc",
        example="desc",
        description="Sort order"
    ),
):
    if not q and not tz and min_pop is None:
        raise HTTPException(
            status_code=400,
            detail="Please provide at least one filter: q, tz, or min_pop"
        )

    data = load_cities()

    if q:
        data = [
            c for c in data
            if fuzz.partial_ratio(q.lower(), c["name"].lower()) >= 70
        ]
    if tz:
        data = [c for c in data if c["tz"] == tz]
    if min_pop is not None:
        data = [c for c in data if c["pop"] >= min_pop]
    if sort:
        reverse = (order == "desc")
        data = sorted(data, key=lambda c: c.get(sort), reverse=reverse)

    paged = data[offset: offset + limit]

    return paged
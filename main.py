from fastapi import Depends, FastAPI, HTTPException, Query, Response
from fastapi.responses import StreamingResponse
from pydantic import BaseModel, HttpUrl
from typing import Optional
import uvicorn
import sqlite3

import database
from bench import details_from_url

class ReqBody(BaseModel):
    url: str


db_conn = sqlite3.connect('data.db')

write_to_in_attendance = database.store_in_table("in")

# Migrations
stmts = """
CREATE TABLE IF NOT EXISTS 'in'  ( roll_num, name, department, time, PRIMARY KEY(roll_num)) ;
CREATE TABLE IF NOT EXISTS 'out' ( roll_num, name, department, time, PRIMARY KEY(roll_num)) ;
""".split(";")

for i in stmts:
    db_conn.execute(i)

app = FastAPI(
    title="URL Processing API",
    description="A simple API for processing URLs",
    version="1.0.0"
)

# Models for request/response
class URLResponse(BaseModel):
    success: bool
    message: str
    url: str

# URL validation using Pydantic's HttpUrl
async def validate_url(url: str) -> str:
    try:
        # HttpUrl will validate the URL format
        validated_url = HttpUrl(url)

        print(validated_url)

        if validated_url.host and validated_url.host == "www.rajalakshmi.org":
            return str(validated_url)

        raise HTTPException(status_code=400, detail="only rajalakshmi.org urls")

    except Exception:
        raise HTTPException(status_code=400, detail="Invalid URL format")

@app.post("/in", response_model=URLResponse)
async def process_in_url(body: ReqBody):
    """
    Process incoming URL endpoint
    """

    url = body.url

    try:
        validated_url = await validate_url(url)
        print(validated_url)

        name, roll_num, dept = details_from_url(validated_url)

        write_to_in_attendance(database.AttendanceRecord(roll_num=roll_num, name=name, department=dept) , db_connection=db_conn)


        return URLResponse(
            success=True,
            message="URL processed successfully",
            url=validated_url
        )
    except Exception as e:
        print(str(e))
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/export", response_model=URLResponse)
async def process_out_url():
    data = database.get_table_as_csv_buffer(db_conn)
    return StreamingResponse(
        iter([data.getvalue()]),
        media_type="text/csv",
        headers={
            'Content-Disposition': 'attachment; filename="export.csv"',
            'Access-Control-Expose-Headers': 'Content-Disposition'
        }
    )

@app.get("/health")
async def health_check():
    return {"status": "healthy"}

from fastapi.middleware.cors import CORSMiddleware

app.add_middleware(
    CORSMiddleware,
    allow_origins=['*'],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

if __name__ == "__main__":
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True)

from typing import Optional
from pydantic import BaseModel
from app.config import model


class TickersLiveData(BaseModel):
    tool_type : int
    
    class Config:
        orm_mode=True

class TickersHistoricalData(BaseModel):
    tool_type : int
    period : str
    
    class Config:
        orm_mode=True

class getPayload(BaseModel):
    ticker_id:str

    class Config:
        orm_mode=True

class dates(BaseModel):
    year:int
    month:int
    day:int

    class Config:
        orm_mode=True

class TickersHistoricalDatabhav(BaseModel):
    start_date:dates
    end_date:dates
    
    class Config:
        orm_mode=True
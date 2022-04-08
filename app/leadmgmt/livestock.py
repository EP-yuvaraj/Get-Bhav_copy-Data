
from multiprocessing.sharedctypes import Value
from sqlite3 import Date
from fastapi import status
from sqlalchemy import null
from app.config.dbconfig import sessionLocal,conn
from app.config import model,app_config
import psycopg2.extras
from app.leadmgmt.utils import *
from app.leadmgmt import consumer
from app.leadmgmt import producer
# from app.leadmgmt.geticker import add_to_mst
from fastapi import APIRouter
from app.config import schemas
import yfinance as yf
from app.config.dbconfig import engine
from json import dumps
from ipaddress import collapse_addresses
from json import loads
from datetime import datetime
import os
import json


import time

from apscheduler.schedulers.background import BackgroundScheduler

globalvar=1

scheduler = BackgroundScheduler(timezone="Asia/Kolkata")


post_route = APIRouter()

db=sessionLocal()



@post_route.post("/stock/live/all/get")
def getAllLive():
    new_data = db.query(model.TickersCurrentData).all() 
    counts=len(new_data)
  
    return jsonresponse("200","success","School information Fetched successfully ","",counts,new_data)


@post_route.post("/stock/kafka/livedata/get")
def getLive(get:schemas.getPayload):

    producer.producerGetLive(get.ticker_id)
    new_data2=consumer.consumerGetLive()

    return jsonresponse("200","success","School information Fetched successfully ","",new_data2[0],new_data2[1])

@post_route.post("/stock/kafka/historical/get")
def getHistory(get:schemas.getPayload):
    producer.producerGetHistory(get.ticker_id)
    new_data2=consumer.consumerGetHistory()

    return jsonresponse("200","success","School information Fetched successfully ","",new_data2[0],new_data2[1])

@post_route.post("/stock/kafka/previousday/historical/get")
def getHistory(get:schemas.getPayload):
    producer.producerGetDailyHistory(get.ticker_id)
    new_data2=consumer.consumerGetDailyHistory()

    return jsonresponse("200","success","History information Fetched successfully ","",new_data2[0],new_data2[1])


# @post_route.post('/add_to_mst')
# def add():
#     add_to_mst()
#     return jsonresponse("200","success","Added Tickers successfully ","","","")


@post_route.post("/bhav/stock/kafka/live/add")
def addHistory():
        producer.bproducerMethodlive()
        return jsonresponse("200", "success", "Live Data Added successfully ", "", "1", "1")


@post_route.post("/bhav/stock/kafka/history/add")
def addHistory(request:schemas.TickersHistoricalDatabhav):
        start=request.start_date
        end=request.end_date
        producer.bproducerMethodhistory(start,end)
        return jsonresponse("200", "success", "Historical information Added successfully ", "", "1", "1")


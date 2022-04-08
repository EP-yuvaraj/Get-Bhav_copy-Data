from json import dumps
from kafka import KafkaProducer 
from app.leadmgmt import consumer
import yfinance as yf
import psycopg2.extras
from app.config.dbconfig import conn
from jugaad_data.nse import NSELive
from datetime import date
from jugaad_data.nse import stock_df

n = NSELive()

# conn = psycopg2.connect('postgresql://postgres:yuviboxer@localhost/stock_db')


producer = KafkaProducer(bootstrap_servers=['localhost:9092'], value_serializer=lambda x:dumps(x).encode('utf-8'))

def producerGetDailyHistory(ticker_id):
    checkid=ticker_id
    producer.send('bv-getdaily', value=checkid)

def producerGetHistory(ticker_id):
    checkid=ticker_id
    producer.send('bvget-historical2', value=checkid)

def producerGetLive(ticker_id):
    producer.send('bvget-live', value=ticker_id)

# def producerMethodHistory(request,period1):
#         if(request==1):
#             cursor = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
#             cursor.execute('select ticker_name from mst_stock_detail')

#             symbols=[]
#             for record in cursor.fetchall():
#                 symbols.append(record[0])
#             tickers = [yf.Ticker(symbol) for symbol in symbols]
#             print(symbols)
#             i=0
#             for ticker in tickers:
#                 info_data=ticker.history(period=period1)
#                 history_daya=info_data.to_json()
#                 print(symbols[i])
#                 producer.send('historical-data', value=history_daya)
#                 data=consumer.ConsumerHistory(symbols[i])
#                 i=i+1
#                 print("next")
#         return data
#         return jsonresponse("400","Fail","Only Yahoo Service is available now ","","","")


# def producerMethod(request):
#     if(request==1):
#         cursor = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
#         cursor.execute('select ticker_name from mst_stock_detail')
#         symbols=[]
#         for record in cursor.fetchall():
#             symbols.append(record[0])
#         tickers = [yf.Ticker(symbol) for symbol in symbols]
#         print(symbols)
#         for ticker in tickers:
#             info_data=ticker.info
#             producer.send('test-topic-test', value=info_data)
#             data=consumer.Consumer()
#     return data


def bproducerMethodlive():
        cursor = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
        cursor.execute('select ticker_name from mst_stock_detail')
        symbols=[]
        for record in cursor.fetchall():
            symbols.append(record[0])
        print(symbols)
        for symbol in symbols:
            dic = n.stock_quote(symbol)
            print(dic)
            info_data={}
            info_data['symbol']=dic['info']['symbol']
            info_data['isin']=dic['info']['isin']
            info_data['currentprice']=dic['priceInfo']['lastPrice']
            info_data['dayhigh']=dic['priceInfo']['intraDayHighLow']['max']
            info_data['daylow']=dic['priceInfo']['intraDayHighLow']['min']
            print(info_data)
            # info_data=dic['priceInfo']
            producer.send('bvtest', value=info_data)
            data=consumer.bConsumerMethodlive()
        return data


def bproducerMethodhistory(start,end):
            print(start.year)
            cursor = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
            cursor.execute('select ticker_name from mst_stock_detail')

            symbols=[]
            for record in cursor.fetchall():
                symbols.append(record[0])
            print(symbols)
            i=0
            for symbolss in symbols:
                info_data= stock_df(symbol=symbolss, from_date=date(start.year,start.month,start.day),
                                    to_date=date(end.year,end.month,end.day), series="EQ")
                history_daya=info_data.to_json()
                print(history_daya)
                producer.send('historical-bvtest', value=history_daya)
                data=consumer.bConsumerMethodhistory(symbolss)
                i=i+1
                print("next")
            return data
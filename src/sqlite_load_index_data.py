import urllib
import sqlite3
from datetime import datetime
from math import log

# Download stock data from Google Finance
def get_data(interval,ticker):
    base_url = "http://www.google.com/finance/getprices?"
    search_query = "i={}&p=45d&f=d,o,h,l,c,v&df=cpct&q={}".format(interval,ticker)
    search_url = '{}{}'.format(base_url, search_query)
    # Read data from Google Finance URL
    read_url=urllib.urlopen(search_url).readlines()
    
    # Read stock data line by line and extract data elements
    trans_id=0
    for element in read_url[7:]:
        if element.split(",")[0][0]=="a":
            linux_date_init=int(element.split(",")[0][1:])
            linux_date=linux_date_init
        else:
             linux_date=linux_date_init+int(element.split(",")[0])*int(interval)        
        date_time=datetime.fromtimestamp(linux_date).strftime("%Y-%m-%d %H:%M")
        week_day=datetime.fromtimestamp(linux_date).strftime("%a")
        
        # Actual price and volume data
        open_price=float(element.split(",")[4])
        high_price=float(element.split(",")[2])
        low_price=float(element.split(",")[3])
        close_price=float(element.split(",")[1])
        volume=int(element.split(",")[5])
        
        # Natural log of price and volume data
        ln_open_price=log(float(element.split(",")[4]))
        ln_high_price=log(float(element.split(",")[2]))
        ln_low_price=log(float(element.split(",")[3]))
        ln_close_price=log(float(element.split(",")[1]))
        if int(element.split(",")[5])==0:
            ln_volume=0
        else:
            ln_volume=log(int(element.split(",")[5]))

        # Insert records into table
        sql_conn.execute("INSERT INTO INDEX_PRICES_1DAY (TRANS_ID,LINUX_DATE,DATE_TIME,WEEK_DAY,TICKER,\
                          OPEN_PRICE,HIGH_PRICE,LOW_PRICE,CLOSE_PRICE,VOLUME,LN_OPEN_PRICE,LN_HIGH_PRICE,\
                          LN_LOW_PRICE,LN_CLOSE_PRICE,LN_VOLUME) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",\
                         (trans_id,linux_date,date_time,week_day,ticker,open_price,high_price,low_price,\      		  		  close_price,volume,ln_open_price,ln_high_price,ln_low_price,ln_close_price,\        				  ln_volume));
        sql_conn.commit()
        trans_id +=1
    
if __name__ == "__main__":
    try:
        # Get indices tickers
        index_list=[".DJI",".INX"]
        
        # Open SQLite database 
        sql_conn=sqlite3.connect("/home/kirill/project/data/stocks.db")
        print("Stocks database opened sucessfully")
        
        # Load data for each ticker
        for index in index_list:
            print(index)
            get_data(86400,index)
            
        # Close SQLite database     
        sql_conn.close()
        print("Index data from Google Finance is loaded successfully in SQLite database")
        print("Stocks database closed sucessfully")
        
    except BaseException:
        pass

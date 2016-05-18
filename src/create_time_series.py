import sqlite3
import csv

# Open SQLite database 
sql_conn=sqlite3.connect("/home/kirill/project/data/stocks.db")

# Query stocks data
cursor=sql_conn.execute(
    """ 
    select sp.ticker, sp.trans_id, sp.date_time, sp.low_price, max_min.max_price, 
           max_min.min_price, 
           round((sp.low_price-max_min.min_price)/(max_min.max_price-max_min.min_price),4) norm_price
    from STOCK_PRICES_1DAY as sp, 
         (select ticker, max(low_price) max_price, min(low_price) min_price 
          from STOCK_PRICES_1DAY
          group by ticker) as max_min
    where sp.ticker=max_min.ticker
    order by sp.ticker, sp.trans_id
    """)

# Save output in CSV files
csv_output=open("/home/kirill/project/data/time_series.csv", "w")
csvwriter=csv.writer(csv_output, delimiter=",")
csvwriter.writerows(cursor)
csv_output.close()

# Close SQLite database     
sql_conn.close()

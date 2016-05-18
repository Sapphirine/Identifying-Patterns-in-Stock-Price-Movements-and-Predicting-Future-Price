import sqlite3

sql_conn=sqlite3.connect("/home/kirill/project/data/stocks.db")
print "Stocks database opened sucessfully"

# Create "1 day frequency" tables to store stocks and index price data
sql_conn.execute("""CREATE TABLE STOCK_PRICES_1DAY
       ( TRANS_ID INTEGER NOT NULL,
         LINUX_DATE INTEGER NOT NULL,
         DATE_TIME DATETIME NOT NULL,
         WEEK_DAY VARCHAR(3) NOT NULL,
         TICKER VARCHAR(10) NOT NULL,
         OPEN_PRICE DECIMAL(20,20) NOT NULL,
         HIGH_PRICE DECIMAL(20,20) NOT NULL,
         LOW_PRICE DECIMAL(20,20) NOT NULL,
         CLOSE_PRICE DECIMAL(20,20) NOT NULL,
         VOLUME INTEGER NOT NULL,
         LN_OPEN_PRICE DECIMAL(20,20) NOT NULL,
         LN_HIGH_PRICE DECIMAL(20,20) NOT NULL,
         LN_LOW_PRICE DECIMAL(20,20) NOT NULL,
         LN_CLOSE_PRICE DECIMAL(20,20) NOT NULL,
         LN_VOLUME DECIMAL(20,20) NOT NULL,
         PRIMARY KEY (TRANS_ID,TICKER,LINUX_DATE)
        );""")
print "STOCK_PRICES_1DAY table created successfully";

sql_conn.execute("""CREATE TABLE INDEX_PRICES_1DAY
       ( TRANS_ID INTEGER NOT NULL,
         LINUX_DATE INTEGER NOT NULL,
         DATE_TIME DATETIME NOT NULL,
         WEEK_DAY VARCHAR(3) NOT NULL,
         TICKER VARCHAR(10) NOT NULL,
         OPEN_PRICE DECIMAL(20,20) NOT NULL,
         HIGH_PRICE DECIMAL(20,20) NOT NULL,
         LOW_PRICE DECIMAL(20,20) NOT NULL,
         CLOSE_PRICE DECIMAL(20,20) NOT NULL,
         VOLUME INTEGER NOT NULL,
         LN_OPEN_PRICE DECIMAL(20,20) NOT NULL,
         LN_HIGH_PRICE DECIMAL(20,20) NOT NULL,
         LN_LOW_PRICE DECIMAL(20,20) NOT NULL,
         LN_CLOSE_PRICE DECIMAL(20,20) NOT NULL,
         LN_VOLUME DECIMAL(20,20) NOT NULL,
         PRIMARY KEY (TRANS_ID,TICKER,LINUX_DATE)
        );""")
print "INDEX_PRICES_1DAY table created successfully";
sql_conn.commit()

sql_conn.close()
print("Stocks database closed sucessfully")


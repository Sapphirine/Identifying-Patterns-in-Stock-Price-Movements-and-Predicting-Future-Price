import sqlite3
import csv

# Connect to SQLite database
sql_conn=sqlite3.connect("/home/kirill/project/data/stocks.db")

# Get ticker
input_file=open("/home/kirill/project/data/stock_details.csv","r")
k=0
for line in input_file:
    while k<>1:
        ticker=line.strip()
        k=1
input_file.close()

# Generate training dataset
cursor=sql_conn.execute("""  
        select 	comb_table.future_open_change, comb_table.curr_open_change, comb_table.prior_open_change, comb_table.prior_2_open_change,
                comb_table.curr_high_change, comb_table.prior_high_change, comb_table.prior_2_high_change, comb_table.curr_low_change, 
                comb_table.prior_low_change, comb_table.prior_2_low_change, comb_table.curr_close_change, comb_table.prior_close_change, 
                comb_table.prior_2_close_change, comb_table.curr_volume_change, comb_table.prior_volume_change, comb_table.prior_2_volume_change,
                comb_table.mon, comb_table.tue, comb_table.wed, comb_table.thu, comb_table.fri, comb_table.uptrend, comb_table.downtrend,
                comb_table.dow_curr_open_change, comb_table.dow_prior_open_change, comb_table.dow_prior_2_open_change,
                comb_table.dow_curr_high_change, comb_table.dow_prior_high_change, comb_table.dow_prior_2_high_change,
                comb_table.dow_curr_low_change, comb_table.dow_prior_low_change, comb_table.dow_prior_2_low_change,
                comb_table.dow_curr_close_change, comb_table.dow_prior_close_change, comb_table.dow_prior_2_close_change, 
                comb_table.dow_curr_volume_change, comb_table.dow_prior_volume_change, comb_table.dow_prior_2_volume_change,
                comb_table.dow_uptrend, comb_table.dow_downtrend, sp_table.curr_open_change as sp_curr_open_change, sp_table.prior_open_change as sp_prior_open_change, sp_table.prior_2_open_change as sp_prior_2_open_change,
                sp_table.curr_high_change as sp_curr_high_change, sp_table.prior_high_change as sp_prior_high_change, sp_table.prior_2_high_change as sp_prior_2_high_change,
                sp_table.curr_low_change as sp_curr_low_change, sp_table.prior_low_change as sp_prior_low_change, sp_table.prior_2_low_change as sp_prior_2_low_change,
                sp_table.curr_close_change as sp_curr_close_change, sp_table.prior_close_change as sp_prior_close_change, sp_table.prior_2_close_change as sp_prior_2_close_change, 
                sp_table.curr_volume_change as sp_curr_volume_change, sp_table.prior_volume_change as sp_prior_volume_change, sp_table.prior_2_volume_change as sp_prior_2_volume_change,
                sp_table.uptrend as sp_uptrend, sp_table.downtrend as sp_downtrend
        from (
        select 	stock_table.linux_date, stock_table.future_open_change, stock_table.curr_open_change, stock_table.prior_open_change, stock_table.prior_2_open_change,
                stock_table.curr_high_change, stock_table.prior_high_change, stock_table.prior_2_high_change, stock_table.curr_low_change, 
                stock_table.prior_low_change, stock_table.prior_2_low_change, stock_table.curr_close_change, stock_table.prior_close_change, 
                stock_table.prior_2_close_change, stock_table.curr_volume_change, stock_table.prior_volume_change, stock_table.prior_2_volume_change,
                stock_table.mon, stock_table.tue, stock_table.wed, stock_table.thu, stock_table.fri, stock_table.uptrend, stock_table.downtrend,
                dow_table.curr_open_change as dow_curr_open_change, dow_table.prior_open_change as dow_prior_open_change, dow_table.prior_2_open_change as dow_prior_2_open_change,
                dow_table.curr_high_change as dow_curr_high_change, dow_table.prior_high_change as dow_prior_high_change, dow_table.prior_2_high_change as dow_prior_2_high_change,
                dow_table.curr_low_change as dow_curr_low_change, dow_table.prior_low_change as dow_prior_low_change, dow_table.prior_2_low_change as dow_prior_2_low_change,
                dow_table.curr_close_change as dow_curr_close_change, dow_table.prior_close_change as dow_prior_close_change, dow_table.prior_2_close_change as dow_prior_2_close_change, 
                dow_table.curr_volume_change as dow_curr_volume_change, dow_table.prior_volume_change as dow_prior_volume_change, dow_table.prior_2_volume_change as dow_prior_2_volume_change,
                dow_table.uptrend as dow_uptrend, dow_table.downtrend as dow_downtrend
        from (
        select 	g.linux_date,
                g.future_open-g.curr_open as future_open_change,
                g.curr_open-g.prior_open as curr_open_change, g.prior_open-g.prior_2_open as prior_open_change, g.prior_2_open-h.ln_open_price as prior_2_open_change,
                g.curr_high-g.prior_high as curr_high_change, g.prior_high-g.prior_2_high as prior_high_change, g.prior_2_high-h.ln_high_price as prior_2_high_change,
                g.curr_low-g.prior_low as curr_low_change, g.prior_low-g.prior_2_low as prior_low_change, g.prior_2_low-h.ln_low_price as prior_2_low_change,
                g.curr_close-g.prior_close as curr_close_change, g.prior_close-g.prior_2_close as prior_close_change, g.prior_2_close-h.ln_close_price as prior_2_close_change, 
                g.curr_volume-g.prior_volume as curr_volume_change, g.prior_volume-g.prior_2_volume as prior_volume_change, g.prior_2_volume-h.ln_volume as prior_2_volume_change,
                case when g.week_day="Mon" then 1 else 0 end mon,
                case when g.week_day="Tue" then 1 else 0 end tue,
                case when g.week_day="Wed" then 1 else 0 end wed,
                case when g.week_day="Thu" then 1 else 0 end thu,
                case when g.week_day="Fri" then 1 else 0 end fri,
                case when g.curr_low-g.prior_low>0 and g.prior_low-g.prior_2_low>0 and g.prior_2_low-h.ln_low_price>0 then 1 else 0 end uptrend,
                case when g.curr_low-g.prior_low<0 and g.prior_low-g.prior_2_low<0 and g.prior_2_low-h.ln_low_price<0 then 1 else 0 end downtrend
        from (
        select e.*, f.ln_open_price as prior_2_open, f.ln_high_price as prior_2_high, f.ln_low_price as prior_2_low, f.ln_close_price as prior_2_close, f.ln_volume as prior_2_volume
        from (
        select c.*, d.ln_open_price as prior_open, d.ln_high_price as prior_high, d.ln_low_price as prior_low, d.ln_close_price as prior_close, d.ln_volume as prior_volume  
        from (
        select 	a.trans_id, a.linux_date, a.week_day, a.ticker, b.ln_open_price as future_open, a.ln_open_price as curr_open, a.ln_high_price as curr_high,  
                a.ln_low_price as curr_low, a.ln_close_price as curr_close, a.ln_volume as curr_volume
        from STOCK_PRICES_1DAY as a 
        inner join STOCK_PRICES_1DAY as b on a.trans_id=b.trans_id-1 and a.ticker=b.ticker) as c
        inner join STOCK_PRICES_1DAY as d on c.trans_id=d.trans_id+1 and c.ticker=d.ticker) as e
        inner join STOCK_PRICES_1DAY as f on e.trans_id=f.trans_id+2 and e.ticker=f.ticker) as g
        inner join STOCK_PRICES_1DAY as h on g.trans_id=h.trans_id+3 and g.ticker=h.ticker where g.ticker=?) as stock_table
        inner join (
        select 	g.linux_date,
                g.curr_open-g.prior_open as curr_open_change, g.prior_open-g.prior_2_open as prior_open_change, g.prior_2_open-h.ln_open_price as prior_2_open_change,
                g.curr_high-g.prior_high as curr_high_change, g.prior_high-g.prior_2_high as prior_high_change, g.prior_2_high-h.ln_high_price as prior_2_high_change,
                g.curr_low-g.prior_low as curr_low_change, g.prior_low-g.prior_2_low as prior_low_change, g.prior_2_low-h.ln_low_price as prior_2_low_change,
                g.curr_close-g.prior_close as curr_close_change, g.prior_close-g.prior_2_close as prior_close_change, g.prior_2_close-h.ln_close_price as prior_2_close_change, 
                g.curr_volume-g.prior_volume as curr_volume_change, g.prior_volume-g.prior_2_volume as prior_volume_change, g.prior_2_volume-h.ln_volume as prior_2_volume_change,
                case when g.curr_low-g.prior_low>0 and g.prior_low-g.prior_2_low>0 and g.prior_2_low-h.ln_low_price>0 then 1 else 0 end uptrend,
                case when g.curr_low-g.prior_low<0 and g.prior_low-g.prior_2_low<0 and g.prior_2_low-h.ln_low_price<0 then 1 else 0 end downtrend
        from (
        select e.*, f.ln_open_price as prior_2_open, f.ln_high_price as prior_2_high, f.ln_low_price as prior_2_low, f.ln_close_price as prior_2_close, f.ln_volume as prior_2_volume
        from (
        select c.*, d.ln_open_price as prior_open, d.ln_high_price as prior_high, d.ln_low_price as prior_low, d.ln_close_price as prior_close, d.ln_volume as prior_volume  
        from (
        select 	a.trans_id, a.linux_date, a.week_day, a.ticker, b.ln_open_price as future_open, a.ln_open_price as curr_open, a.ln_high_price as curr_high,  
                a.ln_low_price as curr_low, a.ln_close_price as curr_close, a.ln_volume as curr_volume
        from INDEX_PRICES_1DAY as a 
        inner join INDEX_PRICES_1DAY as b on a.trans_id=b.trans_id-1 and a.ticker=b.ticker) as c
        inner join INDEX_PRICES_1DAY as d on c.trans_id=d.trans_id+1 and c.ticker=d.ticker) as e
        inner join INDEX_PRICES_1DAY as f on e.trans_id=f.trans_id+2 and e.ticker=f.ticker) as g
        inner join INDEX_PRICES_1DAY as h on g.trans_id=h.trans_id+3 and g.ticker=h.ticker where g.ticker=".DJI") as dow_table
        on stock_table.linux_date=dow_table.linux_date ) as comb_table
        inner join (
        select 	g.linux_date,
                g.curr_open-g.prior_open as curr_open_change, g.prior_open-g.prior_2_open as prior_open_change, g.prior_2_open-h.ln_open_price as prior_2_open_change,
                g.curr_high-g.prior_high as curr_high_change, g.prior_high-g.prior_2_high as prior_high_change, g.prior_2_high-h.ln_high_price as prior_2_high_change,
                g.curr_low-g.prior_low as curr_low_change, g.prior_low-g.prior_2_low as prior_low_change, g.prior_2_low-h.ln_low_price as prior_2_low_change,
                g.curr_close-g.prior_close as curr_close_change, g.prior_close-g.prior_2_close as prior_close_change, g.prior_2_close-h.ln_close_price as prior_2_close_change, 
                g.curr_volume-g.prior_volume as curr_volume_change, g.prior_volume-g.prior_2_volume as prior_volume_change, g.prior_2_volume-h.ln_volume as prior_2_volume_change,
                case when g.curr_low-g.prior_low>0 and g.prior_low-g.prior_2_low>0 and g.prior_2_low-h.ln_low_price>0 then 1 else 0 end uptrend,
                case when g.curr_low-g.prior_low<0 and g.prior_low-g.prior_2_low<0 and g.prior_2_low-h.ln_low_price<0 then 1 else 0 end downtrend
        from (
        select e.*, f.ln_open_price as prior_2_open, f.ln_high_price as prior_2_high, f.ln_low_price as prior_2_low, f.ln_close_price as prior_2_close, f.ln_volume as prior_2_volume
        from (
        select c.*, d.ln_open_price as prior_open, d.ln_high_price as prior_high, d.ln_low_price as prior_low, d.ln_close_price as prior_close, d.ln_volume as prior_volume  
        from (
        select 	a.trans_id, a.linux_date, a.week_day, a.ticker, b.ln_open_price as future_open, a.ln_open_price as curr_open, a.ln_high_price as curr_high,  
                a.ln_low_price as curr_low, a.ln_close_price as curr_close, a.ln_volume as curr_volume
        from INDEX_PRICES_1DAY as a 
        inner join INDEX_PRICES_1DAY as b on a.trans_id=b.trans_id-1 and a.ticker=b.ticker) as c
        inner join INDEX_PRICES_1DAY as d on c.trans_id=d.trans_id+1 and c.ticker=d.ticker) as e
        inner join INDEX_PRICES_1DAY as f on e.trans_id=f.trans_id+2 and e.ticker=f.ticker) as g
        inner join INDEX_PRICES_1DAY as h on g.trans_id=h.trans_id+3 and g.ticker=h.ticker where g.ticker=".INX") as sp_table
        on comb_table.linux_date=sp_table.linux_date;""",(ticker,))

# Save training datasets in CSV files
csv_output=open("/home/kirill/project/data/training_dataset.csv", "w")
csvwriter=csv.writer(csv_output, delimiter=",")
csvwriter.writerows(cursor)
    
# Close SQLite database     
sql_conn.close()

import matplotlib.pyplot as plt
import numpy as np

input_file=open("/home/kirill/project/data/stock_details.csv","r")
lines=input_file.readlines()

ticker=lines[0]
pattern_start_date=lines[1]
ticker_prices_string=lines[3][1:(len(lines[3])-2)].split(",")
ticker_index_string=lines[4][1:(len(lines[4])-2)].split(",")
match_prices_string=lines[5][1:(len(lines[5])-2)].split(",")
match_index_string=lines[6][1:(len(lines[6])-2)].split(",")

ticker_prices=[]
ticker_index=[]
match=[]
match_index=[]
for element in ticker_prices_string:
    ticker_prices.append(float(element))
for element in ticker_index_string:
    ticker_index.append(int(element))
for element in match_prices_string:
    match.append(float(element))
for element in match_index_string:
    match_index.append(int(element))

input_file2=open("/home/kirill/project/data/price_prediction.csv","r")
lines2=input_file2.readlines()

current_price=lines2[0]
predicted_price=lines2[1]

input_file.close()
input_file2.close()
    
print("The best pattern match is: "+str(ticker))
print("The pattern started on: "+str(pattern_start_date))
print("Current price of the stock is: "+str(current_price))
print("Predicted price of the stock in 1 day is: "+str(predicted_price))
    
plt.plot(ticker_index,ticker_prices, 'r', label=ticker)
plt.plot(match_index,match, 'b', label='pattern match')
plt.legend(bbox_to_anchor=(0., 1.02, 1., .102), loc=3,
           ncol=2, mode="expand", borderaxespad=0.)

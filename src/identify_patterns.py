import numpy as np
import time
import sys

# Calculate Euclidean distances
def euclidean_distances():
    distances = np.zeros((len(x), len(y)))
    for i in range(len(x)):
        for j in range(len(y)):
            distances[i,j] = (x[i]-y[j])**2  
    return distances

# Calculate accumulated cost matrix
def find_accumulated_cost(x,y):
    accumulated_cost = np.zeros((len(x), len(y)))
    accumulated_cost[0,0] = distances[0,0]
    for i in range(1, len(x)):
        accumulated_cost[i,0] = distances[i,0] + accumulated_cost[i-1,0]
    for i in range(1, len(y)):
        accumulated_cost[0,i] = distances[0,i] 
    for i in range(1, len(x)):
        for j in range(1, len(y)):
            accumulated_cost[i, j] = min(accumulated_cost[i-1, j-1], accumulated_cost[i-1, j], accumulated_cost[i, j-1]) + distances[i, j]
    return accumulated_cost

# Calculate optimal warping path and cost
def path_cost(x, y, accumulated_cost, distances):
    path = [[len(y)-1, len(x)-1]]
    cost = 0
    i = len(x)-1
    j = len(y)-1
    while i>0 or j>0:
        if i==0:
            j = j - 1
            break
        elif j==0:
            i = i - 1
        else:
            if accumulated_cost[i-1, j] == min(accumulated_cost[i-1, j-1], accumulated_cost[i-1, j], accumulated_cost[i, j-1]):
                i = i - 1
            elif accumulated_cost[i, j-1] == min(accumulated_cost[i-1, j-1], accumulated_cost[i-1, j], accumulated_cost[i, j-1]):
                j = j-1
            else:
                i = i - 1
                j= j- 1
        path.append([j, i])
    for [y, x] in path:
        cost = cost +distances[x, y]
    return path, cost

# Calculate subsequence endpooints
def subsequence(path):
    start_point=path[len(path)-1][0]
    end_point=path[0][0]
    return start_point, end_point

# Process time series file
def process_data():
    ticker_list=[]
    dates_list=[]
    dates=[]
    index_list=[]
    indices=[]
    prices_list=[]
    prices=[]
    time_series_list=[]
    time_series=[]

    ticker="A"
    input_file=open("/home/kirill/project/data/time_series.csv","r")
    for line in input_file:
        if ticker<>line.strip().split(",")[0]:
            ticker=line.strip().split(",")[0]
            ticker_list.append(ticker)
            dates_list.append(dates)
            index_list.append(indices)
            prices_list.append(prices)
            time_series_list.append(time_series)
            dates=[]
            indices=[]
            prices=[]
            time_series=[]
            dates.append(line.strip().split(",")[2])
            indices.append(line.strip().split(",")[1])
            prices.append(float(line.strip().split(",")[3]))
            time_series.append(float(line.strip().split(",")[6]))
        else:
            dates.append(line.strip().split(",")[2])
            indices.append(line.strip().split(",")[1])
            prices.append(float(line.strip().split(",")[3]))
            time_series.append(float(line.strip().split(",")[6]))
    input_file.close()
    return ticker_list, dates_list, index_list, prices_list, time_series_list

start=time.time()
# Select template
if sys.argv[1]=='1':
	# V Bottom
	x=np.array([1,0.75,0.5,0.25,0,0.25,0.5])
elif sys.argv[1]=='2':
	# Cat's Ears
	x=np.array([1,0.25,0.25,0.5,0.25,0.25,0.5,0.25,0])
else:
	# Cup w/ Handle
	x=np.array([1,0.5,0.3,0.15,0,0,0,0.1,0.2,0.5,1,0.85,0.7,0.6])

# Create data arrays
ticker_list, dates_list, index_list, prices_list, time_series_list=process_data()
final_cost=100
final_k=0
final_start_point=0
final_end_point=0
for k in range(len(time_series_list)):
    cost=0
    path=[]
    distances=[]
    accumulated_cost=[]
    
    y=np.array(time_series_list[k])
    distances=euclidean_distances()
    accumulated_cost=find_accumulated_cost(x,y)
    path, cost = path_cost(x, y, accumulated_cost, distances)
    start_point, end_point=subsequence(path)
    if final_cost>cost:
        final_cost=cost
        final_k=k
        final_start_point=start_point
        final_end_point=end_point

# Save output in CSV file
final_ticker=str(ticker_list[final_k])
start_date=dates_list[final_k][final_start_point]
end_date=dates_list[final_k][final_end_point]
price_chart_np=np.array(prices_list[final_k])
price_index_np=np.array(index_list[final_k])
match_np=np.array(price_chart_np[final_start_point:(final_end_point+1)])
match_index_np=np.array(price_index_np[final_start_point:(final_end_point+1)])

price_chart=[]
price_index=[]
for k in range(len(price_chart_np)):
	price_chart.append(price_chart_np[k])
	price_index.append(int(price_index_np[k]))

match=[]
match_index=[]
for k in range(len(match_np)):
	match.append(match_np[k])
	match_index.append(int(match_index_np[k]))

output_file=open("/home/kirill/project/data/stock_details.csv","w")
output_file.write(str(final_ticker)+"\n")
output_file.write(str(start_date)+"\n")
output_file.write(str(end_date)+"\n")
output_file.write(str(price_chart)+"\n")
output_file.write(str(price_index)+"\n")
output_file.write(str(match)+"\n")
output_file.write(str(match_index)+"\n")
output_file.close()

end=time.time()
print 'Running time to find best match is :'+str(end-start)+' seconds'

from pyspark.mllib.tree import RandomForest, RandomForestModel
from pyspark.mllib.regression import LabeledPoint
from pyspark import SparkContext
import sqlite3
from math import exp
import numpy
import csv

# Load and parse the data
def parsePoint(line):
    values = [float(x) for x in line.split(',')]
    return LabeledPoint(values[0], values[1:])

# Get ticker
input_file=open("/home/kirill/project/data/stock_details.csv","r")
k=0
for line in input_file:
	while k<>1:
        	ticker=line.strip()
        	k=1
input_file.close()

# Open SQLite database 
sql_conn=sqlite3.connect("/home/kirill/project/data/stocks.db")

# Get last price
cursor=sql_conn.execute("SELECT OPEN_PRICE FROM STOCK_PRICES_1DAY WHERE TICKER=? ORDER BY TRANS_ID DESC LIMIT 1",(ticker,))
last_price=[row[0] for row in cursor][0]

# Close SQLite database     
sql_conn.close()

# Get prediciton vector
vector=[]
vector_file=open("/home/kirill/project/data/prediction_vector.csv", "r")
line=vector_file.readline().split(",")
for element in line:
	vector.append(float(element))
vector_file.close()

# Start SparkContext
sc = SparkContext()

# Set path
training_file="/home/kirill/project/data/training_dataset.csv"
output_path="/home/kirill/project/data/models/"
#model_path="/home/kirill/project/data/models/regression_model_"+str(ticker).lower()

data = sc.textFile(training_file)
parsedData = data.map(parsePoint)
(trainingData, testData) = parsedData.randomSplit([0.7, 0.3])

# Train a RandomForest model.
# Empty categoricalFeaturesInfo indicates all features are continuous.
# Setting featureSubsetStrategy="auto" lets the algorithm choose.
model = RandomForest.trainRegressor(trainingData, categoricalFeaturesInfo={},
                                        numTrees=4, featureSubsetStrategy="auto",
                                        impurity='variance', maxDepth=4, maxBins=32,
                                        seed=42)

# Evaluate model on test instances and compute test error
predictions = model.predict(testData.map(lambda x: x.features))
labelsAndPredictions = testData.map(lambda lp: lp.label).zip(predictions)
testMSE = labelsAndPredictions.map(lambda (v, p): (v - p) * (v - p)).sum()/float(testData.count())

print("Test Mean Squared Error for "+str(ticker)+" = " + str(testMSE))
    

# Save model
model.save(sc, output_path)
    
# Predict the stock price
prediction=model.predict(vector)
prediction=round(exp(prediction)*last_price,2)       
predictions_output=open("/home/kirill/project/data/price_prediction.csv", "w")
predictions_output.write(str(last_price)+"\n")
predictions_output.write(str(prediction)+"\n")
predictions_output.close()
# Stop SparkContext
sc.stop()

#!/bin/bash

########################################################################
# Final Project - Identifying Patterns in Stock Price Movements        #
#                 and Predicting Future Price                          #
#                                                                      #
# Created by: Kirill Alshewski (ka2513)                                #
########################################################################
if [ "$1" = "--help" ] || [ "$1" = "--?" ]; then
  echo "This script runs application to identify a certain pattern in \
	the stocks comprising S&P500 index."
  exit
fi

exitcode=0

########################################################################
# Choose a pattern to find
########################################################################
title="Select a pattern to find"
prompt="Pick an option:"
options=("V Bottom" "Cat's Ears" "Cup w/ Handle" "Quit")

echo "$title"
PS3="$prompt "

select opt in "${options[@]}"
do
    case $opt in
        "V Bottom")
            echo "You selected V Bottom pattern"
	    PATTERN_OPTION=1
	    break
            ;;
        "Cat's Ears")
            echo "You selected Cat's Ears pattern"
	    PATTERN_OPTION=2
	    break
            ;;
        "Cup w/ Handle")
            echo "You selected Cup w/ Handle pattern"
	    PATTERN_OPTION=3
            break
	    ;;
        "Quit")
	    echo "Closing application..."
            exit
	    ;;
        *) echo invalid option;;
    esac
done

########################################################################
# Set paths for directories
########################################################################
setDirs()
{
	WORK_DIR=/home/kirill/project
	DATA_DIR=$WORK_DIR/data
	SRC_DIR=$WORK_DIR/src
	echo "Setting directories paths at ${WORK_DIR}"

########################################################################
# Create array of time series
########################################################################
timeSeriesArray()
{
	echo "Creating array of time series of stocks in S&P500 index"
	python $SRC_DIR/create_time_series.py 

	ret=$?
	if [ $ret -ne 0 ]; then
		errMsg="Error creating array of time series"
		exitcode=102
		abend
	fi
}

########################################################################
# Identify patterns in time series
########################################################################
identifyPatterns()
{
	echo "Searching for a pattern in time series of stocks in S&P500 index"
	python $SRC_DIR/identify_patterns.py $PATTERN_OPTION

	ret=$?
	if [ $ret -ne 0 ]; then
		errMsg="Error identifying pattern in time series"
		exitcode=103
		abend
	fi
}

########################################################################
# Create training dataset for Random Forests regression
########################################################################
createTrainingData()
{
	echo "Creating training dataset for Random Forests regression"
	python $SRC_DIR/create_training_dataset.py 

	ret=$?
	if [ $ret -ne 0 ]; then
		errMsg="Error creating training dataset"
		exitcode=104
		abend
	fi
}

########################################################################
# Get features vector for predictions
########################################################################
getPredictionVector()
{
	echo "Extracting stock data to make prediction on"
	python $SRC_DIR/create_prediciton_vector.py 

	ret=$?
	if [ $ret -ne 0 ]; then
		errMsg="Error extracting stock data to make prediction on"
		exitcode=105
		abend
	fi
}
########################################################################
# Run regression model and make predictions
########################################################################	
runRegression()
{
	echo "Running Random Forests regression model and making price predictions"
	cd $SPARK_HOME/bin/
	./spark-submit $SRC_DIR/random_forest_regression.py
	
	ret=$?
	if [ $ret -ne 0 ]; then
		errMsg="Error running regression model and making price predictions"
		exitcode=106
		abend
	else echo "Regression model ran successfully and stock price prediction is generated"
	fi
}

########################################################################
# Output final results
########################################################################	
outputResults()
{
	echo "Displaying final results..."
	python $SRC_DIR/output_final_results.py
	
	ret=$?
	if [ $ret -ne 0 ]; then
		errMsg="Error displaying final results"
		exitcode=107
		abend
	fi
}

########################################################################
# Abend details
########################################################################
abend()
{
echo ""
echo "================================================================="
echo "|                                                               |"
echo "|                    !!!Critical Script Abend!!!                |"
echo "|                                                               |"
echo "================================================================="
echo "Script abended on `date +%c`"
echo "The reason for failure is ${errMsg}"

finish
}

finish()
{
	echo "Identification of a pattern and future price prediction \
	      completed on `date +%c` with exit code ${exitcode}"
	exit ${exitcode}
} 

########################################################################
# Main processing logic for the script
########################################################################
echo "Starting execution of application on `date +%c`"
setDirs
timeSeriesArray
identifyPatterns
createTrainingData
getPredictionVector
runRegression
outputResults
finish
########################################################################
# End of the script
########################################################################

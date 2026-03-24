#!/bin/bash

# Setup Python environment
echo "Installing any necessary packages..."
pip install kafka-python pyspark > /dev/null 2>&1

.\bin\windows\zookeeper-server-start.bat .\config\zookeeper.properties

.\bin\windows\kafka-server-start.bat .\config\server.properties

# Stop prior instances if any
pkill -f log_gene.py
pkill -f spark.py

# Start Log Generator in the background
echo "Starting log generator..."
python log_gene.py > log_gene.out 2>&1 &
LOG_PID=$!
echo "Log generator started (PID: $LOG_PID)"

# Start Spark Processor in the background
echo "Starting Spark application..."
# Try to run with spark-submit if possible, fallback to python
if command -v spark-submit &> /dev/null
then
    spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 spark.py > spark.out 2>&1 &
else
    # Assuming pyspark package comes with required dependencies if running directly using python
    python spark.py > spark.out 2>&1 &
fi
SPARK_PID=$!
echo "Spark processor started (PID: $SPARK_PID)"

echo "Log streaming and processing setup is complete. Check log_gene.out and spark.out for logs."
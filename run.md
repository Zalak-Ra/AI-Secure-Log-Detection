# Comprehensive Run Guide: AI Secure Cloud Log Detection

This document provides a step-by-step master guide for running the entire pipeline locally on a Windows machine. Because this project is based on a real-time streaming architecture, you will need to open **multiple separate PowerShell terminals** and leave them running simultaneously.

---

## Technical Prerequisites
Make sure you have downloaded the following external dependencies.
* **Apache Kafka & ZooKeeper:** [Download (Scala 2.13 version)](https://kafka.apache.org/downloads) → extract to `C:\kafka`
* **Elasticsearch:** [Download Windows Zip](https://www.elastic.co/downloads/elasticsearch) → extract to `C:\elastic\elasticsearch` (Disable security in `config/elasticsearch.yml` by setting `xpack.security.enabled: false`)
* **Kibana:** [Download Windows Zip](https://www.elastic.co/downloads/kibana) → extract to `C:\elastic\kibana`
* **Logstash:** [Download Windows Zip](https://www.elastic.co/downloads/logstash) → extract to `C:\elastic\logstash`

---

## 🏗️ STEP 0 — Start Kafka (Required for BOTH Walkthroughs)

These terminals must always be running first, regardless of which path you choose below.

### Terminal #1: ZooKeeper
```powershell
cd C:\kafka
.\bin\windows\zookeeper-server-start.bat .\config\zookeeper.properties
```
*(Leave this running)*

### Terminal #2: Kafka Broker
```powershell
cd C:\kafka
.\bin\windows\kafka-server-start.bat .\config\server.properties
```
*(Leave this running)*

### Terminal #3: Create Kafka Topics *(One-time setup only — skip if done before)*
```powershell
cd C:\kafka

.\bin\windows\kafka-topics.bat --create --if-not-exists --topic infra.raw.metrics --bootstrap-server localhost:9092 --partitions 3 --replication-factor 1
.\bin\windows\kafka-topics.bat --create --if-not-exists --topic infra.feature.windows --bootstrap-server localhost:9092 --partitions 3 --replication-factor 1
.\bin\windows\kafka-topics.bat --create --if-not-exists --topic infra.predictions --bootstrap-server localhost:9092 --partitions 3 --replication-factor 1
.\bin\windows\kafka-topics.bat --create --if-not-exists --topic infra.dlq --bootstrap-server localhost:9092 --partitions 3 --replication-factor 1
```
*(You can close this terminal once all 4 topics are created)*

---

## 🛤️ WALKTHROUGH A — First Time Setup (Train the Model from Scratch)

Follow this walkthrough if you are running this project for the first time and do not have a trained model yet in `models\failure_predictor`.

### Terminal #4: Generate Data and Train the Model
Run these commands **one by one** in a single new terminal, waiting for each to fully finish before running the next.

```powershell
cd "C:\Users\Hp\Desktop\llm\big data\project_inter"
.\.venv311\Scripts\Activate.ps1

# Step 1 — Clear any old corrupted histories
Remove-Item -ErrorAction SilentlyContinue .\data\bootstrap_training_history.jsonl

# Step 2 — Generate a large offline dataset (100,000 events, with accelerated incidents for training variety)
python apps\generate_logs.py --no-kafka --fast --steps 100000 --host-count 12 --incident-rate-scale 6 --history-path .\data\bootstrap_training_history.jsonl

# Step 3 — Verify data integrity and inspect sequences before training
python apps\train_failure_predictor.py --raw-history .\data\bootstrap_training_history.jsonl --prepare-only

# Step 4 — Train the LSTM Neural Network and save the model
python apps\train_failure_predictor.py --raw-history .\data\bootstrap_training_history.jsonl --model-dir .\models\failure_predictor

# Step 5 — Export the training history into 30-second feature windows
python apps\export_feature_windows.py --raw-history .\data\bootstrap_training_history.jsonl --output-path .\data\bootstrap_feature_windows.jsonl

# Step 6 — Run the prediction service on those feature windows (offline mode)
python apps\serve_predictions.py --model-dir .\models\failure_predictor --feature-file .\data\bootstrap_feature_windows.jsonl --output-file .\data\predictions.jsonl
```
*(When all complete, your trained model is saved in `models\failure_predictor` and results are in `data\predictions.jsonl`)*

### Terminal #5: Start the Live Signal Generator
Once training is complete, start the live streaming in a new terminal.
```powershell
cd "C:\Users\Hp\Desktop\llm\big data\project_inter"
.\.venv311\Scripts\Activate.ps1

python apps\generate_logs.py --history-path .\data\raw_history.jsonl
```
*(Leave this running)*

### Terminal #6: Start the Live Prediction Service
Now start the AI prediction server listening to live Kafka data.
```powershell
cd "C:\Users\Hp\Desktop\llm\big data\project_inter"
.\.venv311\Scripts\Activate.ps1

python apps\serve_predictions.py --model-dir .\models\failure_predictor
```
*(Leave this running)*

> **⚡ REQUIRED — Spark Streaming:** You MUST run Apache Spark for live AI feature aggregation to work! (Java 8+ required). Run this in its own terminal:
> `spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 .\spark_jobs\feature_pipeline.py --bootstrap-servers localhost:9092`
> *(Note: The very first batch may take ~10 minutes to process if there is a massive backlog of raw logs).*

---

## 🚀 WALKTHROUGH B — Model Already Trained (Skip Training, Run Live Pipeline Directly)

Follow this walkthrough if `models\failure_predictor` already exists and you just want to boot up the live real-time pipeline.

### Terminal #4: Start the Live Signal Generator
```powershell
cd "C:\Users\Hp\Desktop\llm\big data\project_inter"
.\.venv311\Scripts\Activate.ps1

python apps\generate_logs.py --history-path .\data\raw_history.jsonl
```
*(Leave this running)*

### Terminal #5: Start the Live Prediction Service
```powershell
cd "C:\Users\Hp\Desktop\llm\big data\project_inter"
.\.venv311\Scripts\Activate.ps1

python apps\serve_predictions.py --model-dir .\models\failure_predictor
```
*(Leave this running)*

> **⚡ REQUIRED — Spark Streaming:** You MUST run Apache Spark for live AI feature aggregation to work! (Java 8+ required). Run this in its own terminal:
> `spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 .\spark_jobs\feature_pipeline.py --bootstrap-servers localhost:9092`

---

## 👀 Tracking Predictions Live (JSONL)

To verify the AI model is actually predicting failures in real-time, open a new terminal and stream the output file directly to your screen:
```powershell
cd "C:\Users\Hp\Desktop\llm\big data\project_inter"
Get-Content -Path .\data\predictions.jsonl -Wait -Tail 10
```
> **Offline Shortcut:** If Spark is extremely slow or stuck and you just want to blast the Kibana dashboard with predictions instantly, kill `serve_predictions.py` and run: `python apps\serve_predictions.py --model-dir .\models\failure_predictor --feature-file .\data\bootstrap_feature_windows.jsonl --output-file .\data\predictions.jsonl`

---

## 📊 STEP LAST — Visualize on ELK Stack (Kibana)

### Terminal: Elasticsearch (Database)
**CRITICAL:** Ensure `xpack.security.enabled: false` is set inside `C:\elasticsearch\config\elasticsearch.yml` so you don't get password/HTTPS errors!
```powershell
cd C:\elasticsearch
.\bin\elasticsearch.bat
```
*(Leave this running)*

### Terminal: Kibana (Dashboard Web UI)
**CRITICAL:** Ensure `C:\kibana\config\kibana.yml` has `elasticsearch.hosts: ["http://localhost:9200"]` and all security/password variables are commented out or deleted.
```powershell
cd C:\kibana
.\bin\kibana.bat
```
*(Leave this running)*

### Terminal: Apply Templates & Run Logstash
```powershell
cd "C:\Users\Hp\Desktop\llm\big data\project_inter"

# Install Index Templates (Will fail safely if ES Security wasn't disabled properly)
curl.exe -X PUT "http://localhost:9200/_index_template/infra_predictions" -H "Content-Type: application/json" --data-binary "@elastic/predictions-template.json"
curl.exe -X PUT "http://localhost:9200/_index_template/infra_feature_windows" -H "Content-Type: application/json" --data-binary "@elastic/feature-windows-template.json"

# Start Logstash to stream predictions into Elasticsearch
cd C:\logstash
.\bin\logstash.bat -f "C:\Users\Hp\Desktop\llm\big data\project_inter\logstash\predictions.conf"
```
*(Leave this running)*

**Finish Line:** Open your browser and navigate to `http://localhost:5601`. (Give it 5-10 minutes to finish optimizing if it says "ERR_CONNECTION_REFUSED"). Click on the **Discover** or **Dashboards** tab to see your AI predictions live!

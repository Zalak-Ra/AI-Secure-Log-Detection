# AI Secure Cloud Log Detection

Real-time infrastructure monitoring pipeline for synthetic cloud telemetry, Kafka streaming, Spark feature aggregation, LSTM-based failure prediction, and ELK visualization.

## What Changed

The project is no longer just a console Kafka demo plus offline anomaly notebook.

It now includes:

- A realistic synthetic telemetry generator with correlated metrics, host roles, diurnal variation, staged incidents, and hidden ground-truth labels.
- A Spark Structured Streaming job that converts raw Kafka telemetry into 30-second feature windows.
- A supervised horizon-based predictor pipeline that learns `failure within the next 5 minutes`, instead of only reconstructing anomalies.
- An online prediction service that turns feature windows into live failure-risk events.
- Logstash, Elasticsearch, and Kibana configuration for storing and visualizing proactive alerts.

## Architecture

```text
Telemetry Generator
  -> Kafka topic: infra.raw.metrics
  -> Spark Structured Streaming
  -> Kafka topic: infra.feature.windows
  -> Prediction Service
  -> Kafka topic: infra.predictions
  -> Logstash
  -> Elasticsearch data streams
  -> Kibana dashboards + alerts
```

Historical model training uses the same raw telemetry schema:

```text
Raw JSONL history
  -> 30-second feature windows
  -> 20-step sequences (10-minute context)
  -> LSTM classifier
  -> model artifact + manifest
```

## Repository Layout

```text
apps/
  generate_logs.py
  capture_history.py
  export_feature_windows.py
  train_failure_predictor.py
  serve_predictions.py
src/ai_secure_cloud_log_detection/
  config.py
  simulator.py
  features.py
  training.py
  inference.py
spark_jobs/
  feature_pipeline.py
logstash/
  predictions.conf
  feature_windows.conf
elastic/
  predictions-template.json
  feature-windows-template.json
kibana/
  README.md
```

## Data Contract

Each raw telemetry event contains:

- Identity: `machine_id`, `cluster_id`, `role`, `sequence_no`
- Metrics: `cpu_pct`, `mem_pct`, `gpu_util_pct`, `gpu_mem_pct`, `disk_used_pct`, `disk_io_wait_pct`, `net_rx_bytes`, `net_tx_bytes`, `net_retransmit_pct`, `request_rate`, `error_count`, `latency_p95_ms`, `queue_depth`, `restart_count`, `heartbeat_ok`
- Hidden synthetic labels for training: `gt_incident_id`, `gt_failure_type`, `gt_phase`, `gt_failure_start_ts`, `gt_time_to_failure_sec`, `gt_is_failure_now`

The model does not train on the hidden ground-truth fields directly. They are used only to build future-failure labels.

## Setup

Use Python `3.11` if you want the TensorFlow LSTM path. Python `3.14` can still run generation and feature engineering, but TensorFlow is not supported there.

### 1. Python environment

```powershell
py -3.11 -m venv .venv311
.\.venv311\Scripts\Activate.ps1
python -m pip install -r requirements.txt
```

### 2. Kafka and ZooKeeper

Linux/macOS:

```bash
bash server_setup.sh
```

Windows:

```powershell
.\bin\windows\zookeeper-server-start.bat .\config\zookeeper.properties
.\bin\windows\kafka-server-start.bat .\config\server.properties
.\bin\windows\kafka-topics.bat --create --if-not-exists --topic infra.raw.metrics --bootstrap-server localhost:9092 --partitions 3 --replication-factor 1
.\bin\windows\kafka-topics.bat --create --if-not-exists --topic infra.feature.windows --bootstrap-server localhost:9092 --partitions 3 --replication-factor 1
.\bin\windows\kafka-topics.bat --create --if-not-exists --topic infra.predictions --bootstrap-server localhost:9092 --partitions 3 --replication-factor 1
.\bin\windows\kafka-topics.bat --create --if-not-exists --topic infra.dlq --bootstrap-server localhost:9092 --partitions 3 --replication-factor 1
```

## Execution Order

### Phase 1. Generate raw telemetry

Stream to Kafka and store local history:

```powershell
.\.venv311\Scripts\python.exe .\log_gene.py --history-path .\data\raw_history.jsonl
```

For training bootstraps, increase incident density:

```powershell
.\.venv311\Scripts\python.exe .\log_gene.py --no-kafka --fast --steps 30000 --host-count 12 --incident-rate-scale 6 --history-path .\data\bootstrap_training_history.jsonl
```

### Phase 2. Capture raw Kafka history

```powershell
.\.venv311\Scripts\python.exe .\fetch_log.py --history-path .\data\raw_history.jsonl
```

### Phase 3. Run Spark feature aggregation

```powershell
spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.1 .\spark.py --bootstrap-servers localhost:9092
```

This writes live feature windows to Kafka topic `infra.feature.windows`.

### Phase 4. Train the predictor

Dataset preparation check:

```powershell
.\.venv311\Scripts\python.exe .\train_predictor.py --raw-history .\data\bootstrap_training_history.jsonl --prepare-only
```

Train the model:

```powershell
.\.venv311\Scripts\python.exe .\train_predictor.py --raw-history .\data\bootstrap_training_history.jsonl --model-dir .\models\failure_predictor
```

Preferred behavior:

- If TensorFlow is available, the script trains an LSTM classifier.
- If TensorFlow is unavailable, the code falls back to a sequence baseline so the pipeline can still be smoke-tested end to end.

### Phase 5. Run live prediction service

Kafka mode:

```powershell
.\.venv311\Scripts\python.exe .\serve_predictions.py --model-dir .\models\failure_predictor
```

Offline file mode:

```powershell
.\.venv311\Scripts\python.exe .\build_feature_windows.py --raw-history .\data\bootstrap_training_history.jsonl --output-path .\data\bootstrap_feature_windows.jsonl
.\.venv311\Scripts\python.exe .\serve_predictions.py --model-dir .\models\failure_predictor --feature-file .\data\bootstrap_feature_windows.jsonl --output-file .\data\predictions.jsonl
```

### Phase 6. Start Logstash and Elasticsearch

Predictions:

```powershell
logstash -f .\logstash\predictions.conf
```

Feature windows:

```powershell
logstash -f .\logstash\feature_windows.conf
```

Install index templates:

```powershell
curl -X PUT "http://localhost:9200/_index_template/infra_predictions" -H "Content-Type: application/json" --data-binary "@elastic/predictions-template.json"
curl -X PUT "http://localhost:9200/_index_template/infra_feature_windows" -H "Content-Type: application/json" --data-binary "@elastic/feature-windows-template.json"
```

### Phase 7. Build Kibana dashboards and alerts

Use the guidance in [kibana/README.md](./kibana/README.md).

## Modeling Notes

- Sequence length: `20` timesteps
- Bucket size: `30` seconds
- Lookback window: `10` minutes
- Prediction target: failure starts within the next `5` minutes

Why this is better than the earlier notebook:

- No train/test leakage from overlapping windows after a naive split
- No dependence on hand-made anomaly reconstruction thresholds alone
- Labels align to proactive prediction, not just “something weird is happening now”
- Missing telemetry is preserved as signal through `missing_ratio`

## Validated Locally

These steps were executed in the workspace:

- Generated synthetic raw history into `data/bootstrap_training_history.jsonl`
- Prepared training sequences successfully
- Trained a predictor artifact into `models/failure_predictor`
- Exported feature windows to `data/bootstrap_feature_windows.jsonl`
- Ran the prediction service in offline mode and emitted prediction events

## Current Practical Limits

- TensorFlow on native Windows is CPU-only here.
- Spark + Kafka + Elasticsearch were configured, but not launched inside this workspace session.
- Real production quality still depends on richer host fleets, longer histories, and stricter evaluation on future time ranges.

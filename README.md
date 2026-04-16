# AI Secure Cloud Log Detection

AI Secure Cloud Log Detection is a real-time infrastructure monitoring and failure prediction system built around a streaming architecture. Its purpose is to transform continuous cloud telemetry into structured time-series signals and predict failures before they happen, enabling proactive intervention instead of reactive debugging.

## System Overview

The platform is organized as a set of decoupled services connected through Kafka. Each service owns a specific responsibility in the pipeline, which improves modularity, fault isolation, and horizontal scalability.

At a high level, the system flows through five stages:

```text
Synthetic Telemetry
  -> Kafka Ingestion
  -> Feature Window Aggregation
  -> Sequence-Based Prediction
  -> Search, Storage, and Visualization
```

This design supports both live streaming workflows and historical model training from the same telemetry schema.

## End-to-End Workflow

### 1. Telemetry Simulation

The pipeline begins with synthetic telemetry generation. Instead of relying on production infrastructure data, the system creates realistic cloud-like events that represent machine behavior over time. These events include:

- infrastructure identity such as machine, role, and cluster
- utilization metrics such as CPU, memory, GPU, and disk
- traffic and service signals such as request rate, latency, and retransmits
- hidden failure state used for supervised learning

The simulator is designed to produce correlated behavior rather than random noise. Workload patterns, host roles, staged degradation, and incident progression are all embedded into the generated stream so the downstream model can learn meaningful pre-failure patterns.

### 2. Streaming Ingestion

Generated telemetry is published into Kafka, which acts as the transport layer between independent components. Kafka makes the pipeline event-driven and scalable by allowing producers and consumers to operate asynchronously.

Using Kafka introduces several benefits:

- continuous ingestion instead of batch-only processing
- loose coupling between services
- replayable event streams for recovery and retraining
- easier scaling of consumers and downstream analytics

### 3. Feature Window Construction

Raw events are not directly suitable for sequence modeling, so the next stage aggregates them into fixed-duration feature windows. A feature window is a compact summary of activity over a short interval such as 30 seconds.

Each window captures statistical behavior over that interval, including values such as:

- averages and peaks for resource metrics
- totals for event counts and throughput
- latency and queue measurements
- missing-data ratio as an operational signal

This step converts a raw telemetry stream into structured time-series rows that can be consumed consistently by machine learning models.

### 4. Sequence Modeling

Individual feature windows are then assembled into ordered sequences representing recent system history. For example, a 20-step sequence of 30-second windows captures roughly 10 minutes of context.

The machine learning model uses these sequences to learn temporal patterns that typically occur before failures. This is a sequence learning problem rather than a point-in-time classification problem, because the relationship between degradation and failure unfolds across time.

### 5. Horizon-Based Prediction

The prediction target is not whether the current state is abnormal, but whether a failure will begin within a future time horizon such as the next 5 minutes. This makes the system predictive rather than reactive.

The output of the model is a failure risk score, typically expressed as a probability of near-term failure. That score is transformed into alert-oriented fields such as:

- risk level
- alert state
- likely failure mode
- supporting operational metrics

These prediction events are published back into Kafka for downstream storage and observability.

## Core Concepts

### Streaming Architecture

The system is designed for continuous processing. Telemetry does not wait to accumulate in large batches; instead, it moves through the pipeline as it is created. This allows the platform to detect rising risk in near real time and keeps the monitoring loop responsive.

### Feature Windows

Feature windows provide the bridge between event streams and machine learning. They reduce noisy, high-frequency telemetry into stable summaries while preserving the temporal structure needed for forecasting.

### Sequence Learning

Because failures usually emerge through progression rather than a single spike, the model needs memory of recent behavior. Sequence learning captures trend, accumulation, momentum, and degradation patterns that simpler threshold rules often miss.

### Synthetic Supervision

Synthetic data enables controlled experimentation. The simulator embeds hidden incident structure so the training pipeline can create supervised labels without depending on scarce production incidents. This makes it possible to train and evaluate a predictive model even in early-stage environments.

### Decoupled Services

Each stage in the pipeline can evolve independently. Telemetry generation, stream processing, training, inference, storage, and visualization are separate concerns. This separation improves maintainability and makes the system more resilient to failures in any single component.

## Machine Learning Perspective

The machine learning pipeline converts time-series feature sequences into a probability of future failure. Rather than reconstructing anomalies after they appear, the model learns the signatures that tend to precede failure onset.

This framing has several advantages:

- it aligns directly with operational decision-making
- it supports early warning before service interruption
- it reduces dependence on hand-tuned anomaly thresholds
- it preserves temporal context instead of treating each event independently

The preferred model is an LSTM-style sequence classifier, although the broader architecture allows a simpler fallback model to be used when sequence deep learning is unavailable.

## Observability and Monitoring

Prediction outputs and feature windows are forwarded into the search and analytics layer through Logstash and stored in Elasticsearch. Kibana then provides dashboards and alerting workflows for operators.

This observability layer allows teams to:

- inspect current and historical risk patterns
- correlate predicted failures with infrastructure behavior
- monitor machine-level and cluster-level trends
- create proactive alerts based on rising failure probability

## Data Flow Summary

The full conceptual flow of the system is:

```text
Raw telemetry events
  -> feature windows
  -> time-series sequences
  -> failure prediction model
  -> real-time risk events
  -> storage and visualization
```

In practical terms, the platform continuously converts low-level operational signals into machine-learned forecasts about what is likely to fail next.

## Design Principles

The system is built around four main design principles:

- real-time processing for immediate operational awareness
- modularity through service separation and message queues
- scalability through streaming infrastructure and independent consumers
- predictive intelligence through horizon-based machine learning

Together, these principles shift infrastructure monitoring away from reactive troubleshooting and toward intelligent, forward-looking operations.

## Outcome

AI Secure Cloud Log Detection enables proactive infrastructure management by predicting failures before they occur. By combining synthetic telemetry, streaming data engineering, temporal feature construction, sequence modeling, and observability tooling, the system provides a practical foundation for early-warning monitoring in distributed environments.

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
.\.venv311\Scripts\python.exe .\apps\generate_logs.py --history-path .\data\raw_history.jsonl
```

For training bootstraps, increase incident density:

```powershell
.\.venv311\Scripts\python.exe .\apps\generate_logs.py --no-kafka --fast --steps 30000 --host-count 12 --incident-rate-scale 6 --history-path .\data\bootstrap_training_history.jsonl
```

### Phase 2. Capture raw Kafka history

```powershell
.\.venv311\Scripts\python.exe .\apps\capture_history.py --history-path .\data\raw_history.jsonl
```

### Phase 3. Run Spark feature aggregation

```powershell
spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.1 .\spark_jobs\feature_pipeline.py --bootstrap-servers localhost:9092
```

This writes live feature windows to Kafka topic `infra.feature.windows`.

### Phase 4. Train the predictor

Dataset preparation check:

```powershell
.\.venv311\Scripts\python.exe .\apps\train_failure_predictor.py --raw-history .\data\bootstrap_training_history.jsonl --prepare-only
```

Train the model:

```powershell
.\.venv311\Scripts\python.exe .\apps\train_failure_predictor.py --raw-history .\data\bootstrap_training_history.jsonl --model-dir .\models\failure_predictor
```

Preferred behavior:

- If TensorFlow is available, the script trains an LSTM classifier.
- If TensorFlow is unavailable, the code falls back to a sequence baseline so the pipeline can still be smoke-tested end to end.

### Phase 5. Run live prediction service

Kafka mode:

```powershell
.\.venv311\Scripts\python.exe .\apps\serve_predictions.py --model-dir .\models\failure_predictor
```

Offline file mode:

```powershell
.\.venv311\Scripts\python.exe .\apps\export_feature_windows.py --raw-history .\data\bootstrap_training_history.jsonl --output-path .\data\bootstrap_feature_windows.jsonl
.\.venv311\Scripts\python.exe .\apps\serve_predictions.py --model-dir .\models\failure_predictor --feature-file .\data\bootstrap_feature_windows.jsonl --output-file .\data\predictions.jsonl
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

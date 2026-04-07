# AI-Secure-Log-Detection

End-to-end Data Engineering and AIOps pipeline for simulating Alibaba-style cluster telemetry, streaming it through Kafka, persisting it to CSV, and detecting operational anomalies with an LSTM Autoencoder.

## Architecture

1. `log_gene.py` emits synthetic logs for `server_alibaba_01` through `server_alibaba_05` every 0.5 seconds.
2. Kafka ingests the stream on `localhost:9092` in topic `server_logs`.
3. `spark.py` uses PySpark Structured Streaming to parse and print the live Kafka JSON stream with a strict schema.
4. `fetch_log.py` harvests the same Kafka topic into `live_data.csv` for model training.
5. `model.ipynb` groups rows by machine, resamples to 1-minute windows, trains an LSTM Autoencoder, and flags high reconstruction error.

## Data Contract

Each Kafka message contains these fields in this order:

```text
timestamp,machine,cpu_usage,gpu_wrk_util,avg_mem,max_mem,avg_gpu_wrk_mem,max_gpu_wrk_mem,read,write,read_count,write_count
```

`timestamp` is a Unix epoch value in seconds. The notebook still accepts older `dd-mm-yy HH:MM:SS` rows so existing `live_data.csv` files can be reused safely.

## Simulated Failures

`log_gene.py` can inject the five target failure modes without adding extra schema fields:

- `memory_leak`: steady `avg_mem` and `max_mem` climb until a 200GB reboot reset.
- `cpu_runaway`: extreme CPU usage while network read/write metrics drop to zero.
- `gpu_crash`: GPU memory is saturated while GPU utilization drops to `0.0`.
- `network_ddos`: explosive network read/read_count growth with CPU and memory buffering pressure.
- `silent_death`: all metrics drop to exactly zero for a short burst.

## Local Setup

Use Python 3.10 or 3.11 for the best TensorFlow compatibility.

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -r requirements.txt
```

Start Zookeeper first, then Kafka, from your Kafka installation directory:

```powershell
.\bin\windows\zookeeper-server-start.bat .\config\zookeeper.properties
.\bin\windows\kafka-server-start.bat .\config\server.properties
```

Create the topic if it does not exist:

```powershell
.\bin\windows\kafka-topics.bat --create --topic server_logs --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1
```

Run the pipeline in separate terminals:

```powershell
python log_gene.py
python fetch_log.py
spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 spark.py
jupyter notebook model.ipynb
```

If your local Spark version is not `3.5.1`, change the Kafka package version to match your installed Spark version.

## Cloud Deployment Notes

Recommended free/student VM shape: Ubuntu, 4 vCPU, 16GB RAM.

1. Install Java, Python, Kafka, and Spark on the VM.
2. Open inbound TCP `9092` only if clients outside the VM need to reach Kafka.
3. If producing/consuming from outside the VM, configure Kafka `advertised.listeners` to the VM public DNS/IP instead of `localhost`.
4. Run Zookeeper, Kafka, `log_gene.py`, `fetch_log.py`, and `spark.py` in separate `tmux` panes or systemd services.
5. Keep `live_data.csv` on persistent disk and do not commit generated CSV data to Git.

## Safety Notes

- `model.ipynb` filters out timestamps before 2025 before resampling to avoid the historical Pandas RAM blow-up.
- `log_gene.py` clamps generated metrics with non-negative floors so Gaussian noise cannot produce impossible negative telemetry.
- Start Zookeeper and Kafka before running the Python producer or consumers, otherwise Kafka clients can raise `NoBrokersAvailable`.

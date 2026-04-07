import json
import random
import time
from kafka import KafkaProducer


KAFKA_TOPIC = "server_logs"
BOOTSTRAP_SERVERS = "localhost:9092"
MACHINE_IDS = [
    "server_alibaba_01",
    "server_alibaba_02",
    "server_alibaba_03",
    "server_alibaba_04",
    "server_alibaba_05",
]

MEM_LIMIT_GB = 200
GPU_MEM_LIMIT_GB = 16.0
ANOMALY_START_PROBABILITY = 0.01
POLL_INTERVAL_SECONDS = 0.5


def positive_gauss(mean, std_dev, floor=0.0):
    return max(floor, random.gauss(mean, std_dev))


def normal_io():
    return {
        "read": int(positive_gauss(2.611418e08, 6.745454e08, 10.0)),
        "write": int(positive_gauss(6.187359e07, 2.623019e08, 10.0)),
        "read_count": int(positive_gauss(1.124878e04, 2.735763e04, 10.0)),
        "write_count": int(positive_gauss(8.944866e03, 8.220602e04, 10.0)),
    }


def new_server_state():
    avg_mem = positive_gauss(5.542812e00, 1.062084e01, 0.5)
    max_mem = max(avg_mem, positive_gauss(1.069742e01, 5.770631e01, 1.0))
    return {
        "avg_mem": avg_mem,
        "max_mem": max_mem,
        "anomaly": None,
        "ticks_remaining": 0,
    }


server_state = {machine: new_server_state() for machine in MACHINE_IDS}


def create_producer():
    return KafkaProducer(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )


def maybe_start_anomaly(machine, state):
    if state["anomaly"] is not None:
        return

    if random.random() >= ANOMALY_START_PROBABILITY:
        return

    anomaly = random.choice(
        ["memory_leak", "cpu_runaway", "gpu_crash", "network_ddos", "silent_death"]
    )
    state["anomaly"] = anomaly
    state["ticks_remaining"] = {
        "memory_leak": 999_999,
        "cpu_runaway": random.randint(6, 12),
        "gpu_crash": random.randint(6, 12),
        "network_ddos": random.randint(6, 12),
        "silent_death": random.randint(2, 4),
    }[anomaly]
    print(f"{machine}: {anomaly} anomaly started")


def update_memory(state):
    if state["anomaly"] == "memory_leak":
        state["avg_mem"] += random.uniform(0.5, 2.0)
        state["max_mem"] += random.uniform(1.0, 4.0)
        if state["max_mem"] > MEM_LIMIT_GB:
            print("Memory leak hit 200GB limit; simulating reboot")
            replacement = new_server_state()
            state.update(replacement)
        return

    state["avg_mem"] = max(0.5, state["avg_mem"] + random.uniform(-0.5, 0.5))
    state["max_mem"] = max(
        state["avg_mem"],
        1.0,
        state["max_mem"] + random.uniform(-1.0, 1.0),
    )


def normal_metrics(state):
    io_metrics = normal_io()
    return {
        "cpu_usage": positive_gauss(2.589425e02, 5.674885e02, 0.0),
        "gpu_wrk_util": positive_gauss(1.596829e01, 2.902764e01, 0.0),
        "avg_mem": state["avg_mem"],
        "max_mem": state["max_mem"],
        "avg_gpu_wrk_mem": positive_gauss(2.060904e00, 6.057909e00, 0.0),
        "max_gpu_wrk_mem": positive_gauss(2.795080e00, 7.646590e00, 0.0),
        **io_metrics,
    }


def apply_anomaly(metrics, state):
    anomaly = state["anomaly"]
    if anomaly is None:
        return metrics

    if anomaly == "memory_leak":
        metrics["avg_mem"] = state["avg_mem"]
        metrics["max_mem"] = state["max_mem"]
    elif anomaly == "cpu_runaway":
        metrics["cpu_usage"] = positive_gauss(2.589425e02 * 9, 5.674885e02, 0.0)
        metrics["read"] = 0
        metrics["write"] = 0
        metrics["read_count"] = 0
        metrics["write_count"] = 0
    elif anomaly == "gpu_crash":
        metrics["gpu_wrk_util"] = 0.0
        metrics["avg_gpu_wrk_mem"] = GPU_MEM_LIMIT_GB
        metrics["max_gpu_wrk_mem"] = GPU_MEM_LIMIT_GB
    elif anomaly == "network_ddos":
        metrics["read"] = int(
            max(metrics["read"] * random.uniform(10, 25), 2.611418e08 * 10)
        )
        metrics["read_count"] = int(
            max(metrics["read_count"] * random.uniform(20, 50), 1.124878e04 * 30)
        )
        metrics["cpu_usage"] = max(
            metrics["cpu_usage"] * random.uniform(2.0, 4.0),
            2.589425e02 * 2,
        )
        metrics["avg_mem"] += random.uniform(1.0, 5.0)
        metrics["max_mem"] += random.uniform(2.0, 8.0)
    elif anomaly == "silent_death":
        for key in metrics:
            metrics[key] = 0.0
        metrics["read"] = 0
        metrics["write"] = 0
        metrics["read_count"] = 0
        metrics["write_count"] = 0

    state["ticks_remaining"] -= 1
    if anomaly != "memory_leak" and state["ticks_remaining"] <= 0:
        state["anomaly"] = None
        state["ticks_remaining"] = 0

    return metrics


def build_payload(machine):
    state = server_state[machine]
    maybe_start_anomaly(machine, state)
    update_memory(state)
    metrics = apply_anomaly(normal_metrics(state), state)

    return {
        "timestamp": round(time.time(), 3),
        "machine": machine,
        "cpu_usage": round(metrics["cpu_usage"], 2),
        "gpu_wrk_util": round(metrics["gpu_wrk_util"], 2),
        "avg_mem": round(metrics["avg_mem"], 2),
        "max_mem": round(metrics["max_mem"], 2),
        "avg_gpu_wrk_mem": round(metrics["avg_gpu_wrk_mem"], 2),
        "max_gpu_wrk_mem": round(metrics["max_gpu_wrk_mem"], 2),
        "read": int(metrics["read"]),
        "write": int(metrics["write"]),
        "read_count": int(metrics["read_count"]),
        "write_count": int(metrics["write_count"]),
    }


def main():
    producer = create_producer()
    print(f"Starting Alibaba cluster simulation for {len(MACHINE_IDS)} servers...")

    while True:
        for machine in MACHINE_IDS:
            logs = build_payload(machine)
            producer.send(KAFKA_TOPIC, logs)
            print(
                f"Sent {machine}: CPU {logs['cpu_usage']} | "
                f"Max Mem {logs['max_mem']} | GPU {logs['gpu_wrk_util']}"
            )

        producer.flush()
        time.sleep(POLL_INTERVAL_SECONDS)


if __name__ == "__main__":
    main()

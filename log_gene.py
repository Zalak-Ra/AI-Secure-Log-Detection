import random
import time
import json
import datetime
from kafka import KafkaProducer

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

avg_mem_val = max(0.5, random.gauss(5.542812e+00, 1.062084e+01))
max_mem_val = max(1.0, random.gauss(1.069742e+01, 5.770631e+01))

memory_leak = False
MEM_LIMIT = 200
machine_id = ["server_alibaba_01","server_alibaba_02","server_alibaba_03","server_alibaba_04","server_alibaba_05"] 

print(f"Starting advanced Alibaba cluster simulation for {machine_id}...")

while True:
    if random.random() < 0.99:
        cpu_val = abs(random.gauss(2.589425e+02, 5.674885e+02)) + 1.0
    else:
        cpu_val = random.randint(
            int(2.589425e+02 + 3 * 5.674885e+02),
            int(2.589425e+02 + 9 * 9.674885e+02)
        )

    # 2. GPU
    if random.random() < 0.99:
        gpu_val = abs(random.gauss(1.596829e+01, 2.902764e+01)) + 1.0
        avg_gpu_val = abs(random.gauss(2.060904e+00, 6.057909e+00)) + 1.0
        max_gpu_val = abs(random.gauss(2.795080e+00, 7.646590e+00)) + 1.0
    else:
        gpu_val = random.uniform(0.1, 1.5)
        if random.random() < 0.99:
            avg_gpu_val = abs(random.gauss(2.060904e+00, 6.057909e+00)) + 1.0
            max_gpu_val = abs(random.gauss(2.795080e+00, 7.646590e+00)) + 1.0
        else:
            avg_gpu_val = 16.0
            max_gpu_val = 16.0

    # 3. IO (Network)
    if random.random() < 0.99:
        re_val = abs(random.gauss(2.611418e+08, 6.745454e+08)) + 10.0
        wr_val = abs(random.gauss(6.187359e+07, 2.623019e+08)) + 10.0
        re_co_val = abs(random.gauss(1.124878e+04, 2.735763e+04)) + 10.0
        wr_co_val = abs(random.gauss(8.944866e+03, 8.220602e+04)) + 10.0
    else:
        re_val = abs(random.gauss(2.611418e+08 * 9, 6.745454e+08 * 9)) + 10.0
        wr_val = abs(random.gauss(6.187359e+07 * 9, 2.623019e+08 * 9)) + 10.0
        re_co_val = abs(random.gauss(1.124878e+04 * 9, 2.735763e+04 * 9)) + 10.0
        wr_co_val = abs(random.gauss(8.944866e+03 * 9, 8.220602e+04 * 9)) + 10.0

    # 4. Memory Leak Logic
    if not memory_leak and random.random() < 0.01:
        memory_leak = True
        print("MEMORY LEAK STARTED")

    if memory_leak:
        avg_mem_val += random.uniform(0.5, 2)
        max_mem_val += random.uniform(1, 4)
        if max_mem_val > MEM_LIMIT:
            memory_leak = False
            avg_mem_val = max(0.5, abs(random.gauss(5.542812e+00, 1.062084e+01)))
            max_mem_val = max(1.0, abs(random.gauss(1.069742e+01, 5.770631e+01)))
    else:
        avg_mem_val = max(0.5, avg_mem_val + random.uniform(-0.5, 0.5))
        max_mem_val = max(1.0, max_mem_val + random.uniform(-1, 1))


    # 5. The Final Payload
    logs = {
        "timestamp": datetime.datetime.now().strftime("%d-%m-%y %H:%M:%S"),    
        "machine": random.choice(machine_id),        
        "cpu_usage": round(cpu_val, 2),
        "gpu_wrk_util": round(gpu_val, 2),
        "avg_mem": round(avg_mem_val, 2),
        "max_mem": round(max_mem_val, 2),
        "avg_gpu_wrk_mem": round(avg_gpu_val, 2),
        "max_gpu_wrk_mem": round(max_gpu_val, 2),
        "read": int(re_val),
        "write": int(wr_val),
        "read_count": int(re_co_val),
        "write_count": int(wr_co_val)
    }
    
    producer.send("server_logs", logs)
    print(f"Sent: CPU {logs['cpu_usage']} | Max Mem {logs['max_mem']}")
    time.sleep(0.5)
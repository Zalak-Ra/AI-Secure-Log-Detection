from __future__ import annotations

import json
import math
import random
import time
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Iterable

from .config import (
    DEFAULT_CLUSTER_ID,
    DEFAULT_HOST_COUNT,
    DEFAULT_SEED,
    DEFAULT_TICK_SECONDS,
    FAILURE_TYPES,
    PHASE_TO_CODE,
    RAW_EVENT_COLUMNS,
)


ROLE_LIBRARY = (
    ("api", 42.0, 38.0, 1400.0, 85.0),
    ("web", 35.0, 34.0, 1800.0, 95.0),
    ("db", 48.0, 52.0, 900.0, 70.0),
    ("cache", 22.0, 61.0, 2200.0, 35.0),
    ("gpu", 54.0, 46.0, 500.0, 120.0),
)


def _clamp(value: float, low: float, high: float) -> float:
    return max(low, min(high, value))


def _positive_gauss(rng: random.Random, mean: float, std_dev: float, floor: float = 0.0) -> float:
    return max(floor, rng.gauss(mean, std_dev))


@dataclass
class HostProfile:
    machine_id: str
    role: str
    cpu_base: float
    mem_base: float
    request_base: float
    latency_base: float
    incident_rate_per_tick: float
    diurnal_amplitude: float
    noise_scale: float


@dataclass
class IncidentState:
    incident_id: str
    failure_type: str
    phase_names: list[str]
    phase_durations: list[int]
    failure_start: datetime
    phase_index: int = 0
    ticks_in_phase: int = 0
    restart_pending: bool = False

    @property
    def phase(self) -> str:
        return self.phase_names[self.phase_index]

    def advance(self) -> bool:
        self.ticks_in_phase += 1
        if self.ticks_in_phase < self.phase_durations[self.phase_index]:
            return False
        self.phase_index += 1
        self.ticks_in_phase = 0
        return self.phase_index >= len(self.phase_names)


@dataclass
class HostState:
    profile: HostProfile
    load_level: float
    mem_pressure: float
    disk_used_pct: float
    gpu_pressure: float
    sequence_no: int = 0
    incident: IncidentState | None = None
    incident_counter: int = 0


class TelemetrySimulator:
    """Synthetic telemetry generator with correlated metrics and staged failures."""

    def __init__(
        self,
        host_count: int = DEFAULT_HOST_COUNT,
        tick_seconds: int = DEFAULT_TICK_SECONDS,
        cluster_id: str = DEFAULT_CLUSTER_ID,
        seed: int = DEFAULT_SEED,
        incident_rate_scale: float = 1.0,
    ) -> None:
        self.host_count = host_count
        self.tick_seconds = tick_seconds
        self.cluster_id = cluster_id
        self.incident_rate_scale = max(0.1, incident_rate_scale)
        self.rng = random.Random(seed)
        self.hosts = self._build_hosts()

    def _build_hosts(self) -> list[HostState]:
        hosts: list[HostState] = []
        for index in range(self.host_count):
            role, cpu_base, mem_base, request_base, latency_base = ROLE_LIBRARY[index % len(ROLE_LIBRARY)]
            profile = HostProfile(
                machine_id=f"server_{role}_{index + 1:03d}",
                role=role,
                cpu_base=cpu_base + self.rng.uniform(-6.0, 6.0),
                mem_base=mem_base + self.rng.uniform(-8.0, 8.0),
                request_base=request_base * self.rng.uniform(0.8, 1.25),
                latency_base=latency_base * self.rng.uniform(0.85, 1.2),
                incident_rate_per_tick=self.rng.uniform(0.0006, 0.0014) * self.incident_rate_scale,
                diurnal_amplitude=self.rng.uniform(8.0, 16.0),
                noise_scale=self.rng.uniform(2.5, 7.0),
            )
            hosts.append(
                HostState(
                    profile=profile,
                    load_level=self.rng.uniform(0.3, 0.7),
                    mem_pressure=self.rng.uniform(0.3, 0.6),
                    disk_used_pct=self.rng.uniform(28.0, 64.0),
                    gpu_pressure=self.rng.uniform(0.1, 0.55),
                )
            )
        return hosts

    def iter_events(
        self,
        start_time: datetime | None = None,
        steps: int | None = None,
        sleep: bool = False,
    ) -> Iterable[dict[str, object]]:
        current_time = start_time or datetime.now(timezone.utc)
        produced = 0
        while steps is None or produced < steps:
            for host in self.hosts:
                event = self._build_event(host, current_time)
                produced += 1
                yield event
                if steps is not None and produced >= steps:
                    break
            current_time += timedelta(seconds=self.tick_seconds)
            if sleep:
                time.sleep(self.tick_seconds)

    def _build_event(self, host: HostState, current_time: datetime) -> dict[str, object]:
        self._maybe_start_incident(host, current_time)

        seconds_of_day = current_time.hour * 3600 + current_time.minute * 60 + current_time.second
        diurnal = math.sin((seconds_of_day / 86400.0) * 2.0 * math.pi)
        host.load_level = _clamp(
            0.82 * host.load_level
            + 0.18 * self.rng.uniform(0.18, 0.94)
            + (diurnal * host.profile.diurnal_amplitude / 100.0),
            0.05,
            1.35,
        )
        host.mem_pressure = _clamp(
            0.92 * host.mem_pressure
            + 0.08 * host.load_level
            + self.rng.uniform(-0.02, 0.03),
            0.08,
            1.5,
        )
        host.gpu_pressure = _clamp(
            0.86 * host.gpu_pressure
            + 0.14 * host.load_level
            + self.rng.uniform(-0.03, 0.03),
            0.02,
            1.4,
        )
        host.disk_used_pct = _clamp(host.disk_used_pct + self.rng.uniform(0.003, 0.018), 10.0, 97.0)

        phase = "healthy"
        failure_type = "none"
        incident_id: str | None = None
        failure_start_ts: str | None = None
        time_to_failure_sec: int | None = None
        is_failure_now = 0
        restart_count = 0
        incident_multiplier = 0.0
        if host.incident is not None:
            incident = host.incident
            phase = incident.phase
            failure_type = incident.failure_type
            incident_id = incident.incident_id
            failure_start_ts = incident.failure_start.isoformat()
            incident_multiplier = {
                "stress": 0.35,
                "degraded": 0.72,
                "incident": 1.0,
                "recovery": 0.45,
            }[phase]
            if phase == "incident":
                is_failure_now = 1
            if current_time < incident.failure_start:
                time_to_failure_sec = int((incident.failure_start - current_time).total_seconds())
            else:
                time_to_failure_sec = 0
            if phase == "recovery" and incident.restart_pending:
                restart_count = 1
                incident.restart_pending = False

        cpu_pct = host.profile.cpu_base + host.load_level * 48.0 + self.rng.gauss(0.0, host.profile.noise_scale)
        mem_pct = host.profile.mem_base + host.mem_pressure * 34.0 + self.rng.gauss(0.0, 2.8)
        gpu_util_pct = 0.0
        gpu_mem_pct = 0.0
        if host.profile.role == "gpu":
            gpu_util_pct = 18.0 + host.gpu_pressure * 65.0 + self.rng.gauss(0.0, 4.0)
            gpu_mem_pct = 14.0 + host.gpu_pressure * 58.0 + self.rng.gauss(0.0, 3.2)
        disk_io_wait_pct = 3.0 + host.load_level * 16.0 + self.rng.gauss(0.0, 1.6)
        request_rate = host.profile.request_base * (0.65 + host.load_level) + self.rng.gauss(0.0, 60.0)
        queue_depth = max(0.0, (cpu_pct - 72.0) * 0.65 + self.rng.gauss(6.0, 4.0))
        net_rx_bytes = max(0.0, request_rate * self.rng.uniform(620.0, 1200.0))
        net_tx_bytes = max(0.0, request_rate * self.rng.uniform(410.0, 920.0))
        net_retransmit_pct = _positive_gauss(self.rng, 0.3 + host.load_level * 0.8, 0.25, 0.0)
        latency_p95_ms = max(
            5.0,
            host.profile.latency_base
            + queue_depth * 2.1
            + disk_io_wait_pct * 1.8
            + net_retransmit_pct * 22.0
            + self.rng.gauss(0.0, 6.5),
        )
        error_count = int(max(0.0, self.rng.gauss(max(0.0, (latency_p95_ms - host.profile.latency_base) / 25.0), 1.8)))
        heartbeat_ok = 1

        if host.incident is not None:
            (
                cpu_pct,
                mem_pct,
                gpu_util_pct,
                gpu_mem_pct,
                disk_io_wait_pct,
                request_rate,
                queue_depth,
                net_rx_bytes,
                net_tx_bytes,
                net_retransmit_pct,
                latency_p95_ms,
                error_count,
                heartbeat_ok,
            ) = self._apply_incident(
                host,
                cpu_pct,
                mem_pct,
                gpu_util_pct,
                gpu_mem_pct,
                disk_io_wait_pct,
                request_rate,
                queue_depth,
                net_rx_bytes,
                net_tx_bytes,
                net_retransmit_pct,
                latency_p95_ms,
                error_count,
                heartbeat_ok,
                incident_multiplier,
            )

        event = {
            "event_time": current_time.isoformat(),
            "event_ts": int(current_time.timestamp()),
            "machine_id": host.profile.machine_id,
            "cluster_id": self.cluster_id,
            "role": host.profile.role,
            "sequence_no": host.sequence_no,
            "cpu_pct": round(_clamp(cpu_pct, 0.0, 100.0), 3),
            "mem_pct": round(_clamp(mem_pct, 0.0, 100.0), 3),
            "gpu_util_pct": round(_clamp(gpu_util_pct, 0.0, 100.0), 3),
            "gpu_mem_pct": round(_clamp(gpu_mem_pct, 0.0, 100.0), 3),
            "disk_used_pct": round(_clamp(host.disk_used_pct, 0.0, 100.0), 3),
            "disk_io_wait_pct": round(_clamp(disk_io_wait_pct, 0.0, 100.0), 3),
            "net_rx_bytes": int(max(0.0, net_rx_bytes)),
            "net_tx_bytes": int(max(0.0, net_tx_bytes)),
            "net_retransmit_pct": round(_clamp(net_retransmit_pct, 0.0, 100.0), 4),
            "request_rate": round(max(0.0, request_rate), 3),
            "error_count": int(max(0, error_count)),
            "latency_p95_ms": round(max(0.0, latency_p95_ms), 3),
            "queue_depth": round(max(0.0, queue_depth), 3),
            "restart_count": restart_count,
            "heartbeat_ok": int(heartbeat_ok),
            "gt_incident_id": incident_id,
            "gt_failure_type": failure_type,
            "gt_phase": phase,
            "gt_phase_code": PHASE_TO_CODE[phase],
            "gt_failure_start_ts": failure_start_ts,
            "gt_time_to_failure_sec": time_to_failure_sec,
            "gt_is_failure_now": is_failure_now,
        }

        host.sequence_no += 1
        if host.incident is not None:
            finished = host.incident.advance()
            if not finished and host.incident.phase == "recovery" and not host.incident.restart_pending:
                host.incident.restart_pending = True
            if finished:
                host.incident = None
                host.mem_pressure = min(host.mem_pressure, 0.6)
                host.gpu_pressure = min(host.gpu_pressure, 0.55)
                host.load_level = min(host.load_level, 0.75)

        return event

    def _maybe_start_incident(self, host: HostState, current_time: datetime) -> None:
        if host.incident is not None:
            return
        if self.rng.random() >= host.profile.incident_rate_per_tick:
            return

        failure_type = self.rng.choice(FAILURE_TYPES)
        stress_ticks = self.rng.randint(8, 22)
        degraded_ticks = self.rng.randint(8, 18)
        incident_ticks = self.rng.randint(4, 10)
        recovery_ticks = self.rng.randint(4, 12)
        failure_start = current_time + timedelta(seconds=self.tick_seconds * (stress_ticks + degraded_ticks))
        host.incident_counter += 1
        host.incident = IncidentState(
            incident_id=f"{host.profile.machine_id}-{host.incident_counter:05d}",
            failure_type=failure_type,
            phase_names=["stress", "degraded", "incident", "recovery"],
            phase_durations=[stress_ticks, degraded_ticks, incident_ticks, recovery_ticks],
            failure_start=failure_start,
        )

    def _apply_incident(
        self,
        host: HostState,
        cpu_pct: float,
        mem_pct: float,
        gpu_util_pct: float,
        gpu_mem_pct: float,
        disk_io_wait_pct: float,
        request_rate: float,
        queue_depth: float,
        net_rx_bytes: float,
        net_tx_bytes: float,
        net_retransmit_pct: float,
        latency_p95_ms: float,
        error_count: int,
        heartbeat_ok: int,
        incident_multiplier: float,
    ) -> tuple[float, float, float, float, float, float, float, float, float, float, float, int, int]:
        failure_type = host.incident.failure_type if host.incident is not None else "none"

        if failure_type == "memory_leak":
            host.mem_pressure = _clamp(host.mem_pressure + 0.035 + incident_multiplier * 0.05, 0.0, 1.7)
            mem_pct += 18.0 * incident_multiplier + self.rng.uniform(1.0, 4.0)
            queue_depth += 15.0 * incident_multiplier
            latency_p95_ms += 60.0 * incident_multiplier
            error_count += int(3 + 18 * incident_multiplier)
            if host.incident is not None and host.incident.phase == "incident":
                heartbeat_ok = 0 if self.rng.random() < 0.45 else 1
        elif failure_type == "cpu_saturation":
            cpu_pct += 26.0 * incident_multiplier
            request_rate *= 1.0 + 0.8 * incident_multiplier
            queue_depth += 22.0 * incident_multiplier
            latency_p95_ms += 80.0 * incident_multiplier
            error_count += int(4 + 22 * incident_multiplier)
        elif failure_type == "network_meltdown":
            net_retransmit_pct += 4.0 * incident_multiplier
            latency_p95_ms += 95.0 * incident_multiplier
            net_tx_bytes *= max(0.15, 1.0 - 0.55 * incident_multiplier)
            net_rx_bytes *= 1.0 + 0.45 * incident_multiplier
            queue_depth += 18.0 * incident_multiplier
            error_count += int(5 + 20 * incident_multiplier)
            if host.incident is not None and host.incident.phase == "incident":
                heartbeat_ok = 0 if self.rng.random() < 0.25 else heartbeat_ok
        elif failure_type == "disk_exhaustion":
            host.disk_used_pct = _clamp(host.disk_used_pct + 0.12 + incident_multiplier * 0.75, 0.0, 100.0)
            disk_io_wait_pct += 22.0 * incident_multiplier
            latency_p95_ms += 70.0 * incident_multiplier
            error_count += int(6 + 16 * incident_multiplier)
            request_rate *= max(0.35, 1.0 - 0.3 * incident_multiplier)
        elif failure_type == "gpu_oom":
            if host.profile.role == "gpu":
                gpu_mem_pct += 28.0 * incident_multiplier
                gpu_util_pct += 18.0 * incident_multiplier
                latency_p95_ms += 55.0 * incident_multiplier
                error_count += int(4 + 14 * incident_multiplier)
                if host.incident is not None and host.incident.phase == "incident":
                    gpu_util_pct *= 0.22
                    heartbeat_ok = 0 if self.rng.random() < 0.2 else heartbeat_ok
            else:
                cpu_pct += 8.0 * incident_multiplier
                latency_p95_ms += 18.0 * incident_multiplier
        elif failure_type == "app_crash":
            latency_p95_ms += 90.0 * incident_multiplier
            queue_depth += 16.0 * incident_multiplier
            error_count += int(10 + 26 * incident_multiplier)
            request_rate *= max(0.05, 1.0 - 0.55 * incident_multiplier)
            if host.incident is not None and host.incident.phase == "incident":
                heartbeat_ok = 0
                cpu_pct *= 0.32
                mem_pct *= 0.58
                net_tx_bytes *= 0.12

        return (
            cpu_pct,
            mem_pct,
            gpu_util_pct,
            gpu_mem_pct,
            disk_io_wait_pct,
            request_rate,
            queue_depth,
            net_rx_bytes,
            net_tx_bytes,
            net_retransmit_pct,
            latency_p95_ms,
            error_count,
            heartbeat_ok,
        )


def append_jsonl(events: Iterable[dict[str, object]], destination: Path) -> None:
    destination.parent.mkdir(parents=True, exist_ok=True)
    with destination.open("a", encoding="utf-8") as handle:
        for event in events:
            handle.write(json.dumps(event) + "\n")


def validate_event_payload(event: dict[str, object]) -> None:
    missing = [column for column in RAW_EVENT_COLUMNS if column not in event]
    if missing:
        raise ValueError(f"Event is missing required columns: {missing}")

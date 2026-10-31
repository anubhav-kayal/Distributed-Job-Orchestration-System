"""Fault injection against the Docker Compose stack."""

import random
import subprocess
import time
from dataclasses import dataclass


def compose(*args: str) -> str:
    return subprocess.run(
        ["docker", "compose", *args], check=True, capture_output=True, text=True
    ).stdout


def docker(*args: str) -> None:
    subprocess.run(["docker", *args], check=True, capture_output=True)


def containers(service: str) -> list[str]:
    return compose("ps", "-q", service).split()


@dataclass
class Faults:
    workers: int
    lease_ttl: float

    def kill_worker(self) -> str:
        """SIGKILL a worker mid-job: its lease expires and the monitor reclaims the job."""
        victim = random.choice(containers("worker"))
        docker("kill", victim)
        compose("up", "-d", "--no-recreate", "--scale", f"worker={self.workers}")
        return f"SIGKILL worker {victim[:12]}"

    def pause_worker(self) -> str:
        """Freeze a worker longer than its lease (like a GC pause or a frozen VM). Its job
        is reclaimed and re-run; when it wakes, its stale write must be fenced."""
        victim = random.choice(containers("worker"))
        seconds = self.lease_ttl + 2
        docker("pause", victim)
        time.sleep(seconds)
        docker("unpause", victim)
        return f"paused worker {victim[:12]} for {seconds:.0f}s"

    def restart_control_plane(self) -> str:
        service = random.choice(["monitor", "scheduler"])
        compose("restart", service)
        return f"restarted {service}"

    def kill_redis(self) -> str:
        """SIGKILL Redis, so nothing is saved: every queue entry and every lease is lost.
        Postgres is the source of truth, so the monitor must rebuild what matters."""
        docker("kill", containers("redis")[0])
        time.sleep(1)
        compose("up", "-d", "--wait", "redis")
        return "SIGKILLed Redis (queues and leases lost) and restarted it"

    def unpause_all(self) -> None:
        for c in containers("worker"):
            subprocess.run(["docker", "unpause", c], capture_output=True)


WEIGHTS = {"kill": 5, "pause": 3, "restart-control": 2, "redis-kill": 1}


def pick(enabled: list[str]) -> str:
    return random.choices(enabled, weights=[WEIGHTS[f] for f in enabled])[0]

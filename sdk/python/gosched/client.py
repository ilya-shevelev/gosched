"""GoSched Python client for interacting with the GoSched scheduler API."""

from __future__ import annotations

import json
import urllib.request
import urllib.error
from dataclasses import dataclass, field, asdict
from typing import Optional


@dataclass
class GPUResources:
    """GPU resource requirements."""
    count: int = 0
    gpu_type: str = ""
    min_memory_bytes: int = 0
    model: str = ""


@dataclass
class Resources:
    """Compute resource requirements."""
    cpu_millis: int = 0
    memory_bytes: int = 0
    gpu: Optional[GPUResources] = None
    storage_bytes: int = 0

    @staticmethod
    def cpu_cores(cores: float) -> int:
        """Convert CPU cores to millicores."""
        return int(cores * 1000)

    @staticmethod
    def memory_gib(gib: float) -> int:
        """Convert GiB to bytes."""
        return int(gib * 1024 * 1024 * 1024)


@dataclass
class Job:
    """A schedulable job."""
    id: str = ""
    name: str = ""
    tenant: str = ""
    queue: str = "default"
    priority: int = 500
    resources: Resources = field(default_factory=Resources)
    task_count: int = 1
    labels: dict[str, str] = field(default_factory=dict)
    node_selector: dict[str, str] = field(default_factory=dict)
    preemption_policy: str = "Never"


@dataclass
class ClusterStatus:
    """Cluster status information."""
    jobs: int = 0
    nodes: int = 0
    pending: int = 0


class GoSchedClient:
    """Client for the GoSched HTTP API.

    Example usage:
        client = GoSchedClient("http://localhost:8080")
        status = client.get_status()
        print(f"Nodes: {status.nodes}, Jobs: {status.jobs}")
    """

    def __init__(self, base_url: str = "http://localhost:8080", timeout: int = 30):
        """Initialize the GoSched client.

        Args:
            base_url: The base URL of the GoSched master HTTP API.
            timeout: Request timeout in seconds.
        """
        self.base_url = base_url.rstrip("/")
        self.timeout = timeout

    def get_status(self) -> ClusterStatus:
        """Get the current cluster status.

        Returns:
            ClusterStatus with job and node counts.
        """
        data = self._get("/api/v1/status")
        return ClusterStatus(
            jobs=data.get("jobs", 0),
            nodes=data.get("nodes", 0),
            pending=data.get("pending", 0),
        )

    def health_check(self) -> bool:
        """Check if the master server is healthy.

        Returns:
            True if the server responds with 200 on /healthz.
        """
        try:
            req = urllib.request.Request(f"{self.base_url}/healthz")
            with urllib.request.urlopen(req, timeout=self.timeout) as resp:
                return resp.status == 200
        except (urllib.error.URLError, urllib.error.HTTPError):
            return False

    def ready_check(self) -> bool:
        """Check if the master server is ready to accept work.

        Returns:
            True if the server responds with 200 on /readyz.
        """
        try:
            req = urllib.request.Request(f"{self.base_url}/readyz")
            with urllib.request.urlopen(req, timeout=self.timeout) as resp:
                return resp.status == 200
        except (urllib.error.URLError, urllib.error.HTTPError):
            return False

    def _get(self, path: str) -> dict:
        """Make a GET request and return parsed JSON.

        Args:
            path: API endpoint path.

        Returns:
            Parsed JSON response as a dictionary.

        Raises:
            ConnectionError: If the request fails.
        """
        url = f"{self.base_url}{path}"
        try:
            req = urllib.request.Request(url, headers={"Accept": "application/json"})
            with urllib.request.urlopen(req, timeout=self.timeout) as resp:
                return json.loads(resp.read().decode("utf-8"))
        except urllib.error.URLError as e:
            raise ConnectionError(f"Failed to connect to {url}: {e}") from e

    def _post(self, path: str, data: dict) -> dict:
        """Make a POST request with JSON body.

        Args:
            path: API endpoint path.
            data: Request body as a dictionary.

        Returns:
            Parsed JSON response as a dictionary.

        Raises:
            ConnectionError: If the request fails.
        """
        url = f"{self.base_url}{path}"
        body = json.dumps(data).encode("utf-8")
        try:
            req = urllib.request.Request(
                url, data=body,
                headers={
                    "Content-Type": "application/json",
                    "Accept": "application/json",
                },
            )
            with urllib.request.urlopen(req, timeout=self.timeout) as resp:
                return json.loads(resp.read().decode("utf-8"))
        except urllib.error.URLError as e:
            raise ConnectionError(f"Failed to connect to {url}: {e}") from e

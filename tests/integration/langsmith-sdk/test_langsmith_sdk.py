from __future__ import annotations

import os
import socket
import subprocess
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from uuid import uuid4

import pytest
import requests
from langsmith import Client
from opentelemetry.proto.collector.trace.v1.trace_service_pb2 import (
    ExportTraceServiceRequest,
)
from opentelemetry.proto.common.v1.common_pb2 import AnyValue, KeyValue
from opentelemetry.proto.resource.v1.resource_pb2 import Resource
from opentelemetry.proto.trace.v1.trace_pb2 import ResourceSpans, ScopeSpans, Span, Status


PROJECT_NAME = "demo"
TRACE_ID = bytes([0xAA]) * 16
ROOT_SPAN_ID = bytes([0x11]) * 8
CHILD_SPAN_ID = bytes([0x22]) * 8


def test_langsmith_sdk_lists_runs_from_kevindb() -> None:
    repo_root = Path(__file__).resolve().parents[3]
    mockgres_port = reserve_port()
    server_port = reserve_port()
    server_url = f"http://127.0.0.1:{server_port}"

    mockgres = start_process(
        [
            "mockgres",
            "--host",
            "127.0.0.1",
            "--port",
            str(mockgres_port),
        ],
        cwd=repo_root,
    )
    server: subprocess.Popen[str] | None = None

    try:
        wait_for_tcp("127.0.0.1", mockgres_port, "mockgres")

        env = os.environ.copy()
        env.update(
            {
                "KEVINDB_BIND_ADDR": f"127.0.0.1:{server_port}",
                "KEVINDB_POSTGRES_URL": (
                    f"postgresql://127.0.0.1:{mockgres_port}/postgres"
                ),
                "RUST_LOG": "warn",
            }
        )
        server = start_process(
            ["cargo", "run", "--quiet", "-p", "kevindb-server"],
            cwd=repo_root,
            env=env,
        )
        wait_for_readyz(server_url, server)

        ingest_sample_trace(server_url)

        client = Client(
            api_url=server_url,
            api_key="test-key",
            auto_batch_tracing=False,
        )
        llm_runs = list(
            client.list_runs(project_name=PROJECT_NAME, run_type="llm", limit=1)
        )
        assert [run.name for run in llm_runs] == ["llm.call"]
        assert llm_runs[0].parent_run_id is not None
        assert str(llm_runs[0].trace_id) == "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"

        root_runs = list(client.list_runs(project_name=PROJECT_NAME, is_root=True))
        assert [run.name for run in root_runs] == ["agent.run"]

        sdk_root_id = uuid4()
        sdk_child_id = uuid4()
        sdk_start = datetime(2026, 1, 1, 12, 0, tzinfo=timezone.utc)
        client.create_run(
            id=sdk_root_id,
            project_name=PROJECT_NAME,
            trace_id=sdk_root_id,
            name="sdk.agent",
            run_type="chain",
            inputs={"prompt": "hello"},
            start_time=sdk_start,
        )
        client.create_run(
            id=sdk_child_id,
            project_name=PROJECT_NAME,
            trace_id=sdk_root_id,
            parent_run_id=sdk_root_id,
            name="sdk.llm",
            run_type="llm",
            inputs={"messages": ["hello"]},
            start_time=sdk_start + timedelta(milliseconds=100),
        )
        client.update_run(
            sdk_child_id,
            trace_id=sdk_root_id,
            parent_run_id=sdk_root_id,
            outputs={"text": "world"},
            end_time=sdk_start + timedelta(milliseconds=900),
        )
        client.update_run(
            sdk_root_id,
            trace_id=sdk_root_id,
            outputs={"answer": "world"},
            end_time=sdk_start + timedelta(seconds=1),
        )

        sdk_runs = list(client.list_runs(project_name=PROJECT_NAME, trace_id=sdk_root_id))
        assert [run.name for run in sdk_runs] == ["sdk.agent", "sdk.llm"]
        assert str(sdk_runs[0].id) == str(sdk_root_id)
        assert str(sdk_runs[1].id) == str(sdk_child_id)
        assert sdk_runs[1].parent_run_id == sdk_root_id
        assert sdk_runs[0].end_time is not None
        assert sdk_runs[1].end_time is not None

        read_child = client.read_run(sdk_child_id)
        assert read_child.inputs == {"messages": ["hello"]}
        assert read_child.outputs == {"text": "world"}
        assert read_child.parent_run_id == sdk_root_id
        assert str(read_child.trace_id) == str(sdk_root_id)

        read_root = client.read_run(sdk_root_id)
        assert read_root.inputs == {"prompt": "hello"}
        assert read_root.outputs == {"answer": "world"}

        failed_run_id = uuid4()
        client.create_run(
            id=failed_run_id,
            project_name=PROJECT_NAME,
            name="sdk.tool",
            run_type="tool",
            inputs={"tool": "lookup"},
            start_time=sdk_start + timedelta(seconds=2),
        )
        client.update_run(
            failed_run_id,
            error="lookup failed",
            end_time=sdk_start + timedelta(seconds=3),
        )
        failed_runs = list(client.list_runs(project_name=PROJECT_NAME, error=True))
        assert [run.name for run in failed_runs] == ["sdk.tool"]
        assert client.read_run(failed_run_id).error == "lookup failed"

        query_response = requests.post(
            f"{server_url}/runs/query",
            json={"project_name": PROJECT_NAME, "trace": str(sdk_root_id), "limit": 10},
            timeout=5,
        )
        query_response.raise_for_status()
        query_body = query_response.json()
        assert query_body["cursors"] == {"next": None}
        assert [run["name"] for run in query_body["runs"]] == ["sdk.agent", "sdk.llm"]
        assert query_body["runs"][0]["inputs"] == {"prompt": "hello"}
        assert query_body["runs"][0]["outputs"] == {"answer": "world"}
        assert query_body["runs"][1]["inputs"] == {"messages": ["hello"]}
        assert query_body["runs"][1]["outputs"] == {"text": "world"}

        v1_run_id = uuid4()
        v1_trace_id = uuid4()
        v1_create_response = requests.post(
            f"{server_url}/v1/runs",
            json={
                "id": str(v1_run_id),
                "trace_id": str(v1_trace_id),
                "session_name": PROJECT_NAME,
                "name": "v1.agent",
                "run_type": "chain",
                "inputs": {"via": "v1"},
                "start_time": sdk_start.isoformat(),
            },
            timeout=5,
        )
        assert v1_create_response.status_code == 202
        v1_update_response = requests.patch(
            f"{server_url}/v1/runs/{v1_run_id}",
            json={
                "trace_id": str(v1_trace_id),
                "outputs": {"ok": True},
                "end_time": (sdk_start + timedelta(milliseconds=50)).isoformat(),
            },
            timeout=5,
        )
        assert v1_update_response.status_code == 200
        v1_read_response = requests.get(f"{server_url}/v1/runs/{v1_run_id}", timeout=5)
        v1_read_response.raise_for_status()
        assert v1_read_response.json()["inputs"] == {"via": "v1"}
        assert v1_read_response.json()["outputs"] == {"ok": True}
        v1_query_response = requests.post(
            f"{server_url}/v1/runs/query",
            json={"project_name": PROJECT_NAME, "trace": str(v1_trace_id)},
            timeout=5,
        )
        v1_query_response.raise_for_status()
        assert [run["name"] for run in v1_query_response.json()["runs"]] == [
            "v1.agent"
        ]

        missing_response = requests.get(f"{server_url}/runs/{uuid4()}", timeout=5)
        assert missing_response.status_code == 404
    finally:
        stop_process(server)
        stop_process(mockgres)


def ingest_sample_trace(server_url: str) -> None:
    request = ExportTraceServiceRequest(
        resource_spans=[
            ResourceSpans(
                resource=Resource(
                    attributes=[string_attr("service.name", "langsmith-sdk-test")]
                ),
                scope_spans=[
                    ScopeSpans(
                        spans=[
                            Span(
                                trace_id=TRACE_ID,
                                span_id=ROOT_SPAN_ID,
                                name="agent.run",
                                start_time_unix_nano=1_000_000_000,
                                end_time_unix_nano=2_000_000_000,
                                attributes=[
                                    string_attr("langsmith.run_type", "chain"),
                                ],
                                status=Status(code=Status.STATUS_CODE_OK),
                            ),
                            Span(
                                trace_id=TRACE_ID,
                                span_id=CHILD_SPAN_ID,
                                parent_span_id=ROOT_SPAN_ID,
                                name="llm.call",
                                start_time_unix_nano=1_100_000_000,
                                end_time_unix_nano=1_900_000_000,
                                attributes=[
                                    string_attr("langsmith.run_type", "llm"),
                                ],
                                status=Status(code=Status.STATUS_CODE_OK),
                            ),
                        ]
                    )
                ],
            )
        ]
    )

    response = requests.post(
        f"{server_url}/v1/projects/{PROJECT_NAME}/traces",
        data=request.SerializeToString(),
        headers={"content-type": "application/x-protobuf"},
        timeout=5,
    )
    response.raise_for_status()


def string_attr(key: str, value: str) -> KeyValue:
    return KeyValue(key=key, value=AnyValue(string_value=value))


def start_process(
    args: list[str],
    *,
    cwd: Path,
    env: dict[str, str] | None = None,
) -> subprocess.Popen[str]:
    return subprocess.Popen(
        args,
        cwd=cwd,
        env=env,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        text=True,
    )


def stop_process(process: subprocess.Popen[str] | None) -> None:
    if process is None or process.poll() is not None:
        return

    process.terminate()
    try:
        process.wait(timeout=5)
    except subprocess.TimeoutExpired:
        process.kill()
        process.wait(timeout=5)


def wait_for_tcp(host: str, port: int, name: str, timeout_seconds: float = 15) -> None:
    deadline = time.monotonic() + timeout_seconds
    last_error: OSError | None = None

    while time.monotonic() < deadline:
        try:
            with socket.create_connection((host, port), timeout=0.5):
                return
        except OSError as error:
            last_error = error
            time.sleep(0.1)

    pytest.fail(f"{name} did not accept TCP connections: {last_error}")


def wait_for_readyz(
    server_url: str,
    server: subprocess.Popen[str],
    timeout_seconds: float = 240,
) -> None:
    deadline = time.monotonic() + timeout_seconds
    last_error: Exception | str | None = None

    while time.monotonic() < deadline:
        if server.poll() is not None:
            pytest.fail(f"kevindb-server exited with code {server.returncode}")

        try:
            response = requests.get(f"{server_url}/readyz", timeout=0.5)
            if response.status_code == 200:
                return
            last_error = f"{response.status_code}: {response.text}"
        except requests.RequestException as error:
            last_error = error

        time.sleep(0.2)

    pytest.fail(f"kevindb-server did not become ready: {last_error}")


def reserve_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as listener:
        listener.bind(("127.0.0.1", 0))
        return int(listener.getsockname()[1])

from __future__ import annotations

import os
import socket
import subprocess
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from uuid import NAMESPACE_URL, UUID, uuid4, uuid5

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

        ingest_receipt = ingest_sample_trace(server_url)
        assert ingest_receipt["accepted_spans"] == 2
        assert ingest_receipt["flushed_segments"] == 1

        retry_receipt = ingest_sample_trace(server_url)
        assert retry_receipt["accepted_spans"] == 2
        assert retry_receipt["flushed_segments"] == 0
        assert retry_receipt["flushes"] == []

        client = Client(
            api_url=server_url,
            api_key="test-key",
            auto_batch_tracing=False,
        )
        otlp_root_id = generated_run_id(PROJECT_NAME, TRACE_ID, ROOT_SPAN_ID)
        otlp_child_id = generated_run_id(PROJECT_NAME, TRACE_ID, CHILD_SPAN_ID)
        llm_runs = list(
            client.list_runs(project_name=PROJECT_NAME, run_type="llm", limit=1)
        )
        assert [run.name for run in llm_runs] == ["llm.call"]
        assert llm_runs[0].id == otlp_child_id
        assert llm_runs[0].parent_run_id == otlp_root_id
        assert str(llm_runs[0].trace_id) == "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"
        otlp_child = client.read_run(otlp_child_id)
        assert otlp_child.name == "llm.call"
        assert otlp_child.parent_run_id == otlp_root_id
        assert otlp_child.run_type == "llm"

        root_runs = list(client.list_runs(project_name=PROJECT_NAME, is_root=True))
        assert [run.name for run in root_runs] == ["agent.run"]
        assert root_runs[0].id == otlp_root_id

        sdk_root_id = uuid4()
        sdk_child_id = uuid4()
        sdk_start = datetime(2026, 1, 1, 12, 0, tzinfo=timezone.utc)
        client.create_run(
            id=sdk_root_id,
            project_name=PROJECT_NAME,
            trace_id=sdk_root_id,
            name="sdk.agent",
            run_type="chain",
            inputs={"prompt": "hello root"},
            events=[{"name": "root-start"}],
            extra={
                "metadata": {
                    "thread_id": "thread-sdk",
                    "prompt_tokens": 2,
                    "completion_tokens": 1,
                    "total_tokens": 3,
                    "total_cost": 0.01,
                    "ls_model_name": "gpt-sdk",
                    "ls_provider": "test-provider",
                }
            },
            tags=["thread-sdk", "root"],
            start_time=sdk_start,
        )
        client.create_run(
            id=sdk_child_id,
            project_name=PROJECT_NAME,
            trace_id=sdk_root_id,
            parent_run_id=sdk_root_id,
            name="sdk.llm",
            run_type="llm",
            inputs={
                "messages": [
                    {"role": "user", "content": "invoice alpha request"}
                ]
            },
            tags=["thread-sdk", "llm"],
            start_time=sdk_start + timedelta(milliseconds=100),
        )
        client.update_run(
            sdk_child_id,
            trace_id=sdk_root_id,
            parent_run_id=sdk_root_id,
            outputs={"text": "invoice alpha approved"},
            events=[{"name": "token", "text": "invoice"}],
            extra={
                "metadata": {
                    "tier": "gold",
                    "thread_id": "thread-sdk",
                    "prompt_tokens": 7,
                    "completion_tokens": 5,
                    "total_tokens": 12,
                    "total_cost": 0.04,
                    "first_token_latency_nanos": 25_000_000,
                    "ls_model_name": "gpt-sdk",
                    "ls_provider": "test-provider",
                }
            },
            end_time=sdk_start + timedelta(milliseconds=900),
        )
        client.update_run(
            sdk_root_id,
            trace_id=sdk_root_id,
            outputs={"answer": "root answer world"},
            end_time=sdk_start + timedelta(seconds=1),
        )

        sdk_runs = list(client.list_runs(project_name=PROJECT_NAME, trace_id=sdk_root_id))
        sdk_runs_by_id = {run.id: run for run in sdk_runs}
        assert set(sdk_runs_by_id) == {sdk_root_id, sdk_child_id}
        assert sdk_runs_by_id[sdk_root_id].name == "sdk.agent"
        assert sdk_runs_by_id[sdk_child_id].name == "sdk.llm"
        assert sdk_runs_by_id[sdk_child_id].parent_run_id == sdk_root_id
        assert sdk_runs_by_id[sdk_root_id].end_time is not None
        assert sdk_runs_by_id[sdk_child_id].end_time is not None

        read_child = client.read_run(sdk_child_id)
        assert read_child.inputs == {
            "messages": [{"role": "user", "content": "invoice alpha request"}]
        }
        assert read_child.outputs == {"text": "invoice alpha approved"}
        assert read_child.parent_run_id == sdk_root_id
        assert str(read_child.trace_id) == str(sdk_root_id)
        assert read_child.events == [{"name": "token", "text": "invoice"}]
        assert read_child.tags == ["thread-sdk", "llm"]

        read_root = client.read_run(sdk_root_id)
        assert read_root.inputs == {"prompt": "hello root"}
        assert read_root.outputs == {"answer": "root answer world"}
        assert read_root.events == [{"name": "root-start"}]
        assert read_root.tags == ["thread-sdk", "root"]

        feedback = client.create_feedback(
            sdk_child_id,
            key="quality",
            score=1.0,
            comment="looks good",
        )
        assert feedback.run_id == sdk_child_id
        listed_feedback = list(
            client.list_feedback(
                run_ids=[sdk_child_id],
                feedback_key=["quality"],
                limit=1,
            )
        )
        assert len(listed_feedback) == 1
        assert listed_feedback[0].run_id == sdk_child_id
        assert listed_feedback[0].key == "quality"
        assert listed_feedback[0].score == 1.0
        assert listed_feedback[0].comment == "looks good"
        client.update_feedback(
            feedback.id,
            score=0.5,
            value="edited",
            correction={"text": "world!"},
            comment="edited",
        )
        updated_feedback = client.read_feedback(feedback.id)
        assert updated_feedback.score == 0.5
        assert updated_feedback.value == "edited"
        assert updated_feedback.correction == {"text": "world!"}
        assert updated_feedback.comment == "edited"
        feedback_response = requests.get(
            f"{server_url}/feedback/{feedback.id}",
            timeout=5,
        )
        feedback_response.raise_for_status()
        feedback_body = feedback_response.json()
        assert feedback_body["id"] == str(feedback.id)
        assert feedback_body["score"] == 0.5
        run_feedback_response = requests.get(
            f"{server_url}/runs/{sdk_child_id}/feedback",
            timeout=5,
        )
        run_feedback_response.raise_for_status()
        assert [item["key"] for item in run_feedback_response.json()] == ["quality"]
        otlp_feedback = client.create_feedback(
            otlp_child_id,
            key="otlp_quality",
            score=0.75,
            comment="generated id feedback",
        )
        assert otlp_feedback.run_id == otlp_child_id
        indexed_feedback_response = requests.get(
            f"{server_url}/feedback",
            params={
                "project_name": PROJECT_NAME,
                "trace_id": str(sdk_root_id),
                "score_min": "0.5",
                "score_max": "0.5",
                "value": "edited",
            },
            timeout=5,
        )
        indexed_feedback_response.raise_for_status()
        indexed_feedback = indexed_feedback_response.json()
        assert [item["id"] for item in indexed_feedback] == [str(feedback.id)]

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

        metadata_filtered_runs = list(
            client.list_runs(
                project_name=PROJECT_NAME,
                filter='and(eq(metadata_key, "tier"), eq(metadata_value, "gold"))',
            )
        )
        assert [run.name for run in metadata_filtered_runs] == ["sdk.llm"]
        feedback_filtered_runs = list(
            client.list_runs(
                project_name=PROJECT_NAME,
                filter='and(eq(feedback_key, "quality"), eq(feedback_score, 0.5))',
            )
        )
        assert [run.name for run in feedback_filtered_runs] == ["sdk.llm"]

        unsupported_query_response = requests.post(
            f"{server_url}/runs/query",
            json={"project_name": PROJECT_NAME, "query": "invoice"},
            timeout=5,
        )
        assert unsupported_query_response.status_code == 400
        assert "query is not supported" in unsupported_query_response.text
        unsupported_attachment_response = requests.post(
            f"{server_url}/runs",
            json={
                "id": str(uuid4()),
                "project_name": PROJECT_NAME,
                "name": "sdk.attachment",
                "run_type": "chain",
                "inputs": {},
                "attachments": {"log": {"mime_type": "text/plain", "data": "abc"}},
            },
            timeout=5,
        )
        assert unsupported_attachment_response.status_code == 400
        assert "attachments are not supported" in unsupported_attachment_response.text

        scoped_search_response = requests.post(
            f"{server_url}/runs/query",
            json={
                "project_name": PROJECT_NAME,
                "filter": {
                    "operator": "search",
                    "field": "inputs",
                    "query": "invoice",
                },
                "select": ["id", "name"],
                "limit": 1,
                "debug": True,
            },
            timeout=5,
        )
        scoped_search_response.raise_for_status()
        scoped_search_body = scoped_search_response.json()
        assert [run["name"] for run in scoped_search_body["runs"]] == ["sdk.llm"]
        scoped_search_diagnostics = scoped_search_body["diagnostics"]
        assert scoped_search_diagnostics["candidate_runs"] == 1
        assert scoped_search_diagnostics["actual_object_store_requests"] > 0

        exact_payload_response = requests.post(
            f"{server_url}/runs/query",
            json={
                "project_name": PROJECT_NAME,
                "filter": {
                    "operator": "eq",
                    "field": "outputs",
                    "value": "invoice alpha approved",
                },
                "select": ["id", "name"],
            },
            timeout=5,
        )
        exact_payload_response.raise_for_status()
        assert [run["name"] for run in exact_payload_response.json()["runs"]] == [
            "sdk.llm"
        ]
        token_only_exact_response = requests.post(
            f"{server_url}/runs/query",
            json={
                "project_name": PROJECT_NAME,
                "filter": {
                    "operator": "eq",
                    "field": "outputs",
                    "value": "invoice",
                },
                "select": ["id", "name"],
            },
            timeout=5,
        )
        token_only_exact_response.raise_for_status()
        assert token_only_exact_response.json()["runs"] == []

        in_payload_response = requests.post(
            f"{server_url}/runs/query",
            json={
                "project_name": PROJECT_NAME,
                "filter": {
                    "operator": "in",
                    "field": "outputs",
                    "values": ["missing", "invoice alpha approved"],
                },
                "select": ["id", "name"],
            },
            timeout=5,
        )
        in_payload_response.raise_for_status()
        assert [run["name"] for run in in_payload_response.json()["runs"]] == [
            "sdk.llm"
        ]

        json_key_response = requests.post(
            f"{server_url}/runs/query",
            json={
                "project_name": PROJECT_NAME,
                "filter": {
                    "operator": "json_key",
                    "field": "outputs",
                    "path": "text",
                },
                "select": ["id", "name"],
            },
            timeout=5,
        )
        json_key_response.raise_for_status()
        assert [run["name"] for run in json_key_response.json()["runs"]] == [
            "sdk.llm"
        ]

        scoped_json_key_search_response = requests.post(
            f"{server_url}/runs/query",
            json={
                "project_name": PROJECT_NAME,
                "filter": {
                    "operator": "json_key_search",
                    "scope": "extra",
                    "path": "metadata.thread_id",
                    "query": "thread-sdk",
                },
                "select": ["id", "name"],
            },
            timeout=5,
        )
        scoped_json_key_search_response.raise_for_status()
        assert {
            run["name"] for run in scoped_json_key_search_response.json()["runs"]
        } == {"sdk.agent", "sdk.llm"}

        phase6_aggregate_response = requests.post(
            f"{server_url}/runs/aggregate",
            json={
                "project_name": PROJECT_NAME,
                "group_by": ["run_type"],
                "filter": {
                    "operator": "search",
                    "field": "inputs",
                    "query": "invoice",
                },
                "feedback_key": ["quality"],
                "debug": True,
            },
            timeout=5,
        )
        phase6_aggregate_response.raise_for_status()
        phase6_aggregate = phase6_aggregate_response.json()
        assert phase6_aggregate["source"] == "vortex"
        assert len(phase6_aggregate["rows"]) == 1
        llm_aggregate = phase6_aggregate["rows"][0]
        assert llm_aggregate["group"] == {"run_type": "llm"}
        assert llm_aggregate["metrics"]["count"] == 1
        quality_stats = llm_aggregate["metrics"]["feedback_scores"]["quality"]
        assert quality_stats["count"] == 1
        assert quality_stats["avg"] == 0.5
        assert phase6_aggregate["diagnostics"]["candidate_runs"] == 1
        assert phase6_aggregate["diagnostics"]["actual_object_store_requests"] > 0

        generated_feedback_aggregate_response = requests.post(
            f"{server_url}/v1/runs/aggregate",
            json={
                "project_name": PROJECT_NAME,
                "group_by": ["feedback_key"],
                "feedback_key": ["otlp_quality"],
                "debug": True,
            },
            timeout=5,
        )
        generated_feedback_aggregate_response.raise_for_status()
        generated_feedback_aggregate = generated_feedback_aggregate_response.json()
        assert generated_feedback_aggregate["source"] == "vortex"
        assert len(generated_feedback_aggregate["rows"]) == 1
        generated_feedback_row = generated_feedback_aggregate["rows"][0]
        assert generated_feedback_row["group"] == {"feedback_key": "otlp_quality"}
        assert generated_feedback_row["metrics"]["count"] == 1
        otlp_quality_stats = generated_feedback_row["metrics"]["feedback_scores"][
            "otlp_quality"
        ]
        assert otlp_quality_stats["count"] == 1
        assert otlp_quality_stats["avg"] == 0.75

        query_response = requests.post(
            f"{server_url}/runs/query",
            json={"project_name": PROJECT_NAME, "trace": str(sdk_root_id), "limit": 10},
            timeout=5,
        )
        query_response.raise_for_status()
        query_body = query_response.json()
        assert query_body["cursors"] == {"next": None}
        query_runs_by_id = {run["id"]: run for run in query_body["runs"]}
        assert set(query_runs_by_id) == {str(sdk_root_id), str(sdk_child_id)}
        assert query_runs_by_id[str(sdk_root_id)]["inputs"] == {"prompt": "hello root"}
        assert query_runs_by_id[str(sdk_root_id)]["outputs"] == {
            "answer": "root answer world"
        }
        assert query_runs_by_id[str(sdk_child_id)]["inputs"] == {
            "messages": [{"role": "user", "content": "invoice alpha request"}]
        }
        assert query_runs_by_id[str(sdk_child_id)]["outputs"] == {
            "text": "invoice alpha approved"
        }
        assert query_runs_by_id[str(sdk_root_id)]["child_run_ids"] == [str(sdk_child_id)]

        trace_response = requests.get(
            f"{server_url}/v1/projects/{PROJECT_NAME}/traces/{sdk_root_id}",
            timeout=5,
        )
        trace_response.raise_for_status()
        trace_body = trace_response.json()
        assert trace_body["root_run_ids"] == [str(sdk_root_id)]
        assert [run["name"] for run in trace_body["runs"]] == ["sdk.agent", "sdk.llm"]

        tree_filter_response = requests.post(
            f"{server_url}/runs/query",
            json={
                "project_name": PROJECT_NAME,
                "trace": str(sdk_root_id),
                "tree_filter": {
                    "field": "run_type",
                    "operator": "eq",
                    "value": "llm",
                },
                "select": ["id", "name"],
            },
            timeout=5,
        )
        tree_filter_response.raise_for_status()
        assert {run["name"] for run in tree_filter_response.json()["runs"]} == {
            "sdk.agent",
            "sdk.llm",
        }

        sessions_response = requests.get(
            f"{server_url}/sessions",
            params={"name": PROJECT_NAME},
            timeout=5,
        )
        sessions_response.raise_for_status()
        sessions = sessions_response.json()
        assert [session["name"] for session in sessions] == [PROJECT_NAME]
        project_id = sessions[0]["id"]

        threads_response = requests.post(
            f"{server_url}/v2/threads/query",
            json={"project_id": project_id, "page_size": 10},
            timeout=5,
        )
        threads_response.raise_for_status()
        threads = threads_response.json()
        sdk_threads = [
            thread for thread in threads["items"] if thread["thread_id"] == "thread-sdk"
        ]
        assert len(sdk_threads) == 1
        sdk_thread = sdk_threads[0]
        assert sdk_thread["count"] == 1
        assert sdk_thread["total_tokens"] == 15
        assert sdk_thread["first_inputs"] == "hello root"
        assert sdk_thread["last_outputs"] == "invoice alpha approved"

        thread_traces_response = requests.get(
            f"{server_url}/v2/threads/thread-sdk/traces",
            params=[
                ("project_id", project_id),
                ("page_size", "1"),
                ("selects", "TRACE_ID"),
                ("selects", "THREAD_ID"),
                ("selects", "INPUTS_PREVIEW"),
                ("selects", "OUTPUTS_PREVIEW"),
                ("selects", "TOTAL_TOKENS"),
            ],
            timeout=5,
        )
        thread_traces_response.raise_for_status()
        thread_traces = thread_traces_response.json()
        assert thread_traces.get("next_cursor") is None
        assert len(thread_traces["items"]) == 1
        thread_trace = thread_traces["items"][0]
        assert thread_trace["trace_id"] == str(sdk_root_id)
        assert thread_trace["thread_id"] == "thread-sdk"
        assert thread_trace["inputs_preview"] == "hello root"
        assert thread_trace["outputs_preview"] == "invoice alpha approved"
        assert thread_trace["total_tokens"] == 15

        filtered_response = requests.post(
            f"{server_url}/runs/query",
            json={
                "project_name": PROJECT_NAME,
                "parent_run_id": str(sdk_root_id),
                "start_time_gte": (
                    sdk_start + timedelta(milliseconds=50)
                ).isoformat(),
            },
            timeout=5,
        )
        filtered_response.raise_for_status()
        assert [run["name"] for run in filtered_response.json()["runs"]] == ["sdk.llm"]

        first_page_response = requests.post(
            f"{server_url}/runs/query",
            json={"project_name": PROJECT_NAME, "trace": str(sdk_root_id), "limit": 1},
            timeout=5,
        )
        first_page_response.raise_for_status()
        first_page = first_page_response.json()
        assert len(first_page["runs"]) == 1
        assert first_page["cursors"] == {"next": "1"}
        second_page_response = requests.post(
            f"{server_url}/runs/query",
            json={
                "project_name": PROJECT_NAME,
                "trace": str(sdk_root_id),
                "limit": 1,
                "cursor": first_page["cursors"]["next"],
            },
            timeout=5,
        )
        second_page_response.raise_for_status()
        second_page = second_page_response.json()
        assert len(second_page["runs"]) == 1
        assert {run["name"] for run in first_page["runs"] + second_page["runs"]} == {
            "sdk.agent",
            "sdk.llm",
        }
        assert second_page["cursors"] == {"next": None}

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


def ingest_sample_trace(server_url: str) -> dict[str, object]:
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
    return response.json()


def generated_run_id(project_name: str, trace_id: bytes, span_id: bytes) -> UUID:
    return uuid5(
        NAMESPACE_URL,
        f"kevindb:run:{project_name}:{trace_id.hex()}:{span_id.hex()}",
    )


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

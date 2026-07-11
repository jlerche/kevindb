#[path = "../../../tests/support/mockgres.rs"]
mod mockgres_support;

use std::io::Read;
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

use anyhow::{Context, Result, bail};
use reqwest::StatusCode;
use serde_json::{Value, json};

struct ChildGuard {
    child: Child,
}

impl ChildGuard {
    fn new(child: Child) -> Self {
        Self { child }
    }
}

impl Drop for ChildGuard {
    fn drop(&mut self) {
        if self.child.try_wait().ok().flatten().is_none() {
            let _ = self.child.kill();
            let _ = self.child.wait();
        }
    }
}

#[tokio::test]
async fn local_service_roles_share_metastore_coordination_state() -> Result<()> {
    let mockgres = mockgres_support::start_mockgres_with_migrations().await?;
    let postgres_url = mockgres.postgres_url().to_owned();
    let ingest_port = reserve_port()?;
    let query_port = reserve_port()?;
    let coordinator_port = reserve_port()?;
    let ingest_url = format!("http://127.0.0.1:{ingest_port}");
    let query_url = format!("http://127.0.0.1:{query_port}");
    let coordinator_url = format!("http://127.0.0.1:{coordinator_port}");

    let mut ingest = start_server("ingest", ingest_port, &postgres_url, Some("ingest-a"))?;
    wait_for_ready(&ingest_url, &mut ingest).await?;
    let mut query = start_server("query", query_port, &postgres_url, None)?;
    wait_for_ready(&query_url, &mut query).await?;
    let mut coordinator = start_server("coordinator", coordinator_port, &postgres_url, None)?;
    wait_for_ready(&coordinator_url, &mut coordinator).await?;

    let client = reqwest::Client::new();
    let create_response = client
        .post(format!("{ingest_url}/runs"))
        .json(&json!({
            "id": "11111111-1111-4111-8111-111111111111",
            "trace_id": "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa",
            "session_name": "demo",
            "name": "multi.agent",
            "run_type": "chain",
            "inputs": {"prompt": "hello"},
            "start_time": "2026-01-01T00:00:00Z",
            "end_time": "2026-01-01T00:00:01Z"
        }))
        .send()
        .await?;
    assert_eq!(create_response.status(), StatusCode::ACCEPTED);

    let sessions: Vec<Value> = client
        .get(format!("{query_url}/sessions?name=demo"))
        .send()
        .await?
        .error_for_status()?
        .json()
        .await?;
    assert_eq!(sessions.len(), 1);
    assert_eq!(sessions[0]["name"], "demo");

    let route: Value = client
        .get(format!("{coordinator_url}/v1/projects/demo/route"))
        .send()
        .await?
        .error_for_status()?
        .json()
        .await?;
    assert_eq!(route["node_id"], "ingest-a");
    assert!(
        route["last_segment_uri"]
            .as_str()
            .is_some_and(|uri| uri.ends_with(".vortex"))
    );

    drop(coordinator);
    drop(query);
    drop(ingest);
    mockgres.stop().await?;
    Ok(())
}

fn start_server(
    role: &str,
    port: u16,
    postgres_url: &str,
    node_id: Option<&str>,
) -> Result<ChildGuard> {
    let mut command = Command::new(env!("CARGO_BIN_EXE_kevindb-server"));
    command
        .env("KEVINDB_BIND_ADDR", format!("127.0.0.1:{port}"))
        .env("KEVINDB_POSTGRES_URL", postgres_url)
        .env("KEVINDB_OBJECT_STORE", "memory")
        .env("KEVINDB_SERVICE_ROLE", role)
        .env("KEVINDB_RUN_MIGRATIONS", "false")
        .env("KEVINDB_INGEST_MAX_FLUSH_DELAY_MS", "0")
        .env("RUST_LOG", "warn")
        .stdout(Stdio::null())
        .stderr(Stdio::piped());
    if let Some(node_id) = node_id {
        command.env("KEVINDB_NODE_ID", node_id);
    }
    Ok(ChildGuard::new(
        command.spawn().context("spawn kevindb-server")?,
    ))
}

async fn wait_for_ready(base_url: &str, process: &mut ChildGuard) -> Result<()> {
    let client = reqwest::Client::new();
    let deadline = Instant::now() + Duration::from_secs(20);
    let mut last_error = String::new();
    while Instant::now() < deadline {
        if let Some(status) = process.child.try_wait().context("poll server process")? {
            let stderr = child_stderr(process);
            bail!("server exited before ready: {status}: {stderr}");
        }
        match client.get(format!("{base_url}/readyz")).send().await {
            Ok(response) if response.status() == StatusCode::OK => return Ok(()),
            Ok(response) => last_error = format!("HTTP {}", response.status()),
            Err(error) => last_error = error.to_string(),
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    bail!("server did not become ready: {last_error}")
}

fn child_stderr(process: &mut ChildGuard) -> String {
    let Some(mut stderr) = process.child.stderr.take() else {
        return String::new();
    };
    let mut output = String::new();
    let _ = stderr.read_to_string(&mut output);
    output
}

fn reserve_port() -> Result<u16> {
    let listener = std::net::TcpListener::bind(("127.0.0.1", 0))?;
    Ok(listener.local_addr()?.port())
}

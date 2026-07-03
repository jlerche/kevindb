use std::net::TcpListener;
use std::process::Stdio;
use std::sync::OnceLock;
use std::sync::atomic::{AtomicU16, Ordering};
use std::time::{Duration, Instant};

use anyhow::{Result, anyhow};
use kevindb::db::run_migrations;
use tokio::process::{Child, Command};
use tokio::time::sleep;
use tokio_postgres::NoTls;

pub struct MockgresInstance {
    child: Child,
    postgres_url: String,
}

impl MockgresInstance {
    pub async fn start_with_migrations() -> Result<Self> {
        let instance = Self::start().await?;
        run_migrations(instance.postgres_url()).await?;
        Ok(instance)
    }

    pub fn postgres_url(&self) -> &str {
        &self.postgres_url
    }

    pub async fn stop(mut self) -> Result<()> {
        self.child.start_kill()?;
        let _ = self.child.wait().await?;
        Ok(())
    }

    async fn start() -> Result<Self> {
        let mut last_err = None;
        for _attempt in 0..8 {
            let port = reserve_port()?;
            let postgres_url = format!("postgresql://127.0.0.1:{port}/postgres");
            let child = Command::new("mockgres")
                .arg("--host")
                .arg("127.0.0.1")
                .arg("--port")
                .arg(port.to_string())
                .stdout(Stdio::null())
                .stderr(Stdio::null())
                .spawn()?;

            let mut instance = Self {
                child,
                postgres_url,
            };
            match instance.wait_until_ready(Duration::from_secs(5)).await {
                Ok(()) => return Ok(instance),
                Err(err) => {
                    last_err = Some(err);
                    let _ = instance.child.start_kill();
                    let _ = instance.child.wait().await;
                }
            }
        }

        Err(last_err.unwrap_or_else(|| anyhow!("mockgres failed to start after retries")))
    }

    async fn wait_until_ready(&self, timeout: Duration) -> Result<()> {
        let deadline = Instant::now() + timeout;
        loop {
            match tokio_postgres::connect(&self.postgres_url, NoTls).await {
                Ok((client, connection)) => {
                    tokio::spawn(async move {
                        let _ = connection.await;
                    });
                    if client.simple_query("SELECT 1").await.is_ok() {
                        return Ok(());
                    }
                }
                Err(_) if Instant::now() >= deadline => {
                    return Err(anyhow!(
                        "mockgres did not become ready on {}",
                        self.postgres_url
                    ));
                }
                Err(_) => {}
            }

            if Instant::now() >= deadline {
                return Err(anyhow!(
                    "mockgres did not become ready on {}",
                    self.postgres_url
                ));
            }
            sleep(Duration::from_millis(50)).await;
        }
    }
}

impl Drop for MockgresInstance {
    fn drop(&mut self) {
        let _ = self.child.start_kill();
    }
}

fn reserve_port() -> Result<u16> {
    const MAX_CANDIDATES: usize = 1024;
    let next_port = next_port_counter();

    for _ in 0..MAX_CANDIDATES {
        let candidate = next_port.fetch_add(1, Ordering::Relaxed);
        if TcpListener::bind(("127.0.0.1", candidate)).is_ok() {
            return Ok(candidate);
        }
    }

    Err(anyhow!(
        "could not reserve mockgres port after trying {MAX_CANDIDATES} candidates"
    ))
}

fn next_port_counter() -> &'static AtomicU16 {
    static NEXT_PORT: OnceLock<AtomicU16> = OnceLock::new();
    NEXT_PORT.get_or_init(|| {
        let seed = TcpListener::bind("127.0.0.1:0")
            .ok()
            .and_then(|listener| listener.local_addr().ok().map(|addr| addr.port()))
            .unwrap_or(30_000);
        AtomicU16::new(seed.saturating_add(32))
    })
}

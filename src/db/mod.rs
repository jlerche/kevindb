use anyhow::Result;
use refinery::embed_migrations;
use tokio_postgres::NoTls;

mod embedded {
    use super::embed_migrations;
    // Keep this module in the same crate as migrations so new files are embedded.
    embed_migrations!("src/db/migrations");
}

pub async fn run_migrations(postgres_url: &str) -> Result<()> {
    let (mut client, connection) = tokio_postgres::connect(postgres_url, NoTls).await?;
    tokio::spawn(async move {
        if let Err(err) = connection.await {
            tracing::warn!(error = %err, "postgres migration connection failed");
        }
    });

    embedded::migrations::runner()
        .run_async(&mut client)
        .await?;
    Ok(())
}

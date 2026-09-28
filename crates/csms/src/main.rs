use std::env;

use anyhow::Context;
use csms::{AppState, MIGRATOR, router};
use sqlx::postgres::PgPoolOptions;
use tracing_subscriber::EnvFilter;
use utoipa::OpenApi;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // `csms openapi` prints the spec, used to keep openapi.json in the repo current.
    if env::args().nth(1).as_deref() == Some("openapi") {
        println!("{}", csms::api::ApiDoc::openapi().to_pretty_json()?);
        return Ok(());
    }

    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info,sqlx=warn")),
        )
        .init();

    let database_url = env::var("DATABASE_URL").context("DATABASE_URL is not set")?;
    let bind = env::var("BIND_ADDR").unwrap_or_else(|_| "0.0.0.0:8180".into());
    let heartbeat_interval = match env::var("HEARTBEAT_INTERVAL_SECS") {
        Ok(v) => v
            .parse()
            .context("HEARTBEAT_INTERVAL_SECS must be an integer")?,
        Err(_) => 300,
    };

    let pool = PgPoolOptions::new()
        .max_connections(20)
        .connect(&database_url)
        .await
        .context("connecting to Postgres")?;
    MIGRATOR.run(&pool).await.context("running migrations")?;

    let listener = tokio::net::TcpListener::bind(&bind)
        .await
        .with_context(|| format!("binding {bind}"))?;
    tracing::info!("listening on {bind}: OCPP at /ocpp/{{id}}, API at /api");

    axum::serve(listener, router(AppState::new(pool, heartbeat_interval)))
        .with_graceful_shutdown(async {
            let _ = tokio::signal::ctrl_c().await;
        })
        .await?;
    Ok(())
}

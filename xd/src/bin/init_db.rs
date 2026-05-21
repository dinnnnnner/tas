use anyhow::{Context, Result, bail};
use demo2::db::SCHEMA_SQL;
use tokio_postgres::{Config as PgConfig, NoTls};
use std::time::Instant;

#[derive(Debug, Clone)]
struct InitDbConfig {
    host: String,
    port: u16,
    user: String,
    password: String,
    database: String,
    bootstrap_db: String,
}

impl Default for InitDbConfig {
    fn default() -> Self {
        Self {
            host: std::env::var("DEMO2_DB_HOST").unwrap_or_else(|_| "127.0.0.1".to_string()),
            port: std::env::var("DEMO2_DB_PORT")
                .ok()
                .and_then(|v| v.parse::<u16>().ok())
                .unwrap_or(5432),
            user: std::env::var("DEMO2_DB_USER").unwrap_or_else(|_| "postgres".to_string()),
            password: std::env::var("DEMO2_DB_PASSWORD").unwrap_or_else(|_| "123456".to_string()),
            database: std::env::var("DEMO2_DB_NAME").unwrap_or_else(|_| "demo2".to_string()),
            bootstrap_db: std::env::var("DEMO2_BOOTSTRAP_DB")
                .unwrap_or_else(|_| "postgres".to_string()),
        }
    }
}

impl InitDbConfig {
    fn from_args() -> Result<Self> {
        let mut cfg = Self::default();
        let mut args = std::env::args().skip(1);

        while let Some(arg) = args.next() {
            match arg.as_str() {
                "--host" => cfg.host = next_arg(&mut args, "--host")?,
                "--port" => {
                    cfg.port = next_arg(&mut args, "--port")?
                        .parse::<u16>()
                        .context("invalid value for --port")?
                }
                "--user" => cfg.user = next_arg(&mut args, "--user")?,
                "--password" => cfg.password = next_arg(&mut args, "--password")?,
                "--database" => cfg.database = next_arg(&mut args, "--database")?,
                "--bootstrap-db" => cfg.bootstrap_db = next_arg(&mut args, "--bootstrap-db")?,
                "--help" | "-h" => {
                    print_help();
                    std::process::exit(0);
                }
                _ => bail!("unknown argument: {arg}"),
            }
        }

        Ok(cfg)
    }

    fn pg_config(&self, dbname: &str) -> PgConfig {
        let mut cfg = PgConfig::new();
        cfg.host(&self.host);
        cfg.port(self.port);
        cfg.user(&self.user);
        cfg.password(&self.password);
        cfg.dbname(dbname);
        cfg
    }
}

fn next_arg(args: &mut impl Iterator<Item = String>, flag: &str) -> Result<String> {
    args.next()
        .with_context(|| format!("missing value for {flag}"))
}

fn print_help() {
    println!(
        "Usage: cargo run --bin init_db -- [options]\n\
         \n\
         Options:\n\
           --host <host>                 PostgreSQL host (default: 127.0.0.1)\n\
           --port <port>                 PostgreSQL port (default: 5432)\n\
           --user <user>                 PostgreSQL user (default: postgres)\n\
           --password <password>         PostgreSQL password (default: 123456)\n\
           --database <name>             Target database to create (default: demo2)\n\
           --bootstrap-db <name>         Existing database used for bootstrap (default: postgres)\n\
         \n\
         Environment variables:\n\
           DEMO2_DB_HOST, DEMO2_DB_PORT, DEMO2_DB_USER,\n\
           DEMO2_DB_PASSWORD, DEMO2_DB_NAME, DEMO2_BOOTSTRAP_DB"
    );
}

fn quote_ident(value: &str) -> String {
    format!("\"{}\"", value.replace('"', "\"\""))
}

async fn connect(cfg: &InitDbConfig, dbname: &str) -> Result<tokio_postgres::Client> {
    let (client, connection) = cfg
        .pg_config(dbname)
        .connect(NoTls)
        .await
        .with_context(|| format!("failed to connect to database {dbname}"))?;

    tokio::spawn(async move {
        if let Err(err) = connection.await {
            eprintln!("postgres connection task ended: {err}");
        }
    });

    Ok(client)
}

async fn ensure_database(cfg: &InitDbConfig) -> Result<()> {
    let client = connect(cfg, &cfg.bootstrap_db).await?;
    let exists = client
        .query_opt(
            "SELECT 1 FROM pg_database WHERE datname = $1",
            &[&cfg.database],
        )
        .await
        .context("failed to check existing databases")?
        .is_some();

    if exists {
        println!("database '{}' already exists", cfg.database);
        return Ok(());
    }

    let create_sql = format!("CREATE DATABASE {}", quote_ident(&cfg.database));
    client
        .execute(&create_sql, &[])
        .await
        .with_context(|| format!("failed to create database {}", cfg.database))?;
    println!("database '{}' created", cfg.database);
    Ok(())
}

async fn ensure_schema(cfg: &InitDbConfig) -> Result<()> {
    let client = connect(cfg, &cfg.database).await?;
    let started = Instant::now();
    println!("applying partitioned schema...");
    client
        .batch_execute(SCHEMA_SQL)
        .await
        .with_context(|| format!("failed to initialize schema in {}", cfg.database))?;
    println!("schema applied in {:.2?}", started.elapsed());
    println!("schema initialized in '{}'", cfg.database);
    Ok(())
}

#[tokio::main]
async fn main() -> Result<()> {
    let cfg = InitDbConfig::from_args()?;
    println!(
        "initializing database '{}' on {}:{} as user '{}'",
        cfg.database, cfg.host, cfg.port, cfg.user
    );

    ensure_database(&cfg).await?;
    ensure_schema(&cfg).await?;

    println!("done");
    Ok(())
}

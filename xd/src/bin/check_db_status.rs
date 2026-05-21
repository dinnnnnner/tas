use anyhow::Result;
use tokio_postgres::NoTls;

const DEFAULT_DSN: &str = "host=127.0.0.1 port=5432 user=postgres password=123456 dbname=demo2";

#[tokio::main]
async fn main() -> Result<()> {
    let dsn = std::env::var("DEMO2_DB_CHECK_DSN").unwrap_or_else(|_| DEFAULT_DSN.to_string());
    let (client, connection) = tokio_postgres::connect(&dsn, NoTls).await?;
    tokio::spawn(async move {
        let _ = connection.await;
    });

    print_table_status(&client).await?;
    print_partition_status(&client).await?;
    print_latest_telemetry(&client).await?;

    Ok(())
}

async fn print_table_status(client: &tokio_postgres::Client) -> Result<()> {
    println!("== Table Status ==");
    for table in [
        "telemetry_samples",
        "telemetry_samples_legacy",
        "telemetry_samples_2026_04_16",
        "telemetry_samples_2026_04_17",
        "alarm_events",
        "system_events",
    ] {
        if table_exists(client, table).await? {
            let row = client
                .query_one(&format!("SELECT COUNT(*)::BIGINT FROM {table}"), &[])
                .await?;
            let count: i64 = row.get(0);
            println!("{table}: {count}");
        } else {
            println!("{table}: <missing>");
        }
    }
    println!();
    Ok(())
}

async fn print_partition_status(client: &tokio_postgres::Client) -> Result<()> {
    println!("== Partition Status ==");
    let rows = client
        .query(
            "SELECT c.relname, c.relkind, pt.partstrat IS NOT NULL AS is_partitioned
             FROM pg_class c
             JOIN pg_namespace n ON n.oid = c.relnamespace
             LEFT JOIN pg_partitioned_table pt ON pt.partrelid = c.oid
             WHERE n.nspname = 'public'
               AND c.relname IN ('telemetry_samples', 'telemetry_samples_legacy')
             ORDER BY c.relname",
            &[],
        )
        .await?;

    for row in rows {
        let name: String = row.get(0);
        let relkind: i8 = row.get(1);
        let is_partitioned: bool = row.get(2);
        println!(
            "{name}: relkind={} partitioned={is_partitioned}",
            relkind as u8 as char
        );
    }

    let rows = client
        .query(
            "SELECT child.relname
             FROM pg_inherits
             JOIN pg_class parent ON pg_inherits.inhparent = parent.oid
             JOIN pg_class child ON pg_inherits.inhrelid = child.oid
             JOIN pg_namespace n ON n.oid = parent.relnamespace
             WHERE n.nspname = 'public'
               AND parent.relname = 'telemetry_samples'
             ORDER BY child.relname",
            &[],
        )
        .await?;

    if rows.is_empty() {
        println!("partitions: <none>");
    } else {
        println!("partitions:");
        for row in rows {
            let name: String = row.get(0);
            println!("  - {name}");
        }
    }
    println!();
    Ok(())
}

async fn print_latest_telemetry(client: &tokio_postgres::Client) -> Result<()> {
    println!("== Latest Telemetry ==");
    let rows = client
        .query(
            "SELECT
                tableoid::regclass::text AS physical_table,
                id,
                ts_ms,
                created_at,
                device_id,
                sensor_id,
                axis,
                value,
                request_id
             FROM telemetry_samples
             ORDER BY created_at DESC, id DESC
             LIMIT 10",
            &[],
        )
        .await?;

    if rows.is_empty() {
        println!("latest rows: <none>");
        return Ok(());
    }

    for row in rows {
        let physical_table: String = row.get(0);
        let id: i64 = row.get(1);
        let ts_ms: i64 = row.get(2);
        let created_at: std::time::SystemTime = row.get(3);
        let device_id: String = row.get(4);
        let sensor_id: i32 = row.get(5);
        let axis: String = row.get(6);
        let value: f64 = row.get(7);
        let request_id: i64 = row.get(8);
        println!(
            "{physical_table} | id={id} ts_ms={ts_ms} created_at={created_at:?} device={device_id} sensor={sensor_id} axis={axis} value={value} req={request_id}"
        );
    }

    Ok(())
}

async fn table_exists(client: &tokio_postgres::Client, table: &str) -> Result<bool> {
    let row = client
        .query_one(
            "SELECT EXISTS (
                SELECT 1
                FROM information_schema.tables
                WHERE table_schema = 'public'
                  AND table_name = $1
            )",
            &[&table],
        )
        .await?;
    Ok(row.get(0))
}

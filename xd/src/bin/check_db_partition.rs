use tokio_postgres::NoTls;

const DEFAULT_DSN: &str = "host=127.0.0.1 port=5432 user=postgres password=123456 dbname=demo2";

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let dsn = std::env::var("DEMO2_DB_CHECK_DSN").unwrap_or_else(|_| DEFAULT_DSN.to_string());
    let (client, connection) = tokio_postgres::connect(&dsn, NoTls).await?;
    tokio::spawn(async move {
        let _ = connection.await;
    });

    println!("TABLES");
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
        println!("{name}|{}|partitioned={is_partitioned}", relkind as u8 as char);
    }

    println!("PARTITIONS");
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
    for row in rows {
        let name: String = row.get(0);
        println!("{name}");
    }

    Ok(())
}

mod publication;
mod replication;

use std::env;
use std::sync::Arc;
use tokio::task;

/// starts streaming changes
pub async fn start_streaming_changes(
    db_string: String,
    table_name: String,
) -> Result<(), tokio_postgres::Error> {
    let repl_client = Arc::new(
        replication::DBClient::new(&format!("{} replication=database", db_string))
            .await
            .unwrap()
            .client,
    );

    let storage_client = Arc::new(replication::DBClient::new(&db_string).await.unwrap().client);

    // Hardcoded for now
    let schema_name = "public".to_string();
    let ddb_path = "./basin.duck.db";

    let mut publication =
        publication::Publication::new(Arc::clone(&storage_client), &schema_name, &table_name);

    // Check if publication exists or create it if it doesn't
    if publication.check_exists().await? {
    } else {
        publication.create().await?;
    }

    // Inspect the source table and fetch the columns
    publication.get_columns().await?;

    let slot_name = format!("basin_{}", table_name);
    let mut slot = replication::Slot::new(Arc::clone(&repl_client), &slot_name);
    slot.get_confirmed_lsn().await?;
    // Create new replication slot if LSN is None
    if slot.lsn.is_none() {
        println!("Replication slot {:?} doesn't exist", slot_name);
        println!("Creating replication slot {:?}", slot_name);
        slot.create().await?;
    }

    let mut replicator = replication::Replicator::new(
        Arc::clone(&repl_client),
        slot,
        publication,
        &ddb_path.to_string(),
    );
    replicator.start_replication().await;
    Ok(())
}

#[tokio::main]
async fn main() {
    let db_conn_str = match env::var("DB_CONF") {
        Ok(val) => val,
        Err(_) => {
            "user=postgres password=password host=localhost port=5432 dbname=postgres".to_string()
        }
    };
    println!("Connecting to PG with DB_CONF: {}", db_conn_str);

    let table_name = match env::var("TABLE_NAME") {
        Ok(val) => val,
        Err(_) => panic!("TABLE_NAME not set"),
    };
    println!("Listening to Table: {}", &table_name);

    let streaming_handle =
        task::spawn(async { start_streaming_changes(db_conn_str, table_name).await });
    streaming_handle.await.unwrap().unwrap();
}

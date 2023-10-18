use crate::publication::Column;
use crate::publication::Publication;
use bytes::Bytes;
use duckdb::Connection;
use futures::{
    future::{self},
    ready, Sink, StreamExt,
};
use serde_json::Value;
use std::pin::Pin;
use std::sync::Arc;
use std::{
    task::Poll,
    time::{SystemTime, UNIX_EPOCH},
};
use tokio_postgres::{
    types::{self, PgLsn},
    CopyBothDuplex, NoTls, SimpleQueryMessage,
};

const SECONDS_FROM_UNIX_EPOCH_TO_2000: u128 = 946684800;

pub struct Slot {
    client: Arc<tokio_postgres::Client>,
    name: String,
    pub lsn: Option<PgLsn>,
}

impl Slot {
    pub fn new(client: Arc<tokio_postgres::Client>, slot_name: &String) -> Self {
        Self {
            client: client,
            name: slot_name.clone(),
            lsn: None,
        }
    }

    pub async fn get_confirmed_lsn(&mut self) -> Result<(), tokio_postgres::Error> {
        let query = format!(
            "SELECT confirmed_flush_lsn FROM pg_replication_slots WHERE slot_name = '{}'",
            self.name
        );
        let result = self.client.simple_query(&query).await?;
        let rows = result
            .into_iter()
            .filter_map(|msg| match msg {
                SimpleQueryMessage::Row(row) => Some(row),
                _ => None,
            })
            .collect::<Vec<_>>();

        if let Some(slot) = rows.first() {
            let lsn = slot.get("confirmed_flush_lsn").unwrap().to_string();
            self.lsn = Some(lsn.parse::<PgLsn>().unwrap());
        }

        Ok(())
    }

    pub async fn create(&mut self) -> Result<(), tokio_postgres::Error> {
        let slot_query = format!(
            "CREATE_REPLICATION_SLOT {} LOGICAL \"wal2json\" NOEXPORT_SNAPSHOT",
            self.name
        );
        let result = self.client.simple_query(&slot_query).await?;

        let lsn = result
            .into_iter()
            .filter_map(|msg| match msg {
                SimpleQueryMessage::Row(row) => Some(row),
                _ => None,
            })
            .collect::<Vec<_>>()
            .first()
            .unwrap()
            .get("consistent_point")
            .unwrap()
            .to_owned();
        println!("Created replication slot: {:?}", lsn);
        self.lsn = Some(lsn.parse::<PgLsn>().unwrap());
        Ok(())
    }
}

pub struct DBClient {
    pub client: tokio_postgres::Client,
}

impl DBClient {
    pub async fn new(db_config: &str) -> Result<Self, tokio_postgres::Error> {
        let (client, connection) = tokio_postgres::connect(db_config, NoTls).await?;
        tokio::spawn(async move { connection.await });
        Ok(Self { client })
    }
}

pub struct Replicator {
    commit_lsn: types::PgLsn,
    slot_name: String,
    schema_name: String,
    table_name: String,
    table_cols: Vec<Column>,
    client: Arc<tokio_postgres::Client>,
    ddb_path: String,
    records: Vec<Value>,
    stream: Option<Pin<Box<CopyBothDuplex<Bytes>>>>,
}

fn prepare_ssu(write_lsn: PgLsn) -> Bytes {
    let write_lsn_bytes = u64::from(write_lsn).to_be_bytes();
    let time_since_2000: u64 = (SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_micros()
        - (SECONDS_FROM_UNIX_EPOCH_TO_2000 * 1000 * 1000))
        .try_into()
        .unwrap();

    // see here for format details: https://www.postgresql.org/docs/10/protocol-replication.html
    let mut data_to_send: Vec<u8> = vec![];
    // Byte1('r'); Identifies the message as a receiver status update.
    data_to_send.extend_from_slice(&[114]); // "r" in ascii

    // The location of the last WAL byte + 1 received and written to disk in the standby.
    data_to_send.extend_from_slice(write_lsn_bytes.as_ref());

    // The location of the last WAL byte + 1 flushed to disk in the standby.
    data_to_send.extend_from_slice(write_lsn_bytes.as_ref());

    // The location of the last WAL byte + 1 applied in the standby.
    data_to_send.extend_from_slice(write_lsn_bytes.as_ref());

    // The client's system clock at the time of transmission, as microseconds since midnight on 2000-01-01.
    //0, 0, 0, 0, 0, 0, 0, 0,
    data_to_send.extend_from_slice(&time_since_2000.to_be_bytes());
    // Byte1; If 1, the client requests the server to reply to this message immediately. This can be used to ping the server, to test if the connection is still healthy.
    data_to_send.extend_from_slice(&[1]);

    Bytes::from(data_to_send)
}

pub fn create_table(
    conn: &Connection,
    table_name: &String,
    columns: &Vec<Column>,
) -> Result<(), duckdb::Error> {
    let mut cols = String::new();
    let mut pks = String::new();

    for (i, column) in columns.iter().enumerate() {
        let cname = column.name.clone();
        let ctype = column.data_type.clone();
        // (todo): remap PG data types to duckdb data types
        // https://www.postgresql.org/docs/current/datatype.html
        // https://duckdb.org/docs/archive/0.4.0/sql/data_types/overview
        let mut col = format!("{cname} {ctype}");

        if !column.is_nullable {
            col = format!("{col} NOT NULL");
        }
        let is_pk = column.is_primary;

        if i == 0 {
            cols = col;
            if is_pk {
                pks = cname;
            }
        } else {
            cols = format!("{cols},{col}");
            if is_pk {
                pks = format!("{pks},{cname}");
            }
        }
    }

    if !pks.is_empty() {
        cols = format!("{cols},PRIMARY KEY ({pks})");
    }

    if cols.is_empty() {
        return Err(duckdb::Error::InvalidQuery);
    }

    let query = format!("CREATE TABLE IF NOT EXISTS {table_name} ({cols})",);

    println!("Create query: {:?}", query);
    conn.execute_batch(&query)?;

    Ok(())
}

impl Replicator {
    pub fn new(
        client: Arc<tokio_postgres::Client>,
        slot: Slot,
        publication: Publication,
        db_path: &String,
    ) -> Self {
        Self {
            schema_name: publication.schema_name.clone(),
            table_name: publication.table_name.clone(),
            table_cols: publication.table_cols.clone(),
            // lsn must be assigned at this point else we panic
            commit_lsn: slot.lsn.unwrap().clone(),
            slot_name: slot.name.clone(),
            client,
            ddb_path: db_path.clone(),
            records: vec![],
            stream: None,
        }
    }

    async fn commit(&mut self) {
        let buf = prepare_ssu(self.commit_lsn);
        self.send_ssu(buf).await;

        self.records.clear();
        println!("Clearing records: {:?}", self.records);
    }

    async fn send_ssu(&mut self, buf: Bytes) {
        println!("Trying to send SSU");
        let mut next_step = 1;
        future::poll_fn(|cx| loop {
            // println!("Doing step:{}", next_step);
            match next_step {
                1 => {
                    ready!(self.stream.as_mut().unwrap().as_mut().poll_ready(cx)).unwrap();
                }
                2 => {
                    self.stream
                        .as_mut()
                        .unwrap()
                        .as_mut()
                        .start_send(buf.clone())
                        .unwrap();
                }
                3 => {
                    ready!(self.stream.as_mut().unwrap().as_mut().poll_flush(cx)).unwrap();
                }
                4 => return Poll::Ready(()),
                _ => panic!(),
            }
            next_step += 1;
        })
        .await;
        println!("Sent SSU");
    }

    fn replicate(&self) -> Result<(), duckdb::Error> {
        // open duck db for replicating
        let conn = Connection::open(&self.ddb_path).unwrap();

        // Create table if it doesn't exist in destination db
        create_table(&conn, &self.table_name.to_string(), &self.table_cols).unwrap();

        let inserts = self
            .records
            .iter()
            .filter_map(|r| {
                let mut cols = String::new();
                let mut vals = String::new();
                let columns = r["columns"].as_array().unwrap();
                for (i, column) in columns.iter().enumerate() {
                    let cname = column["name"].as_str().unwrap();
                    // (important!)
                    // checkout: https://github.com/duckdb/postgres_scanner/blob/7bf89ccbc4bd3832418c205522a02a32cc2ae755/src/postgres_utils.cpp#L65
                    // (todo): maybe we need to carefully handle individual types
                    // instead of just converting to string
                    let value = format! {"{}", column["value"]};
                    if i == 0 {
                        cols = cname.to_string();
                        vals = value.to_string();
                    } else {
                        cols = format!("{cols},{cname}");
                        vals = format!("{vals},{value}");
                    }
                }

                if cols.is_empty() || vals.is_empty() {
                    // ignore if no columns or values in the record
                    return None;
                }

                Some(
                    format!("INSERT INTO {} ({cols}) VALUES({vals})", self.table_name)
                        .replace('\"', "'"),
                )
            })
            .collect::<Vec<_>>();

        if inserts.is_empty() {
            return Err(duckdb::Error::InvalidQuery);
        }

        let insert_query = inserts.join(";");
        println!("Executing insert query: {:?}", insert_query);
        conn.execute_batch(&insert_query)?;
        Ok(())
    }

    async fn process_record(&mut self, record: Value) {
        match record["action"].as_str().unwrap() {
            "B" => {
                println!("Begin===");
                println!("{}", serde_json::to_string_pretty(&record).unwrap());
                let lsn_str = record["nextlsn"].as_str().unwrap();
                self.commit_lsn = lsn_str.parse::<PgLsn>().unwrap();
            }
            "C" => {
                let end_lsn_str = record["nextlsn"].as_str().unwrap();
                let end_lsn = end_lsn_str.parse::<PgLsn>().unwrap();
                if end_lsn != self.commit_lsn {
                    println!(
                        "commit and begin next_lsn don't match: {:?}",
                        record["nextlsn"]
                    );
                }
                println!("Commit===");
                println!("{}", serde_json::to_string_pretty(&record).unwrap());

                // (todo): handle error
                self.replicate().unwrap_or_else(|_| {
                    println!("Ignoring replication error");
                });
                self.commit().await;
            }
            "I" => {
                println!("Insert===");
                println!("{}", serde_json::to_string_pretty(&record).unwrap());
                self.records.push(record);
            }
            _ => {
                println!("unknown message");
            }
        }
    }

    async fn process_txn(&mut self, event: &[u8]) {
        match event[0] {
            b'w' => {
                // first 24 bytes are metadata
                let json: Value = serde_json::from_slice(&event[25..]).unwrap();
                self.process_record(json).await;
            }
            b'k' => {
                let last_byte = event.last().unwrap();
                let timeout_imminent = last_byte == &1;
                println!(
                    "Got keepalive message @timeoutImminent:{}, @LSN:{:x?}",
                    timeout_imminent, self.commit_lsn,
                );
                if timeout_imminent {
                    let buf = prepare_ssu(self.commit_lsn);
                    self.send_ssu(buf).await;
                }
            }
            _ => (),
        }
    }

    pub async fn start_replication(&mut self) {
        let full_table_name = format!("{}.{}", self.schema_name, self.table_name);
        let options = vec![
            ("pretty-print", "false"),
            ("include-transaction", "true"),
            ("include-lsn", "true"),
            ("include-timestamp", "true"),
            ("include-pk", "true"),
            ("format-version", "2"),
            ("include-xids", "true"),
            ("add-tables", &full_table_name),
        ];
        let start_lsn = self.commit_lsn.to_string();
        let query = format!(
            "START_REPLICATION SLOT {} LOGICAL {} ({})",
            self.slot_name,
            start_lsn,
            // specify table for replication
            options
                .iter()
                .map(|(k, v)| format!("\"{}\" '{}'", k, v))
                .collect::<Vec<_>>()
                .join(", ")
        );
        let duplex_stream = self
            .client
            .copy_both_simple::<bytes::Bytes>(&query)
            .await
            .unwrap();

        // Pin the stream
        self.stream = Some(Box::pin(duplex_stream));

        // listen
        loop {
            match self.stream.as_mut().unwrap().next().await {
                Some(Ok(event)) => {
                    // (todo:) should return error?
                    self.process_txn(&event).await;
                }
                Some(Err(e)) => {
                    println!("Error reading from stream:{}", e);
                    continue;
                }
                None => {
                    println!("Stream closed");
                    break;
                }
            }
        }
    }
}

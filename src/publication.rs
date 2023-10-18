use std::sync::Arc;
use tokio_postgres::SimpleQueryMessage;

pub struct Publication {
    pub client: Arc<tokio_postgres::Client>,
    pub schema_name: String,
    pub table_name: String,
    pub table_cols: Vec<Column>,
}

#[derive(Debug, Clone)]
pub struct Column {
    pub name: String,
    pub data_type: String,
    pub is_nullable: bool,
    pub is_primary: bool,
}

impl Publication {
    pub fn new(
        client: Arc<tokio_postgres::Client>,
        schema_name: &String,
        table_name: &String,
    ) -> Self {
        Self {
            client: client,
            schema_name: schema_name.clone(),
            table_name: table_name.clone(),
            table_cols: vec![],
        }
    }

    #[inline]
    pub fn pub_name(&self) -> String {
        format!("basin_{}", self.table_name)
    }

    pub async fn check_exists(&self) -> Result<bool, tokio_postgres::Error> {
        let pub_name = self.pub_name();
        let query = format!(
            "SELECT schemaname, tablename
                FROM pg_publication p
                JOIN pg_publication_tables pt ON p.pubname = pt.pubname
                WHERE p.pubname = '{}'",
            pub_name
        );
        let result = self.client.simple_query(&query).await?;
        let rows = result
            .into_iter()
            .filter_map(|msg| match msg {
                SimpleQueryMessage::Row(row) => Some(row),
                _ => None,
            })
            .collect::<Vec<_>>();

        if let Some(publication) = rows.first() {
            let schema_name = publication.get("schemaname").unwrap().to_string();
            let table_name = publication.get("tablename").unwrap().to_string();
            println!(
                "Found publication {:?}/{:?}, ready to start replication",
                schema_name, table_name
            );
            return Ok(true);
        } else {
            return Ok(false);
        }
    }

    pub async fn create(&self) -> Result<u64, tokio_postgres::Error> {
        let query = format!(
            "CREATE PUBLICATION {} FOR TABLE {}",
            self.pub_name(),
            self.table_name
        );
        println!("Creating publication: {:?}", query);
        let result = self.client.execute(&query, &[]).await?;
        println!("Created publication: {:?}", result);
        Ok(result)
    }

    pub async fn get_columns(&mut self) -> Result<(), tokio_postgres::Error> {
        let query = "WITH primary_key_info AS
        (SELECT tc.constraint_schema,
                tc.table_name,
                ccu.column_name
        FROM information_schema.table_constraints tc
        JOIN information_schema.constraint_column_usage AS ccu USING (CONSTRAINT_SCHEMA, CONSTRAINT_NAME)
        WHERE constraint_type = 'PRIMARY KEY' )
    SELECT
        c.column_name,
        c.data_type,
        c.is_nullable = 'YES' AS is_nullable,
        pki.column_name IS NOT NULL AS is_primary
    FROM information_schema.columns AS c
    LEFT JOIN primary_key_info pki ON c.table_schema = pki.constraint_schema
        AND pki.table_name = c.table_name
        AND pki.column_name = c.column_name
    WHERE c.table_name = $1; 
    ";

        let res = self.client.query(query, &[&self.table_name]).await?;
        let cols = res
            .into_iter()
            .map(|row| {
                let name = row.get::<_, String>("column_name");
                let data_type = row.get::<_, String>("data_type");
                let is_nullable = row.get::<_, bool>("is_nullable");
                let is_primary = row.get::<_, bool>("is_primary");
                Column {
                    name: name,
                    data_type: data_type,
                    is_nullable: is_nullable,
                    is_primary: is_primary,
                }
            })
            .collect::<Vec<_>>();
        self.table_cols = cols;
        Ok(())
    }
}

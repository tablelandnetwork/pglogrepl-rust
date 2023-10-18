# Postgres Logical Replication

## Overview

This is a POC to test out the streaming logical replication in Postgres. It requires a Postgres database running. It currenlty only replicates `insert` statements. To try it run the following command with your db config:

```rust
DB_CONF="user=postgres password=password host=localhost port=5432 dbname=postgres" cargo run
```

It will start replicating every insert transactions on the source database as they arrive by following the Postgres Replication protocol. As soon as receives a new transaction, it replays it in an embeded [duckdb](https://duckdb.org/) database.

This POC uses [Materialize](https://materialize.com/)'s fork of [rust-postgres](https://github.com/MaterializeInc/rust-postgres) for logical replication protocol implementation.

### Useful links

- Logical decoding [example](https://github.com/seddonm1/logicaldecoding)
- `rust-postgres`: https://github.com/sfackler/rust-postgres/issues/116
- [Materialize](https://materialize.com/)'s fork of `rust-postgres` with the patches required to support logical decoding: https://github.com/MaterializeInc/rust-postgres
- replication example: https://github.com/debate-map/app/blob/afc6467b6c6c961f7bcc7b7f901f0ff5cd79d440/Packages/app-server-rs/src/pgclient.rs

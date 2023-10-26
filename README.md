# Postgres Logical Replication

## Overview

This is a POC to test out Postgres streaming logical replication in Rust using [rust-postgres](https://github.com/sfackler/rust-postgres).

It requires:

- a running postgres database server.
- [wal2json](https://github.com/eulerto/wal2json) plugin should be installed.
- ensure `wal_level` config is set to 'logical' (`ALTER SYSTEM SET wal_level = logical;`)

This example shows how to:

- create a [publication](https://www.postgresql.org/docs/current/logical-replication-publication.html#LOGICAL-REPLICATION-PUBLICATION)
- create a [replication slot](https://www.postgresql.org/docs/current/logical-replication-subscription.html#LOGICAL-REPLICATION-SUBSCRIPTION-SLOT)
- read [protocol](https://www.postgresql.org/docs/current/protocol-replication.html) messages
- read json encoded WAL updates

Currently, the example only replicates `insert` statements on the given table. To try it run the following command with your db config:

```rust
DB_CONF="user=postgres password=password host=localhost port=5432 dbname=postgres" TABLE_NAME=<your-table-name> cargo run
```

It will start replicating every insert transactions on the source database as they arrive by following the Postgres Replication protocol. As soon as receives a new transaction, it replays it in an embeded [duckdb](https://duckdb.org/) database.

This POC uses [Materialize](https://materialize.com/)'s fork of [rust-postgres](https://github.com/MaterializeInc/rust-postgres) for logical replication protocol implementation.

### Helpful links

- Logical decoding [example](https://github.com/seddonm1/logicaldecoding)
- Replication issue on `rust-postgres` [repo](https://github.com/sfackler/rust-postgres/issues/116)
- [Materialize](https://github.com/MaterializeInc/rust-postgres)'s fork of `rust-postgres` with the patches required to support logical decoding
- Replication [example](https://github.com/debate-map/app/blob/afc6467b6c6c961f7bcc7b7f901f0ff5cd79d440/Packages/app-server-rs/src/pgclient.rs)
- Instructure's Change data [capture](https://github.com/instructure/jsoncdc)
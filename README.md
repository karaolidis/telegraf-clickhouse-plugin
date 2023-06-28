# telegraf-clickhouse-plugin

The ClickHouse output plugin saves Telegraf metric data to a ClickHouse database.

The plugin uses a simple, hard-coded database schema. There is a table for each metric type and the table name is the metric name. There is a column per field and a column per tag. There is an optional column for the metric timestamp.

A row is written for every input metric. This means multiple metrics are never merged into a single row, even if they have the same metric name, tags, and timestamp.

The plugin uses Golang's generic "database/sql" interface and third party drivers.

## Getting started

To use the plugin, set the data source name (DSN). The user account must have privileges to insert rows and create tables.

## Advanced options

When the plugin first connects it runs SQL from the init_sql setting, allowing you to perform custom initialization for the connection.

Before inserting a row, the plugin checks whether the table exists. If it doesn't exist, the plugin creates the table.

## DSN

Currently, Telegraf's sql output plugin depends on [clickhouse-go v1.5.4](https://github.com/ClickHouse/clickhouse-go/tree/v1.5.4) which uses a [different DSN format](https://github.com/ClickHouse/clickhouse-go/tree/v1.5.4#dsn) than its newer `v2.*` version.

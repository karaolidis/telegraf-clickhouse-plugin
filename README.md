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

The name of the timestamp column is "timestamp" but it can be changed with the timestamp_column setting. The timestamp column can be completely disabled by setting it to "".

## Configuration

```toml @sample.conf
# Save metrics to an SQL Database
[[outputs.sql]]
  ## Data source name
  # data_source_name = ""

  ## Timestamp column name
  # timestamp_column = "timestamp"

  ## Initialization SQL
  # init_sql = ""

  ## Maximum amount of time a connection may be idle. "0s" means connections are
  ## never closed due to idle time.
  # connection_max_idle_time = "0s"

  ## Maximum amount of time a connection may be reused. "0s" means connections
  ## are never closed due to age.
  # connection_max_lifetime = "0s"

  ## Maximum number of connections in the idle connection pool. 0 means unlimited.
  # connection_max_idle = 2

  ## Maximum number of open connections to the database. 0 means unlimited.
  # connection_max_open = 0
```

## Driver-specific information

### clickhouse

#### DSN

Currently, Telegraf's sql output plugin depends on [clickhouse-go v1.5.4](https://github.com/ClickHouse/clickhouse-go/tree/v1.5.4) which uses a [different DSN format](https://github.com/ClickHouse/clickhouse-go/tree/v1.5.4#dsn) than its newer `v2.*` version.

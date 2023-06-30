# telegraf-clickhouse-plugin

The ClickHouse output plugin saves Telegraf metric data to a ClickHouse database.

The plugin uses Golang's generic "database/sql" interface and third party drivers.

## Getting started

To use the plugin, set the data source name (DSN). The user account must have privileges to insert rows and create tables.

## Advanced options

When the plugin first connects it runs SQL from the init_sql setting, allowing you to perform custom initialization for the connection.

Before inserting a row, the plugin checks whether the table exists. If it doesn't exist, the plugin creates the table.

## DSN

Currently, Telegraf's sql output plugin depends on [clickhouse-go v1.5.4](https://github.com/ClickHouse/clickhouse-go/tree/v1.5.4) which uses a [different DSN format](https://github.com/ClickHouse/clickhouse-go/tree/v1.5.4#dsn) than its newer `v2.*` version.

//go:generate ../../../tools/readme_config_includer/generator
package sql

import (
	gosql "database/sql"
	_ "embed"
	"fmt"
	"strings"
	"time"

	_ "github.com/ClickHouse/clickhouse-go"

	"github.com/influxdata/telegraf"
	"github.com/influxdata/telegraf/config"
	"github.com/influxdata/telegraf/plugins/outputs"
)

//go:embed sample.conf
var sampleConfig string

type SingleTableOptions struct {
	TableName string `toml:"table_name"`
}

type MultiTableOptions struct {
	TablePrefix string `toml:"table_prefix"`
}

type ClickHouse struct {
	DataSourceName        string             `toml:"data_source_name"`
	InitSQL               string             `toml:"init_sql"`
	TimestampColumn       string             `toml:"timestamp_column"`
	TTL                   string             `toml:"ttl"`
	TableMode             string             `toml:"table_mode"`
	SingleTableOptions    SingleTableOptions `toml:"single_table"`
	MultiTableOptions     MultiTableOptions  `toml:"multi_table"`
	ConnectionMaxIdleTime config.Duration    `toml:"connection_max_idle_time"`
	ConnectionMaxLifetime config.Duration    `toml:"connection_max_lifetime"`
	ConnectionMaxIdle     int                `toml:"connection_max_idle"`
	ConnectionMaxOpen     int                `toml:"connection_max_open"`

	db  *gosql.DB
	Log telegraf.Logger `toml:"-"`
}

func (*ClickHouse) SampleConfig() string {
	return sampleConfig
}

func (p *ClickHouse) Init() error {
	if p.DataSourceName == "" {
		return fmt.Errorf("data_source_name is a required configuration option")
	}

	if p.TimestampColumn == "" {
		fmt.Println("timestamp_column is not set, using default value: timestamp")
		p.TimestampColumn = "timestamp"
	}

	if p.TableMode == "" {
		fmt.Println("table_mode is not set, using default value: single")
		p.TableMode = "single"
	} else if p.TableMode != "single" && p.TableMode != "multi" {
		return fmt.Errorf("table_mode must be one of: single, multi")
	}

	if p.TableMode == "single" && p.SingleTableOptions.TableName == "" {
		p.SingleTableOptions.TableName = "telegraf"
	}

	return nil
}

func (p *ClickHouse) Connect() error {
	db, err := gosql.Open("clickhouse", p.DataSourceName)
	if err != nil {
		return err
	}

	err = db.Ping()
	if err != nil {
		return err
	}

	db.SetConnMaxIdleTime(time.Duration(p.ConnectionMaxIdleTime))
	db.SetConnMaxLifetime(time.Duration(p.ConnectionMaxLifetime))
	db.SetMaxIdleConns(p.ConnectionMaxIdle)
	db.SetMaxOpenConns(p.ConnectionMaxOpen)

	if p.InitSQL != "" {
		_, err = db.Exec(p.InitSQL)
		if err != nil {
			return err
		}
	}

	p.db = db
	fmt.Println("Connected to ClickHouse!")

	return nil
}

func (p *ClickHouse) Close() error {
	return p.db.Close()
}

func quoteIdent(name string) string {
	return `"` + strings.ReplaceAll(sanitizeQuoted(name), `"`, `""`) + `"`
}

func sanitizeQuoted(in string) string {
	// https://dev.mysql.com/doc/refman/8.0/en/identifiers.html
	// https://www.postgresql.org/docs/13/sql-syntax-lexical.html#SQL-SYNTAX-IDENTIFIERS

	// Whitelist allowed characters
	return strings.Map(func(r rune) rune {
		switch {
		case r >= '\u0001' && r <= '\uFFFF':
			return r
		default:
			return '_'
		}
	}, in)
}

func (p *ClickHouse) deriveDatatype(value interface{}) string {
	var datatype string

	switch value.(type) {
	case int64:
		datatype = "Int64"
	case uint64:
		datatype = "UInt64"
	case float64:
		datatype = "Float64"
	case string:
		datatype = "String"
	case bool:
		datatype = "UInt8"
	case time.Time:
		datatype = "DateTime"
	default:
		datatype = "String"
		p.Log.Errorf("unknown datatype for value %v", value)
	}

	return datatype
}

func (p *ClickHouse) generateCreateTable(tablename string, columns []string, datatypes map[string]string) string {
	columnDefs := make([]string, 0, len(columns))
	for _, column := range columns {
		columnDefs = append(columnDefs, fmt.Sprintf("%s %s", quoteIdent(column), datatypes[column]))
	}

	orderBy := make([]string, 0, len(columns))
	if _, ok := datatypes["host"]; ok {
		orderBy = append(orderBy, "host")
	}
	orderBy = append(orderBy, quoteIdent(p.TimestampColumn))
	if _, ok := datatypes["measurement"]; ok {
		orderBy = append(orderBy, "measurement")
	}

	createTable := fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s (%s) ENGINE = MergeTree() ORDER BY (%s) PARTITION BY toYYYYMM(%s)",
		quoteIdent(tablename),
		strings.Join(columnDefs, ","),
		strings.Join(orderBy, ","),
		quoteIdent(p.TimestampColumn))

	if p.TTL != "" {
		createTable += fmt.Sprintf(" TTL %s + INTERVAL %s", quoteIdent(p.TimestampColumn), p.TTL)
	}

	return createTable
}

func (p *ClickHouse) generateAlterTable(tablename string, columns []string, datatypes map[string]string) string {
	alterDefs := make([]string, 0, len(columns))

	for _, column := range columns {
		alterDefs = append(alterDefs, fmt.Sprintf("ADD COLUMN IF NOT EXISTS %s %s",
			quoteIdent(column), datatypes[column]))
	}

	return fmt.Sprintf("ALTER TABLE %s %s",
		quoteIdent(tablename),
		strings.Join(alterDefs, ","))
}

func (p *ClickHouse) generateInsert(tablename string, columns []string, batchSize int) string {
	quotedColumns := make([]string, 0, len(columns))
	for _, column := range columns {
		quotedColumns = append(quotedColumns, quoteIdent(column))
	}

	placeholder := "(" + strings.Repeat("?,", len(columns)-1) + "?)"
	placeholders := strings.Repeat(placeholder+",", batchSize-1) + placeholder

	return fmt.Sprintf("INSERT INTO %s(%s) VALUES %s",
		quoteIdent(tablename),
		strings.Join(quotedColumns, ", "),
		placeholders)
}

func (p *ClickHouse) isUnknownTableErr(err error) bool {
	if err == nil {
		return false
	}

	return strings.Contains(err.Error(), "code: 60")
}

func (p *ClickHouse) isNoSuchColumnInTableErr(err error) bool {
	if err == nil {
		return false
	}

	return strings.Contains(err.Error(), "code: 16")
}

func (p *ClickHouse) Write(metrics []telegraf.Metric) error {
	metricsByTable := make(map[string][]map[string]interface{})
	columDatatypes := make(map[string]map[string]string)
	tableColumns := make(map[string][]string)
	tableLengths := make(map[string]int)

	for _, metric := range metrics {
		tablename := metric.Name()

		if _, ok := metricsByTable[tablename]; !ok {
			metricsByTable[tablename] = make([]map[string]interface{}, 0, len(metrics))
		}

		if _, ok := columDatatypes[tablename]; !ok {
			columDatatypes[tablename] = make(map[string]string)
			tableColumns[tablename] = make([]string, 0, len(metric.FieldList())+len(metric.TagList())+1)
		}

		if _, ok := tableLengths[tablename]; !ok {
			tableLengths[tablename] = 0
		}

		metricEntry := make(map[string]interface{})

		metricEntry["timestamp"] = metric.Time()
		columDatatypes[tablename]["timestamp"] = p.deriveDatatype(metric.Time())
		tableColumns[tablename] = append(tableColumns[tablename], "timestamp")

		for _, tag := range metric.TagList() {
			metricEntry[tag.Key] = tag.Value
			columDatatypes[tablename][tag.Key] = p.deriveDatatype(tag.Value)
			tableColumns[tablename] = append(tableColumns[tablename], tag.Key)
		}

		for _, field := range metric.FieldList() {
			metricEntry[field.Key] = field.Value
			columDatatypes[tablename][field.Key] = p.deriveDatatype(field.Value)
			tableColumns[tablename] = append(tableColumns[tablename], field.Key)
		}

		metricsByTable[tablename] = append(metricsByTable[tablename], metricEntry)
		tableLengths[tablename]++
	}

	for tablename, metrics := range metricsByTable {
		for {
			sql := p.generateInsert(tablename, tableColumns[tablename], tableLengths[tablename])
			values := make([]interface{}, 0, tableLengths[tablename]*len(columDatatypes[tablename]))

			tx, err := p.db.Begin()
			if err != nil {
				return fmt.Errorf("begin failed: %w", err)
			}

			stmt, err := tx.Prepare(sql)
			if err != nil {
				if p.isUnknownTableErr(err) {
					createTableStmt := p.generateCreateTable(tablename, tableColumns[tablename], columDatatypes[tablename])

					_, err = p.db.Exec(createTableStmt)
					if err != nil {
						return fmt.Errorf("CREATE TABLE failed: %w", err)
					}
					continue
				}

				if p.isNoSuchColumnInTableErr(err) {
					alterTableStmt := p.generateAlterTable(tablename, tableColumns[tablename], columDatatypes[tablename])
					_, err = p.db.Exec(alterTableStmt)
					if err != nil {
						return fmt.Errorf("ALTER TABLE failed: %w", err)
					}
					continue
				}

				return fmt.Errorf("prepare failed: %w", err)
			}
			defer stmt.Close()

			for _, metric := range metrics {
				for _, column := range tableColumns[tablename] {
					values = append(values, metric[column])
				}
			}

			_, err = stmt.Exec(values...)
			if err != nil {
				return fmt.Errorf("exec failed: %w", err)
			}

			err = tx.Commit()
			if err != nil {
				return fmt.Errorf("commit failed: %w", err)
			}

			break
		}
	}
	return nil
}

func init() {
	outputs.Add("clickhouse", func() telegraf.Output {
		return &ClickHouse{
			ConnectionMaxIdle: 2,
		}
	})
}

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

type SQL struct {
	DataSourceName        string
	TimestampColumn       string
	InitSQL               string `toml:"init_sql"`
	ConnectionMaxIdleTime config.Duration
	ConnectionMaxLifetime config.Duration
	ConnectionMaxIdle     int
	ConnectionMaxOpen     int

	db  *gosql.DB
	Log telegraf.Logger `toml:"-"`
}

func (*SQL) SampleConfig() string {
	return sampleConfig
}

func (p *SQL) Connect() error {
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

	return nil
}

func (p *SQL) Close() error {
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

func (p *SQL) deriveDatatype(value interface{}) string {
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
	default:
		datatype = "String"
	}
	return datatype
}

func (p *SQL) generateCreateTable(tablename string, columns map[string]struct{}) string {
	columnDefs := make([]string, 0, len(columns))

	for column := range columns {
		dataType := p.deriveDatatype(column)
		columnDefs = append(columnDefs, fmt.Sprintf("%s %s", quoteIdent(column), dataType))
	}

	return fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s (%s)",
		quoteIdent(tablename),
		strings.Join(columnDefs, ","))
}

func (p *SQL) generateAlterTable(tablename string, columns map[string]struct{}) string {
	alterDefs := make([]string, 0, len(columns))

	for column := range columns {
		dataType := p.deriveDatatype(column)
		alterDefs = append(alterDefs, fmt.Sprintf("ADD COLUMN IF NOT EXISTS %s %s",
			quoteIdent(column), dataType))
	}

	return fmt.Sprintf("ALTER TABLE %s %s",
		quoteIdent(tablename),
		strings.Join(alterDefs, ","))
}

func (p *SQL) generateInsert(tablename string, columns map[string]struct{}, batchSize int) string {
	quotedColumns := make([]string, 0, len(columns))
	for column := range columns {
		quotedColumns = append(quotedColumns, quoteIdent(column))
	}

	placeholder := "(" + strings.Repeat("?,", len(columns)-1) + "?)"
	placeholders := strings.Repeat(placeholder+",", batchSize-1) + placeholder

	return fmt.Sprintf("INSERT INTO %s(%s) VALUES %s",
		quoteIdent(tablename),
		strings.Join(quotedColumns, ","),
		placeholders)
}

func (p *SQL) isUnknownTableErr(err error) bool {
	if err == nil {
		return false
	}

	return strings.Contains(err.Error(), "UNKNOWN_TABLE")
}

func (p *SQL) isNoSuchColumnInTableErr(err error) bool {
	if err == nil {
		return false
	}

	return strings.Contains(err.Error(), "NO_SUCH_COLUMN_IN_TABLE")
}

func (p *SQL) Write(metrics []telegraf.Metric) error {
	metricsByTable := make(map[string][]telegraf.Metric)
	tableLengths := make(map[string]int)
	tableColumns := make(map[string]map[string]struct{})

	for _, metric := range metrics {
		tablename := metric.Name()
		metricsByTable[tablename] = append(metricsByTable[tablename], metric)
		tableLengths[tablename]++

		for _, tag := range metric.TagList() {
			if _, ok := tableColumns[tablename]; !ok {
				tableColumns[tablename] = make(map[string]struct{})
			}
			tableColumns[tablename][tag.Key] = struct{}{}
		}

		for _, field := range metric.FieldList() {
			if _, ok := tableColumns[tablename]; !ok {
				tableColumns[tablename] = make(map[string]struct{})
			}
			tableColumns[tablename][field.Key] = struct{}{}
		}
	}

	for tablename, tableMetrics := range metricsByTable {
		for {
			sql := p.generateInsert(tablename, tableColumns[tablename], tableLengths[tablename])
			values := make([]interface{}, 0, len(tableMetrics)*len(tableColumns[tablename]))

			tx, err := p.db.Begin()
			if err != nil {
				return fmt.Errorf("begin failed: %w", err)
			}

			stmt, err := tx.Prepare(sql)
			if err != nil {
				return fmt.Errorf("prepare failed: %w", err)
			}
			defer stmt.Close()

			_, err = stmt.Exec(values...)
			if err != nil {
				if p.isUnknownTableErr(err) {
					createTableStmt := p.generateCreateTable(tablename, tableColumns[tablename])
					_, err = p.db.Exec(createTableStmt)
					if err != nil {
						return fmt.Errorf("create table failed: %w", err)
					}
					continue
				}

				if p.isNoSuchColumnInTableErr(err) {
					alterTableStmt := p.generateAlterTable(tablename, tableColumns[tablename])
					_, err = p.db.Exec(alterTableStmt)
					if err != nil {
						return fmt.Errorf("alter table failed: %w", err)
					}
					continue
				}

				return fmt.Errorf("execution failed: %w", err)
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
	outputs.Add("sql", func() telegraf.Output { return newSQL() })
}

func newSQL() *SQL {
	return &SQL{
		TimestampColumn: "timestamp",
		// Defaults for the connection settings (ConnectionMaxIdleTime,
		// ConnectionMaxLifetime, ConnectionMaxIdle, and ConnectionMaxOpen)
		// mirror the golang defaults. As of go 1.18 all of them default to 0
		// except max idle connections which is 2. See
		// https://pkg.go.dev/database/sql#DB.SetMaxIdleConns
		ConnectionMaxIdle: 2,
	}
}

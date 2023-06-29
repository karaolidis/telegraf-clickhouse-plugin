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
	"github.com/wk8/go-ordered-map/v2"
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

	db    *gosql.DB
	Log   telegraf.Logger `toml:"-"`
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

func sanitizeQuoted(in string) string {
	// https://dev.mysql.com/doc/refman/8.0/en/identifiers.html
	// https://www.postgresql.org/docs/13/sql-syntax-lexical.html#SQL-SYNTAX-IDENTIFIERS
	return strings.Map(func(r rune) rune {
		switch {
		case r >= '\u0001' && r <= '\uFFFF':
			return r
		default:
			return '_'
		}
	}, in)
}

func quoteIdent(name string) string {
	return `"` + strings.ReplaceAll(sanitizeQuoted(name), `"`, `""`) + `"`
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

func (p *ClickHouse) generateCreateTable(tablename string, columns *orderedmap.OrderedMap[string, string]) string {
	columnDefs := make([]string, 0, columns.Len())

	for pair := columns.Oldest(); pair != nil; pair = pair.Next() {
		columnDefs = append(columnDefs, fmt.Sprintf("%s %s", quoteIdent(pair.Key), pair.Value))
	}

	orderBy := make([]string, 0, 3)
	if _, ok := columns.Get("host"); ok {
		orderBy = append(orderBy, "host")
	}
	orderBy = append(orderBy, quoteIdent(p.TimestampColumn))
	if _, ok := columns.Get("measurement"); ok {
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

func (p *ClickHouse) generateAlterTable(tablename string, columns *orderedmap.OrderedMap[string, string]) string {
	alterDefs := make([]string, 0, columns.Len())

	for pair := columns.Oldest(); pair != nil; pair = pair.Next() {
		alterDefs = append(alterDefs, fmt.Sprintf("ADD COLUMN IF NOT EXISTS %s %s",
			quoteIdent(pair.Key),
			pair.Value))
	}

	return fmt.Sprintf("ALTER TABLE %s %s",
		quoteIdent(tablename),
		strings.Join(alterDefs, ","))
}

func (p *ClickHouse) ensureTable(tablename string, columns *orderedmap.OrderedMap[string, string]) error {
	var res *gosql.Rows
	var err error

	for {
		res, err = p.db.Query(fmt.Sprintf("DESCRIBE TABLE %s", quoteIdent(tablename)))

		if err != nil {
			// Unknown Table Error
			if strings.Contains(err.Error(), "code: 60") {
				_, err = p.db.Exec(p.generateCreateTable(tablename, columns))
				if err != nil {
					return err
				}
				fmt.Println("Created table", tablename)
				continue
			}
			return err
		}

		defer res.Close()
		break
	}

	tableColumns := make(map[string]struct{})
	for res.Next() {
		var name string
		var _i string

		err = res.Scan(&name, &_i, &_i, &_i, &_i, &_i, &_i)
		if err != nil {
			return err
		}

		tableColumns[name] = struct{}{}
	}

	for pair := columns.Oldest(); pair != nil; pair = pair.Next() {
		if _, ok := tableColumns[pair.Key]; !ok {
			_, err = p.db.Exec(p.generateAlterTable(tablename, columns))
			if err != nil {
				return err
			}
			fmt.Println("Altered table", tablename)
			break
		}
	}

	return nil
}

func (p *ClickHouse) generateInsert(tablename string, columns *orderedmap.OrderedMap[string, string], batchSize int) string {
	quotedColumns := make([]string, 0, columns.Len())
	for pair := columns.Oldest(); pair != nil; pair = pair.Next() {
		quotedColumns = append(quotedColumns, quoteIdent(pair.Key))
	}

	placeholder := "(" + strings.Repeat("?,", columns.Len()-1) + "?)"
	placeholders := strings.Repeat(placeholder+",", batchSize-1) + placeholder

	return fmt.Sprintf("INSERT INTO %s(%s) VALUES %s",
		quoteIdent(tablename),
		strings.Join(quotedColumns, ", "),
		placeholders)
}

func (p *ClickHouse) writeMetrics(tablename string, columns *orderedmap.OrderedMap[string, string], metrics []map[string]interface{}) error {
	err := p.ensureTable(tablename, columns)
	if err != nil {
		return err
	}

	sql := p.generateInsert(tablename, columns, len(metrics))
	values := make([]interface{}, 0, len(metrics)*columns.Len())

	tx, err := p.db.Begin()
	if err != nil {
		return fmt.Errorf("begin failed: %w", err)
	}

	stmt, err := tx.Prepare(sql)
	if err != nil {
		return fmt.Errorf("prepare failed: %w", err)
	}
	defer stmt.Close()

	for _, metric := range metrics {
		for pair := columns.Oldest(); pair != nil; pair = pair.Next() {
			values = append(values, metric[pair.Key])
		}
	}

	start := time.Now()
	_, err = stmt.Exec(values...)
	if err != nil {
		return fmt.Errorf("exec failed: %w", err)
	}
	p.Log.Debugf("Wrote %d metrics to %s in %s", len(metrics), tablename, time.Since(start))

	err = tx.Commit()
	if err != nil {
		return fmt.Errorf("commit failed: %w", err)
	}
	return nil
}

func (p *ClickHouse) WriteMultiTable(metrics []telegraf.Metric) error {
	metricsData := make(map[string][]map[string]interface{})
	columns := make(map[string]*orderedmap.OrderedMap[string, string])

	start := time.Now()
	for _, metric := range metrics {
		tablename := metric.Name()

		if p.MultiTableOptions.TablePrefix != "" {
			tablename = p.MultiTableOptions.TablePrefix + "_" + tablename
		}

		if _, ok := metricsData[tablename]; !ok {
			metricsData[tablename] = make([]map[string]interface{}, 0, len(metrics))
		}

		if _, ok := columns[tablename]; !ok {
			columns[tablename] = orderedmap.New[string, string](len(metrics))
		}

		metricEntry := make(map[string]interface{})

		metricEntry[p.TimestampColumn] = metric.Time()
		columns[tablename].Set(p.TimestampColumn, p.deriveDatatype(metric.Time()))

		for _, tag := range metric.TagList() {
			metricEntry[tag.Key] = tag.Value
			columns[tablename].Set(tag.Key, p.deriveDatatype(tag.Value))
		}

		for _, field := range metric.FieldList() {
			metricEntry[field.Key] = field.Value
			columns[tablename].Set(field.Key, p.deriveDatatype(field.Value))
		}

		metricsData[tablename] = append(metricsData[tablename], metricEntry)
	}
	p.Log.Debugf("Prepared %d metrics for writing in %s", len(metrics), time.Since(start))

	start = time.Now()
	for tablename, metrics := range metricsData {
		err := p.writeMetrics(tablename, columns[tablename], metrics)
		if err != nil {
			return err
		}
	}
	p.Log.Debugf("Wrote %d metrics to %d tables in %s", len(metrics), len(metricsData), time.Since(start))

	return nil
}

func (p *ClickHouse) WriteSingleTable(metrics []telegraf.Metric) error {
	tablename := p.SingleTableOptions.TableName
	metricsData := make([]map[string]interface{}, 0, len(metrics))
	columns := orderedmap.New[string, string](len(metrics))

	start := time.Now()
	for _, metric := range metrics {
		metricName := metric.Name()

		metricEntry := make(map[string]interface{})
		metricEntry[p.TimestampColumn] = metric.Time()
		columns.Set(p.TimestampColumn, p.deriveDatatype(metric.Time()))

		metricEntry["measurement"] = metricName
		columns.Set("measurement", p.deriveDatatype(metricName))

		for _, tag := range metric.TagList() {
			colName := fmt.Sprintf("%s_%s", metricName, tag.Key)
			metricEntry[colName] = tag.Value
			columns.Set(colName, p.deriveDatatype(tag.Value))
		}

		for _, field := range metric.FieldList() {
			colName := fmt.Sprintf("%s_%s", metricName, field.Key)
			metricEntry[colName] = field.Value
			columns.Set(colName, p.deriveDatatype(field.Value))
		}

		metricsData = append(metricsData, metricEntry)
	}
	p.Log.Debugf("Prepared %d metrics for writing in %s", len(metrics), time.Since(start))

	start = time.Now()
	err := p.writeMetrics(tablename, columns, metricsData)
	if err != nil {
		return err
	}
	p.Log.Debugf("Wrote %d metrics to %s in %s", len(metrics), tablename, time.Since(start))

	return nil
}

func (p *ClickHouse) Write(metrics []telegraf.Metric) error {
	if p.TableMode == "single" {
		return p.WriteSingleTable(metrics)
	}

	return p.WriteMultiTable(metrics)
}

func init() {
	outputs.Add("clickhouse", func() telegraf.Output {
		return &ClickHouse{
			ConnectionMaxIdle: 2,
		}
	})
}

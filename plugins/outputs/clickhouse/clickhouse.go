//go:generate ../../../tools/readme_config_includer/generator
package sql

import (
	"database/sql"
	_ "embed"
	"fmt"
	"math"
	"strings"
	"sync"
	"time"

	_ "github.com/ClickHouse/clickhouse-go"

	"github.com/influxdata/telegraf"
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
	QueueSize             int                `toml:"queue_size"`
	QueueLimit            int                `toml:"queue_limit"`
	FlushInterval         time.Duration      `toml:"flush_interval"`
	ConnectionMaxIdleTime time.Duration      `toml:"connection_max_idle_time"`
	ConnectionMaxLifetime time.Duration      `toml:"connection_max_lifetime"`
	ConnectionMaxIdle     int                `toml:"connection_max_idle"`
	ConnectionMaxOpen     int                `toml:"connection_max_open"`

	db            *sql.DB
	Log           telegraf.Logger
	metricQueue   []telegraf.Metric
	metricLock    sync.Mutex
	metricTrigger chan struct{}
}

func (*ClickHouse) SampleConfig() string {
	return sampleConfig
}

func (p *ClickHouse) Init() error {
	if p.DataSourceName == "" {
		return fmt.Errorf("data_source_name is a required configuration option")
	}

	if p.TimestampColumn == "" {
		p.TimestampColumn = "timestamp"
		p.Log.Info("timestamp_column is not set, using default value:", p.TimestampColumn)
	}

	if p.TableMode == "" {
		p.TableMode = "multi"
		p.Log.Info("table_mode is not set, using default value:", p.TableMode)
	} else if p.TableMode != "single" && p.TableMode != "multi" {
		return fmt.Errorf("table_mode must be one of: single, multi")
	}

	if p.TableMode == "single" && p.SingleTableOptions.TableName == "" {
		p.SingleTableOptions.TableName = "telegraf"
		p.Log.Info("table_name is not set, using default value:", p.SingleTableOptions.TableName)
	}

	if p.QueueSize <= 0 {
		p.QueueSize = 100000
		p.Log.Info("queue_size is not set, using default value:", p.QueueSize)
	}

	if p.QueueLimit <= 0 {
		p.QueueLimit = int(math.MaxUint64 >> 1)
		p.Log.Info("queue_limit is not set, using default value:", p.QueueLimit)
	}

	if p.FlushInterval <= 0 {
		p.FlushInterval = 5 * time.Second
		p.Log.Info("flush_interval is not set, using default value:", p.FlushInterval)
	}

	p.metricQueue = make([]telegraf.Metric, 0, p.QueueSize)
	p.metricTrigger = make(chan struct{}, 1)

	go p.metricWriter(p.FlushInterval)

	return nil
}

func (p *ClickHouse) Connect() error {
	db, err := sql.Open("clickhouse", p.DataSourceName)
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
	p.Log.Info("Connected to ClickHouse!")

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

func (p *ClickHouse) valueToDatatype(value interface{}) string {
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

// TODO: fix this fuckyness
type NullUint64 struct {
	Uint64 uint64
	Valid  bool
}

func (p *ClickHouse) datatypeToNullable(datatype string) interface{} {
	var nullable interface{}

	switch datatype {
	case "Int64":
		nullable = int64(0) // sql.NullInt64{}
	case "UInt64":
		nullable = uint64(0) // NullUint64{}
	case "Float64":
		nullable = float64(0) // sql.NullFloat64{}
	case "String":
		nullable = "" // sql.NullString{}
	case "UInt8":
		nullable = false // sql.NullBool{}
	case "DateTime":
		nullable = time.Unix(0, 0) // sql.NullTime{}
	default:
		nullable = "" // sql.NullString{}
		p.Log.Errorf("unknown datatype %s", datatype)
	}

	return nullable
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
	var res *sql.Rows
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
				p.Log.Info("Created table", tablename)
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
			p.Log.Info("Altered table", tablename)
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

	return fmt.Sprintf("INSERT INTO %s (%s) VALUES %s",
		quoteIdent(tablename),
		strings.Join(quotedColumns, ", "),
		placeholders)
}

func (p *ClickHouse) nullifyMissingValues(columns *orderedmap.OrderedMap[string, string], metrics []map[string]interface{}) {
	for _, metric := range metrics {
		for pair := columns.Oldest(); pair != nil; pair = pair.Next() {
			if _, ok := metric[pair.Key]; !ok {
				metric[pair.Key] = p.datatypeToNullable(pair.Value)
			}
		}
	}
}

func (p *ClickHouse) writeMetrics(tablename string, columns *orderedmap.OrderedMap[string, string], metrics []map[string]interface{}) error {
	err := p.ensureTable(tablename, columns)
	if err != nil {
		return err
	}

	sql := p.generateInsert(tablename, columns, len(metrics))

	tx, err := p.db.Begin()
	if err != nil {
		return fmt.Errorf("begin failed: %w", err)
	}

	stmt, err := tx.Prepare(sql)
	if err != nil {
		return fmt.Errorf("prepare failed: %w", err)
	}
	defer stmt.Close()

	values := make([][]interface{}, 0, len(metrics))

	for _, metric := range metrics {
		value := make([]interface{}, 0, columns.Len())
		for pair := columns.Oldest(); pair != nil; pair = pair.Next() {
			value = append(value, metric[pair.Key])
		}
		values = append(values, value)
	}

	for _, value := range values {
		_, err = stmt.Exec(value...)
		if err != nil {
			return fmt.Errorf("exec failed: %w", err)
		}
	}
	if err != nil {
		return fmt.Errorf("exec failed: %w", err)
	}

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
		columns[tablename].Set(p.TimestampColumn, p.valueToDatatype(metric.Time()))

		for _, tag := range metric.TagList() {
			metricEntry[tag.Key] = tag.Value
			columns[tablename].Set(tag.Key, p.valueToDatatype(tag.Value))
		}

		for _, field := range metric.FieldList() {
			metricEntry[field.Key] = field.Value
			columns[tablename].Set(field.Key, p.valueToDatatype(field.Value))
		}

		metricsData[tablename] = append(metricsData[tablename], metricEntry)
	}

	for tablename, metrics := range metricsData {
		p.nullifyMissingValues(columns[tablename], metrics)
	}
	p.Log.Infof("Prepared %d metrics for writing in %s\n", len(metrics), time.Since(start))

	start = time.Now()
	for tablename, metrics := range metricsData {
		err := p.writeMetrics(tablename, columns[tablename], metrics)
		if err != nil {
			return err
		}
	}
	p.Log.Infof("Wrote %d metrics to %d tables in %s\n", len(metrics), len(metricsData), time.Since(start))

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
		columns.Set(p.TimestampColumn, p.valueToDatatype(metric.Time()))

		metricEntry["measurement"] = metricName
		columns.Set("measurement", p.valueToDatatype(metricName))

		for _, tag := range metric.TagList() {
			colName := fmt.Sprintf("%s_%s", metricName, tag.Key)
			metricEntry[colName] = tag.Value
			columns.Set(colName, p.valueToDatatype(tag.Value))
		}

		for _, field := range metric.FieldList() {
			colName := fmt.Sprintf("%s_%s", metricName, field.Key)
			metricEntry[colName] = field.Value
			columns.Set(colName, p.valueToDatatype(field.Value))
		}

		for pair := columns.Oldest(); pair != nil; pair = pair.Next() {
			if _, ok := metricEntry[pair.Key]; !ok {
				metricEntry[pair.Key] = p.datatypeToNullable(pair.Value)
			}
		}

		metricsData = append(metricsData, metricEntry)
	}

	p.nullifyMissingValues(columns, metricsData)
	p.Log.Infof("Prepared %d metrics for writing in %s\n", len(metrics), time.Since(start))

	start = time.Now()
	err := p.writeMetrics(tablename, columns, metricsData)
	if err != nil {
		return err
	}
	p.Log.Infof("Wrote %d metrics to %s in %s\n", len(metrics), tablename, time.Since(start))

	return nil
}

func (p *ClickHouse) Write(metrics []telegraf.Metric) error {
	p.metricLock.Lock()
	if len(p.metricQueue) >= p.QueueLimit {
		p.Log.Errorf("Metrics queue is full (%d/%d), dropping metrics", len(p.metricQueue), p.QueueLimit)
	} else {
		p.metricQueue = append(p.metricQueue, metrics...)
	}
	p.metricLock.Unlock()

	select {
	case p.metricTrigger <- struct{}{}:
	default:
	}

	return nil
}

func (p *ClickHouse) metricWriter(delay time.Duration) {
	timer := time.NewTimer(delay)
	defer timer.Stop()

	for {
		select {
		case <-timer.C:
			p.metricLock.Lock()
			metrics := p.metricQueue
			p.metricQueue = nil
			p.metricLock.Unlock()

			if len(metrics) > 0 {
				if p.TableMode == "single" {
					err := p.WriteSingleTable(metrics)
					if err != nil {
						p.Log.Errorf("Error writing to ClickHouse: %s", err)
					}
				} else {
					err := p.WriteMultiTable(metrics)
					if err != nil {
						p.Log.Errorf("Error writing to ClickHouse: %s", err)
					}
				}
			}

			timer.Reset(delay)

		case <-p.metricTrigger:
			if !timer.Stop() {
				<-timer.C
			}
			timer.Reset(delay)
		}
	}
}

func init() {
	outputs.Add("clickhouse", func() telegraf.Output {
		return &ClickHouse{
			ConnectionMaxIdle: 2,
		}
	})
}

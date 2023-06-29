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

func (ch *ClickHouse) Init() error {
	if ch.DataSourceName == "" {
		return fmt.Errorf("data_source_name is a required configuration option")
	}

	if ch.TimestampColumn == "" {
		ch.TimestampColumn = "timestamp"
		ch.Log.Info("timestamp_column is not set, using default value: ", ch.TimestampColumn)
	}

	if ch.TableMode == "" {
		ch.TableMode = "multi"
		ch.Log.Info("table_mode is not set, using default value: ", ch.TableMode)
	} else if ch.TableMode != "single" && ch.TableMode != "multi" {
		return fmt.Errorf("table_mode must be one of: single, multi")
	}

	if ch.TableMode == "single" && ch.SingleTableOptions.TableName == "" {
		ch.SingleTableOptions.TableName = "telegraf"
		ch.Log.Info("table_name is not set, using default value: ", ch.SingleTableOptions.TableName)
	}

	if ch.QueueSize <= 0 {
		ch.QueueSize = 100000
		ch.Log.Info("queue_size is not set, using default value: ", ch.QueueSize)
	}

	if ch.QueueLimit <= 0 {
		ch.QueueLimit = int(math.MaxUint64 >> 1)
		ch.Log.Info("queue_limit is not set, using default value: ", ch.QueueLimit)
	}

	if ch.FlushInterval <= 0 {
		ch.FlushInterval = 5 * time.Second
		ch.Log.Info("flush_interval is not set, using default value: ", ch.FlushInterval)
	}

	ch.metricQueue = make([]telegraf.Metric, 0, ch.QueueSize)
	ch.metricTrigger = make(chan struct{}, 1)

	go ch.backgroundWriter(ch.FlushInterval)

	return nil
}

func (ch *ClickHouse) Connect() error {
	db, err := sql.Open("clickhouse", ch.DataSourceName)
	if err != nil {
		return err
	}

	err = db.Ping()
	if err != nil {
		return err
	}

	db.SetConnMaxIdleTime(time.Duration(ch.ConnectionMaxIdleTime))
	db.SetConnMaxLifetime(time.Duration(ch.ConnectionMaxLifetime))
	db.SetMaxIdleConns(ch.ConnectionMaxIdle)
	db.SetMaxOpenConns(ch.ConnectionMaxOpen)

	if ch.InitSQL != "" {
		_, err = db.Exec(ch.InitSQL)
		if err != nil {
			return err
		}
	}

	ch.db = db
	ch.Log.Info("Connected to ClickHouse!")

	return nil
}

func (ch *ClickHouse) Close() error {
	return ch.db.Close()
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

func (ch *ClickHouse) valueToDatatype(value interface{}) string {
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
		ch.Log.Errorf("unknown datatype for value %v", value)
	}

	return datatype
}

func (ch *ClickHouse) generateCreateTable(tablename string, columns *orderedmap.OrderedMap[string, string]) string {
	columnDefs := make([]string, 0, columns.Len())

	for pair := columns.Oldest(); pair != nil; pair = pair.Next() {
		columnType := pair.Value
		if pair.Key != "host" && pair.Key != ch.TimestampColumn && pair.Key != "measurement" {
			columnType = fmt.Sprintf("Nullable(%s)", pair.Value)
		}

		columnDefs = append(columnDefs, fmt.Sprintf("%s %s", quoteIdent(pair.Key), columnType))
	}

	orderBy := make([]string, 0, 3)
	if _, ok := columns.Get("host"); ok {
		orderBy = append(orderBy, "host")
	}
	orderBy = append(orderBy, quoteIdent(ch.TimestampColumn))
	if _, ok := columns.Get("measurement"); ok {
		orderBy = append(orderBy, "measurement")
	}

	createTable := fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s (%s) ENGINE = MergeTree() ORDER BY (%s) PARTITION BY toYYYYMM(%s)",
		quoteIdent(tablename),
		strings.Join(columnDefs, ","),
		strings.Join(orderBy, ","),
		quoteIdent(ch.TimestampColumn))

	if ch.TTL != "" {
		createTable += fmt.Sprintf(" TTL %s + INTERVAL %s", quoteIdent(ch.TimestampColumn), ch.TTL)
	}

	return createTable
}

func (ch *ClickHouse) generateAlterTable(tablename string, columns *orderedmap.OrderedMap[string, string]) string {
	alterDefs := make([]string, 0, columns.Len())

	for pair := columns.Oldest(); pair != nil; pair = pair.Next() {
		columnType := pair.Value
		if pair.Key != "host" && pair.Key != ch.TimestampColumn && pair.Key != "measurement" {
			columnType = fmt.Sprintf("Nullable(%s)", pair.Value)
		}

		alterDefs = append(alterDefs, fmt.Sprintf("ADD COLUMN IF NOT EXISTS %s %s",
			quoteIdent(pair.Key),
			columnType))
	}

	return fmt.Sprintf("ALTER TABLE %s %s",
		quoteIdent(tablename),
		strings.Join(alterDefs, ","))
}

func (ch *ClickHouse) ensureTable(tablename string, columns *orderedmap.OrderedMap[string, string]) error {
	var res *sql.Rows
	var err error

	for {
		res, err = ch.db.Query(fmt.Sprintf("DESCRIBE TABLE %s", quoteIdent(tablename)))

		if err != nil {
			// Unknown Table Error
			if strings.Contains(err.Error(), "code: 60") {
				_, err = ch.db.Exec(ch.generateCreateTable(tablename, columns))
				if err != nil {
					return err
				}
				ch.Log.Info("Created table: ", tablename)
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
			_, err = ch.db.Exec(ch.generateAlterTable(tablename, columns))
			if err != nil {
				return err
			}
			ch.Log.Info("Altered table: ", tablename)
			break
		}
	}

	return nil
}

func (ch *ClickHouse) generateInsert(tablename string, columns *orderedmap.OrderedMap[string, string], batchSize int) string {
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

func (ch *ClickHouse) writeMetrics(tablename string, columns *orderedmap.OrderedMap[string, string], metrics []map[string]interface{}) error {
	err := ch.ensureTable(tablename, columns)
	if err != nil {
		return err
	}

	sql := ch.generateInsert(tablename, columns, len(metrics))

	tx, err := ch.db.Begin()
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

func (ch *ClickHouse) WriteMultiTable(metrics []telegraf.Metric) error {
	metricsData := make(map[string][]map[string]interface{})
	columns := make(map[string]*orderedmap.OrderedMap[string, string])

	start := time.Now()
	for _, metric := range metrics {
		tablename := metric.Name()

		if ch.MultiTableOptions.TablePrefix != "" {
			tablename = ch.MultiTableOptions.TablePrefix + "_" + tablename
		}

		if _, ok := metricsData[tablename]; !ok {
			metricsData[tablename] = make([]map[string]interface{}, 0, len(metrics))
		}

		if _, ok := columns[tablename]; !ok {
			columns[tablename] = orderedmap.New[string, string](len(metrics))
		}

		metricEntry := make(map[string]interface{})

		metricEntry[ch.TimestampColumn] = metric.Time()
		columns[tablename].Set(ch.TimestampColumn, ch.valueToDatatype(metric.Time()))

		for _, tag := range metric.TagList() {
			metricEntry[tag.Key] = tag.Value
			columns[tablename].Set(tag.Key, ch.valueToDatatype(tag.Value))
		}

		for _, field := range metric.FieldList() {
			metricEntry[field.Key] = field.Value
			columns[tablename].Set(field.Key, ch.valueToDatatype(field.Value))
		}

		metricsData[tablename] = append(metricsData[tablename], metricEntry)
	}
	ch.Log.Infof("Prepared %d metrics for writing in %s\n", len(metrics), time.Since(start))

	start = time.Now()
	for tablename, metrics := range metricsData {
		err := ch.writeMetrics(tablename, columns[tablename], metrics)
		if err != nil {
			return err
		}
	}
	ch.Log.Infof("Wrote %d metrics to %d tables in %s\n", len(metrics), len(metricsData), time.Since(start))

	return nil
}

func (ch *ClickHouse) WriteSingleTable(metrics []telegraf.Metric) error {
	tablename := ch.SingleTableOptions.TableName
	metricsData := make([]map[string]interface{}, 0, len(metrics))
	columns := orderedmap.New[string, string](len(metrics))

	start := time.Now()
	for _, metric := range metrics {
		metricName := metric.Name()

		metricEntry := make(map[string]interface{})
		metricEntry[ch.TimestampColumn] = metric.Time()
		columns.Set(ch.TimestampColumn, ch.valueToDatatype(metric.Time()))

		metricEntry["measurement"] = metricName
		columns.Set("measurement", ch.valueToDatatype(metricName))

		for _, tag := range metric.TagList() {
			colName := fmt.Sprintf("%s_%s", metricName, tag.Key)
			metricEntry[colName] = tag.Value
			columns.Set(colName, ch.valueToDatatype(tag.Value))
		}

		for _, field := range metric.FieldList() {
			colName := fmt.Sprintf("%s_%s", metricName, field.Key)
			metricEntry[colName] = field.Value
			columns.Set(colName, ch.valueToDatatype(field.Value))
		}

		for pair := columns.Oldest(); pair != nil; pair = pair.Next() {
			if _, ok := metricEntry[pair.Key]; !ok {
				metricEntry[pair.Key] = pair.Value
			}
		}

		metricsData = append(metricsData, metricEntry)
	}
	ch.Log.Infof("Prepared %d metrics for writing in %s\n", len(metrics), time.Since(start))

	start = time.Now()
	err := ch.writeMetrics(tablename, columns, metricsData)
	if err != nil {
		return err
	}
	ch.Log.Infof("Wrote %d metrics to %s in %s\n", len(metrics), tablename, time.Since(start))

	return nil
}

func (ch *ClickHouse) Write(metrics []telegraf.Metric) error {
	ch.metricLock.Lock()
	if len(ch.metricQueue) >= ch.QueueLimit {
		ch.Log.Errorf("Metrics queue is full (%d/%d), dropping metrics", len(ch.metricQueue), ch.QueueLimit)
	} else {
		ch.metricQueue = append(ch.metricQueue, metrics...)
	}
	ch.metricLock.Unlock()

	select {
	case ch.metricTrigger <- struct{}{}:
	default:
	}

	return nil
}

func (ch *ClickHouse) backgroundWriter(delay time.Duration) {
	timer := time.NewTimer(delay)
	defer timer.Stop()

	for {
		select {
		case <-timer.C:
			ch.metricLock.Lock()
			metrics := ch.metricQueue
			ch.metricQueue = nil
			ch.metricLock.Unlock()

			if len(metrics) > 0 {
				if ch.TableMode == "single" {
					err := ch.WriteSingleTable(metrics)
					if err != nil {
						ch.Log.Error("Error writing to ClickHouse: ", err)
					}
				} else {
					err := ch.WriteMultiTable(metrics)
					if err != nil {
						ch.Log.Error("Error writing to ClickHouse: ", err)
					}
				}
			}

			timer.Reset(delay)

		case <-ch.metricTrigger:
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

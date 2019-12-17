package query

import (
	"database/sql"

	"kye-mask-data/mask/redis"

	"go.uber.org/zap"
)

var log *zap.Logger

// SetLogger set logger
func SetLogger(l *zap.Logger) {
	log = l
	redis.SetLogger(l)
}

// TableInfo table info
type TableInfo struct {
	DBName              string
	Name                string
	PrimaryKeyFirstData int64
	PrimaryKeyLastData  int64
	Fields              []string
	SQLDB               *sql.DB
}

type queryInfo struct {
	DBName              string
	TableName           string
	Fields              []string
	PrimaryKeyStartData int64
	LimitCount          int64
	SQLDB               *sql.DB
}

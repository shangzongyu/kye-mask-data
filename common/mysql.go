package common

import (
	"database/sql"
	"errors"
	"fmt"
	"log"
	"strings"
)

// MySQLTableInfo mysql table info
type MySQLTableInfo struct {
	Field   *string
	Type    *string
	Null    *string
	Key     *string
	Default *string
	Extra   *string
}

// GetTablePrimaryKeyFirstData get primary key first data
func GetTablePrimaryKeyFirstData(db *sql.DB, tableName, primaryKeyField string) (int64, error) {
	if db == nil {
		return -1, errors.New("GetTablePrimaryKeyFirstData db is nil")
	}

	rows, err := db.Query(fmt.Sprintf(`SELECT %s FROM %s ORDER BY %s LIMIT 1`,
		primaryKeyField, tableName, primaryKeyField))
	if err != nil {
		return 0, err
	}

	var primaryKey int64
	for rows.Next() {
		if err := rows.Scan(&primaryKey); err != nil {
			return 0, err
		}
		break
	}

	return primaryKey, nil
}

// GetTablePrimaryKeyLastData get primary key last data
func GetTablePrimaryKeyLastData(db *sql.DB, tableName, primaryKeyField string) (int64, error) {
	rows, err := db.Query(fmt.Sprintf(`SELECT %s FROM %s ORDER BY %s DESC LIMIT 1`,
		primaryKeyField, tableName, primaryKeyField))
	if err != nil {
		log.Printf("GetTablePrimaryKeyFirstData|mysqlDB.db.Query %s", err.Error())
		return 0, err
	}

	var primaryKey int64
	for rows.Next() {
		if err := rows.Scan(&primaryKey); err != nil {
			return 0, err
		}
		break
	}

	return primaryKey, nil
}

// showDBAllTables show db all tables
func showDBAllTables(db *sql.DB) ([]string, error) {
	if db == nil {
		return nil, errors.New("showDBAllTables db is nil")
	}

	rows, err := db.Query("SHOW TABLES")
	if err != nil {
		log.Printf("showDBAllTables|db.Query %s", err.Error())
		return nil, err
	}
	defer rows.Close()

	// fmt.Println(rows.Columns())
	var names []string
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			fmt.Printf("showDBAllTables|scan %s", err.Error())
			continue
		}

		names = append(names, name)
	}
	return names, nil
}

// GetDBAllTableInfos get db all tables info
func GetDBAllTableInfos(db *sql.DB) (map[string][]MySQLTableInfo, error) {
	if db == nil {
		return nil, errors.New("GetDBAllTableInfos db is nil")
	}

	tables, err := showDBAllTables(db)
	if err != nil {
		log.Printf("GetTablesInfos|ShowDBAllTables %s", err.Error())
		return nil, err
	}

	tablesInfo := make(map[string][]MySQLTableInfo)
	for _, tableName := range tables {
		tableInfos, err := DescDBTable(db, tableName)
		if err != nil {
			log.Printf("saveTables|ShowTables %s", err.Error())
			continue
		}

		if _, ok := tablesInfo[tableName]; !ok {
			tablesInfo[tableName] = make([]MySQLTableInfo, 0)
		}

		for _, info := range tableInfos {
			tablesInfo[tableName] = append(tablesInfo[tableName], info)
		}
	}

	return tablesInfo, nil
}

// DescDBTable desc table
func DescDBTable(db *sql.DB, tableName string) (map[string]MySQLTableInfo, error) {
	if db == nil {
		return nil, errors.New("descDBTable db is nil")
	}

	if tableName == "" {
		return nil, errors.New("descDBTable tableName is empty")
	}

	rows, err := db.Query(fmt.Sprintf("DESC %s", tableName))
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	datas := make(map[string]MySQLTableInfo)
	for rows.Next() {
		var info MySQLTableInfo
		if err := rows.Scan(&info.Field,
			&info.Type,
			&info.Null,
			&info.Key,
			&info.Default,
			&info.Extra); err != nil {
			continue
		}
		datas[*info.Field] = info
	}

	return datas, nil
}

// GetTableMaxCount get table max count
func GetTableMaxCount(db *sql.DB, tableName string) (int64, error) {
	if db == nil {
		return -1, errors.New("getTableMaxCount db is nil")
	}

	if tableName == "" {
		return -1, errors.New("getTableMaxCount tableName is empty")
	}

	sqlStr := fmt.Sprintf(`SELECT COUNT(*) FROM %s`, tableName)
	rows, err := db.Query(sqlStr)
	if err != nil {
		return -1, err
	}
	defer rows.Close()

	var maxCount int64
	if rows.Next() {
		if err := rows.Scan(&maxCount); err != nil {
			return 1, err
		}
	}

	return maxCount, nil
}

// GetTableMaxConnections get table max count
func GetTableMaxConnections(db *sql.DB) (int, error) {
	if db == nil {
		return 0, errors.New("getTableMaxCount db is nil")
	}

	rows, err := db.Query(`show variables like 'max_connection%'`)
	if err != nil {
		return 0, err
	}
	defer rows.Close()

	var maxCount int
	var maxField string
	for rows.Next() {
		if err := rows.Scan(&maxField, &maxCount); err != nil {
			return 0, err
		}

		if maxField == "max_connections" {
			break
		}
	}

	return maxCount, nil
}

// FindPrimaryKeyByTableName find Primary key
func FindPrimaryKeyByTableName(db *sql.DB, tableName string) (string, error) {
	if db == nil {
		return "", errors.New("findPrimaryKeyByTableName db is nil")
	}

	if tableName == "" {
		return "", errors.New("findPrimaryKeyByTableName tableName is empty")
	}

	infos, err := DescDBTable(db, tableName)
	if err != nil {
		return "", err
	}

	var primaryKey string
	for _, info := range infos {
		if strings.Contains(strings.ToUpper(*info.Key), "PRI") {
			primaryKey = *(info.Field)
			break
		}
	}

	return primaryKey, nil
}

package generator

import (
	"database/sql"
	"fmt"
	"io/ioutil"
	"regexp"
	"sort"
	"strings"
	"sync"

	"kye-mask-data/common"
	"kye-mask-data/config"

	_ "github.com/go-sql-driver/mysql"
)

type Config struct {
	confInfo       *config.Info
	exportFileName string
}

// Config generate config
func (c *Config) Run() error {
	for _, sourceDB := range c.confInfo.SourceConf {
		_ = sourceDB.DB.Close()
		sourceDB.DB = nil
	}
	infoBytes, err := json.MarshalIndent(c.confInfo, "", "    ")
	if err != nil {
		return err
	}

	log.Info(fmt.Sprintf("generate config(%s) success", c.exportFileName))
	return ioutil.WriteFile(c.exportFileName, infoBytes, 0664)
}

func (c *Config) Close() {
	fmt.Printf("generate config close db start\n")
	for _, sourceDB := range c.confInfo.SourceConf {
		if sourceDB == nil || sourceDB.DB == nil {
			continue
		}

		if err := sourceDB.DB.Close(); err != nil {
			fmt.Printf("generate config close db(%s), error(%s)", sourceDB.Name, err.Error())
		} else {
			fmt.Printf("generate config close db(%s) success", sourceDB.Name)
		}
	}
	fmt.Printf("generate config close db end\n")
}

// NewConfig new config
func NewConfig(info *config.GenerateDBFlag) (*Config, error) {
	if err := configParameterCheck(info); err != nil {
		return nil, err
	}
	dbNames, err := common.ScanFileByLine(info.DBFile)
	if err != nil {
		return nil, fmt.Errorf("open dbfile error %s", err.Error())
	}

	regexpSlice, err := common.ScanFileByLine(info.RegexpFile)
	if err != nil {
		return nil, fmt.Errorf("open regexp error %s", err.Error())
	}

	for _, reg := range regexpSlice {
		if _, err := regexp.Compile(reg); err != nil {
			return nil, err
		}
	}

	log.Info(fmt.Sprintf("generator config %#v", *info))
	log.Info(fmt.Sprintf("regex=%v", regexpSlice))

	confInfo := &config.Info{
		LogLevel:            "info",
		LogFile:             "",
		ChannelCount:        50000,
		QueryPrintLogCount:  10000,
		QueryPoolCount:      10,
		UpdatePoolCount:     1000,
		UpdatePrintLogCount: 10000,
		LimitCount:          10000,
		UserName:            info.Username,
		Password:            info.Password,
		Host:                info.Host,
		Port:                info.Port,
		RedisConf:           defaultRedisConfig(),
		SourceConf:          make([]*config.DB, 0),
	}

	for _, dbName := range dbNames {
		log.Info(fmt.Sprintf("generator config dbname(%s) start", dbName))
		dbConfig := &config.DB{
			Name: dbName,
		}

		db, err := sql.Open("mysql", fmt.Sprintf("%s:%s@(%s:%d)/%s",
			info.Username, info.Password, info.Host, info.Port, dbName))
		if err != nil {
			panic(err)
		}

		if confInfo.MaxIdleConns == 0 {
			maxCount, err := common.GetTableMaxConnections(db)
			if err == nil && maxCount > 0 {
				confInfo.MaxOpenConns = maxCount
				confInfo.MaxIdleConns = maxCount / 2
				log.Info(fmt.Sprintf("newTestData get max connections %v)", maxCount))
			} else {
				confInfo.MaxOpenConns = 200
				confInfo.MaxIdleConns = 100
				log.Error(fmt.Sprintf("newTestData get max connections error(%s)", err.Error()))
			}
		}

		db.SetMaxOpenConns(confInfo.MaxOpenConns)
		db.SetMaxIdleConns(confInfo.MaxIdleConns)
		dbConfig.DB = db
		confInfo.SourceConf = append(confInfo.SourceConf, dbConfig)
	}

	for _, sourceConfig := range confInfo.SourceConf {
		allTableInfos, err := common.GetDBAllTableInfos(sourceConfig.DB)
		if err != nil {
			panic(err)
		}

		log.Info(fmt.Sprintf("generator config dbname(%s) get all tables count(%d)", sourceConfig.Name, len(allTableInfos)))
		var mutex sync.Mutex
		tablesFilterFields := make(map[string][]string, 0)
		for tableName, tableInfo := range allTableInfos {
			mutex.Lock()
			tablesFilterFields[tableName] = make([]string, 0)
			mutex.Unlock()

			log.Info(fmt.Sprintf("generator config dbname(%s) table(%s) process ...", sourceConfig.Name, tableName))
			var fieldWG sync.WaitGroup
			for _, fieldInfo := range tableInfo {
				fieldName := *fieldInfo.Field
				if strings.Contains(strings.ToUpper(*fieldInfo.Key), "PRI") {
					continue
				}
				fieldWG.Add(1)

				go func(tableName string, fieldName string, fieldWG *sync.WaitGroup) {
					defer fieldWG.Done()

					var from int64
					var totalCount float64
					var filedCount float64
					for true {
						var rowsCount int64
						sqlStr := fmt.Sprintf(`SELECT %s FROM %s WHERE %s!='' AND %s is NOT NULL LIMIT %v,%v`,
							fieldName, tableName, fieldName, fieldName, from, info.PerCount)
						rows, err := sourceConfig.DB.Query(sqlStr)
						if err != nil {
							log.Error(fmt.Sprintf("db.Query %s", err.Error()))
							return
						}

						for rows.Next() {
							var value interface{}
							if err := rows.Scan(&value); err != nil {
								log.Error(fmt.Sprintf("rows.Scan %s", err.Error()))
								continue
							}
							var fieldValue string
							switch value.(type) {
							case []byte:
								fieldValue = string(value.([]byte))
								// log.Println("[]byte", string(value.([]byte)))
							case *[]byte:
								fieldValue = string(*value.(*[]byte))
								// log.Println("*[]byte", value)
							case string:
								log.Info(fmt.Sprintf("string(%v)", fieldValue))
							case int:
								log.Info(fmt.Sprintf("int(%v)", fieldValue))
							}

							totalCount++
							rowsCount++
							if len(fieldValue) != 11 {
								continue
							}

							if common.ValidateRegular(fieldValue, regexpSlice) {
								filedCount++
							}
						}
						_ = rows.Close()

						log.Debug(fmt.Sprintf("table=%v,field=%v,from=%v,count=%v,fieldCount=%v,rowCount=%v,calcount=%v",
							tableName, fieldName, from, info.PerCount, filedCount, rowsCount, info.CalCount))
						if rowsCount < info.PerCount {
							break
						}

						if from+info.PerCount >= info.CalCount {
							break
						}

						from += info.PerCount
					}

					if totalCount == 0 {
						return
					}

					log.Info(fmt.Sprintf("dbname(%s)-table(%s)-field(%s)-percent(%0.2f),fieldCount=%v,totalCount=%v",
						sourceConfig.Name, tableName, fieldName, filedCount/totalCount, filedCount, totalCount))

					if filedCount/totalCount >= info.Percent {
						mutex.Lock()
						tablesFilterFields[tableName] = append(tablesFilterFields[tableName], fieldName)
						mutex.Unlock()
					}
				}(tableName, fieldName, &fieldWG)
			}
			fieldWG.Wait()
		}

		tablesConfig := make([]*config.Table, 0)
		for tableName, fields := range tablesFilterFields {
			if len(fields) > 0 {
				sort.Strings(fields)
				primaryKey, err := common.FindPrimaryKeyByTableName(sourceConfig.DB, tableName)
				if err != nil {
					return nil, fmt.Errorf("common.FindPrimaryKeyByTableName err(%s)", err.Error())
				}

				var tempFields []string
				tempFields = append(tempFields, primaryKey)
				tempFields = append(tempFields, fields...)
				tablesConfig = append(tablesConfig, &config.Table{
					Name:   tableName,
					Fields: tempFields,
				})
			}
		}

		sourceConfig.Tables = tablesConfig
	}

	return &Config{
		confInfo:       confInfo,
		exportFileName: info.FileName,
	}, nil
}

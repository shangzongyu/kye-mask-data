package query

import (
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"sync/atomic"
	"time"

	"kye-mask-data/common"
	"kye-mask-data/config"
	"kye-mask-data/mask/redis"

	_ "github.com/go-sql-driver/mysql"
	"github.com/panjf2000/ants"
)

// Data mysql query data
type Data struct {
	dbs          map[string][]*TableInfo
	pools        *ants.PoolWithFunc
	redis        *redis.Store
	channels     chan<- *common.ChannelData
	done         chan bool
	logCount     int64
	processCount int64
	limitCount   int64

	maskedCount           int64
	notMaskedCount        int64
	fieldCount            int64
	maskedNotReplaceCount int64
}

func (d *Data) processTable(queryData *queryInfo) ([]*common.ChannelData, error) {
	if queryData == nil {
		return nil, errors.New("queryData is nil")
	}

	primaryKeyField := queryData.Fields[0]
	sqlStr := fmt.Sprintf(`SELECT %s FROM %s WHERE %v>=%v LIMIT %v`,
		strings.Join(queryData.Fields, ","),
		queryData.TableName,
		primaryKeyField,
		queryData.PrimaryKeyStartData,
		queryData.LimitCount)
	rows, err := queryData.SQLDB.Query(sqlStr)
	if err != nil {
		log.Error(fmt.Sprintf("mysql db select db.Query %s" + err.Error()))
		return nil, err
	}
	defer rows.Close()

	resultDatas := make([]*common.ChannelData, 0)
	for rows.Next() {
		rowValues := make([]interface{}, len(queryData.Fields))
		for index := range rowValues {
			bytes := make([]byte, 0)
			rowValues[index] = &bytes
		}

		if err := rows.Scan(rowValues...); err != nil {
			log.Error(fmt.Sprintf("Select|rows.Scan %s", err.Error()))
			continue
		}

		requiredColumes := make(map[string]string)
		primaryKeyColumes := make(map[string]string)
		for index := 0; index < len(queryData.Fields); index++ {
			v := rowValues[index]
			var fieldValue string
			switch v.(type) {
			case *[]byte:
				fieldValue = string(*v.(*[]byte))
			case []byte:
				fieldValue = string(v.([]byte))
			}

			if index == 0 {
				primaryKeyColumes[queryData.Fields[index]] = fieldValue
			} else {
				if fieldValue != "" {
					requiredColumes[queryData.Fields[index]] = fieldValue
					atomic.AddInt64(&d.fieldCount, 1)
				}
			}
		}

		if len(requiredColumes) <= 0 {
			continue
		}

		resultDatas = append(resultDatas, &common.ChannelData{
			DBName:      queryData.DBName,
			TableName:   queryData.TableName,
			UpdateDatas: requiredColumes,
			Conditions:  primaryKeyColumes,
		})
	}

	return resultDatas, nil
}

// processOne MySQLQueryData Select query phone numbers
func (d *Data) processOne(inputData interface{}) error {
	if inputData == nil {
		return errors.New("inputData is nil")
	}
	channelDatas, ok := inputData.([]*common.ChannelData)
	if !ok {
		return errors.New("inputData is not type *queryInfo")
	}

	for _, channelData := range channelDatas {
		deleteField := make([]string, 0)
		for fieldName, sourceData := range channelData.UpdateDatas {
			if d.redis.IsMasked(sourceData) {
				deleteField = append(deleteField, sourceData)
				log.Debug(fmt.Sprintf("query db(%s)-table(%s)-field(%s)-value(%s)-IsMasked",
					channelData.DBName, channelData.TableName, fieldName, sourceData))
				atomic.AddInt64(&d.maskedCount, 1)
				continue
			}

			maskedData := d.redis.GetMaskedData(sourceData)
			if maskedData == "" {
				unusedData := d.redis.GetOneUnusedData()
				if err := d.redis.UpdateMaskedData(sourceData, unusedData); err != nil {
					log.Error(fmt.Sprintf("UpdateMaskedData(%s)", err.Error()))
				}
				maskedData = unusedData
				atomic.AddInt64(&d.notMaskedCount, 1)
			} else {
				atomic.AddInt64(&d.maskedNotReplaceCount, 1)
				log.Debug(fmt.Sprintf("UpdateMaskedData(source:%s:masked:%s)", sourceData, maskedData))
			}

			channelData.UpdateDatas[fieldName] = maskedData
		}

		for _, sourceData := range deleteField {
			delete(channelData.UpdateDatas, sourceData)
		}

		if len(channelData.UpdateDatas) <= 0 {
			continue
		}

		d.channels <- channelData
		go d.recordProcessCount()
	}

	return nil
}

func (d *Data) recordProcessCount() {
	atomic.AddInt64(&d.processCount, 1)

	if d.processCount%d.logCount == 0 {
		log.Info(fmt.Sprintf("+++ mysql query count(%d)-time(%v)", d.processCount, time.Now().Format("2006-01-02 15:04:05")))
	}
}

// Run run
func (d *Data) Run() {
	go func() {
		for _, dbInfo := range d.dbs {
			for _, table := range dbInfo {
				startIndex := table.PrimaryKeyFirstData
				for {
					resultData, err := d.processTable(&queryInfo{
						DBName:              table.DBName,
						TableName:           table.Name,
						Fields:              table.Fields[:],
						PrimaryKeyStartData: startIndex,
						LimitCount:          d.limitCount,
						SQLDB:               table.SQLDB,
					})
					if err != nil {
						log.Error(fmt.Sprintf("query db(%s)-table(%s)-start(%v)-end(%v)-err(%s)",
							table.DBName, table.Name, startIndex, d.limitCount, err.Error()))
						continue
					}

					if len(resultData) <= 0 {
						log.Info(fmt.Sprintf("==table:%s, dbname=%s，startIndex:%d, len(resultData)=%d, break",
							table.Name, table.DBName, startIndex, len(resultData)))
						break
					}

					if err := d.pools.Invoke(resultData); err != nil {
						log.Error(fmt.Sprintf("query db(%s)-table(%s)-start(%v)-end(%v)-err(%s)",
							table.DBName, table.Name, startIndex, d.limitCount, err.Error()))
						continue
					}

					log.Info(fmt.Sprintf("table:%s, dbname=%s，startIndex:%d, len(resultData) = %d",
						table.Name, table.DBName, startIndex, len(resultData)))
					if int64(len(resultData)) < d.limitCount {
						log.Info(fmt.Sprintf("table:%s, dbname=%s，startIndex:%d, len(resultData)=%d, break",
							table.Name, table.DBName, startIndex, len(resultData)))
						break
					}

					startIndex += d.limitCount
				}
			}
		}

		for {
			if d.pools.Running() <= 0 {
				close(d.channels)
				_ = d.pools.Release()
				d.done <- true
				log.Info(fmt.Sprintf("query update fileds count(%v), already mask count(%v), not mask count(%v), already mask but not rellace count(%v),",
					d.fieldCount, d.maskedCount, d.maskedNotReplaceCount, d.notMaskedCount))
				break
			}

			time.Sleep(3 * time.Second)
		}
	}()
}

func (d *Data) Close() {
	_ = d.pools.Release()
	fmt.Printf("mask query release pools\n")
	close(d.channels)
	fmt.Printf("mask query close channel\n")
	fmt.Printf("mask query close db start\n")
	for _, tableDBs := range d.dbs {
		for _, tableDB := range tableDBs {
			if tableDB == nil || tableDB.SQLDB == nil {
				continue
			}
			if err := tableDB.SQLDB.Close(); err != nil {
				fmt.Printf("mask query close db(%s), error(%s)", tableDB.Name, err.Error())
			} else {
				fmt.Printf("mask query close db(%s) success", tableDB.Name)
			}
		}
	}
	fmt.Println("mask query close db end")
}

// Done done
func (d *Data) Done() <-chan bool {
	return d.done
}

// New new channel data
func New(globalConfig *config.Info, channelData chan<- *common.ChannelData) (*Data, error) {
	if globalConfig == nil {
		return nil, errors.New("conf is nil")
	}

	if channelData == nil {
		return nil, errors.New("channelData is nil")
	}

	if globalConfig.SourceConf == nil || globalConfig.RedisConf == nil {
		return nil, errors.New("SourceConf or RedisConf is nil")
	}

	for _, conf := range globalConfig.SourceConf {
		if conf.Name == "" {
			return nil, errors.New("newMySQLDB name or username or addr or port is empty")
		}
	}

	dbs := make(map[string][]*TableInfo)
	for _, conf := range globalConfig.SourceConf {
		db, err := sql.Open("mysql", fmt.Sprintf("%s:%s@(%s:%d)/%s",
			globalConfig.UserName, globalConfig.Password, globalConfig.Host, globalConfig.Port, conf.Name))
		if err != nil {
			return nil, fmt.Errorf("newMySQLDB|sql.Open %s", err.Error())
		}

		db.SetMaxOpenConns(globalConfig.UpdatePoolCount)
		db.SetMaxIdleConns(globalConfig.UpdatePoolCount / 2)

		for _, table := range conf.Tables {
			primaryKeyNowSec := time.Now().UnixNano() / int64(time.Second)
			primaryKeyFirstData, err := common.GetTablePrimaryKeyFirstData(db, table.Name, table.Fields[0])
			if err != nil {
				log.Error(fmt.Sprintf("source db get db(%s)-table(%s) primary key first data error(%s)", conf.Name, table.Name, err.Error()))
				panic(fmt.Sprintf("source db get db(%s)-table(%s) primary key first data error(%s)", conf.Name, table.Name, err.Error()))
			}

			primaryKeyLastData, err := common.GetTablePrimaryKeyLastData(db, table.Name, table.Fields[0])
			if err != nil {
				log.Error(fmt.Sprintf("source db get db(%s)-table(%s) primary key last data error(%s)", conf.Name, table.Name, err.Error()))
				panic(fmt.Sprintf("source db get db(%s)-table(%s) primary key last data error(%s)", conf.Name, table.Name, err.Error()))
			}

			log.Info(fmt.Sprintf("source db get db(%s)-table(%s)-primaryKeyFirstData(%v)-primaryKeyFirstData(%v)-Cost(%.2fs)",
				conf.Name, table.Name, primaryKeyFirstData, primaryKeyLastData,
				float64(time.Now().UnixNano()/int64(time.Second)-primaryKeyNowSec)))

			dbs[conf.Name] = append(dbs[conf.Name], &TableInfo{
				SQLDB:               db,
				PrimaryKeyFirstData: primaryKeyFirstData,
				PrimaryKeyLastData:  primaryKeyLastData,
				DBName:              conf.Name,
				Name:                table.Name,
				Fields:              table.Fields,
			})
		}
	}

	mysqlDB := &Data{
		dbs:        dbs,
		done:       make(chan bool),
		channels:   channelData,
		logCount:   globalConfig.QueryPrintLogCount,
		limitCount: globalConfig.LimitCount,
	}

	pools, err := ants.NewPoolWithFunc(globalConfig.QueryPoolCount, func(data interface{}) {
		if err := mysqlDB.processOne(data); err != nil {
			log.Error(fmt.Sprintf("query process one error(%s)", err.Error()))
		}
	})
	if err != nil {
		return nil, err
	}

	r, err := redis.New(globalConfig.RedisConf)
	if err != nil {
		return nil, fmt.Errorf("query new redis error(%s)", err.Error())
	}

	mysqlDB.pools = pools
	mysqlDB.redis = r

	return mysqlDB, nil
}

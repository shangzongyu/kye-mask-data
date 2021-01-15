package update

import (
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"kye-mask-data/common"
	"kye-mask-data/config"

	"github.com/panjf2000/ants"
)

// Data data struct
type Data struct {
	done         chan bool
	dbs          *sync.Map // map[string]*sql.DB
	pools        *ants.PoolWithFunc
	channels     <-chan *common.ChannelData
	processCount int64
	logCount     int64
}

func (d *Data) processOne(inputData interface{}) error {
	if inputData == nil {
		return errors.New("update data is nil")
	}
	channelData, ok := inputData.(*common.ChannelData)
	if !ok {
		return errors.New("update data is not *common.ChannelData")
	}

	dbInterface, ok := d.dbs.Load(channelData.DBName)
	if !ok {
		return errors.New("update data is not *common.ChannelData")
	}
	db, ok := dbInterface.(*sql.DB)
	if !ok {
		return errors.New("update data db is not *sql.DB")
	}
	if len(channelData.UpdateDatas) <= 0 {
		return nil
	}

	var dataTemp []string
	for field, value := range channelData.UpdateDatas {
		dataTemp = append(dataTemp, fmt.Sprintf(`%s="%s"`, field, value))
	}

	var condsTemp []string
	for field, value := range channelData.Conditions {
		condsTemp = append(condsTemp, fmt.Sprintf(`%s="%s"`, field, value))
	}

	sqlStr := fmt.Sprintf("UPDATE `%s` SET %s WHERE %s", channelData.TableName, strings.Join(dataTemp, ", "),
		strings.Join(condsTemp, " AND "))
	log.Debug(fmt.Sprintf("mysql db update sqlstr=%s", sqlStr))
	result, err := db.Exec(sqlStr)
	if err != nil {
		return err
	}

	if _, err := result.RowsAffected(); err != nil {
		return err
	}
	log.Debug(fmt.Sprintf("update processOne %#v", inputData))
	go d.recordProcessCount()

	return nil
}

// UpdateRow update one row
func (d *Data) Run() {
	go func() {
		for channelData := range d.channels {
			if err := d.pools.Invoke(channelData); err != nil {
				log.Error(fmt.Sprintf("update data error(%s)", err.Error()))
				continue
			}
		}

		for {
			log.Error(fmt.Sprintf("update run count(%d)", d.pools.Running()))
			if d.pools.Running() <= 0 {
				d.pools.Release()
				d.done <- true
				break
			}

			time.Sleep(3 * time.Second)
		}
	}()
}

// Done done
func (d *Data) Done() <-chan bool {
	return d.done
}

func (d *Data) Close() {
	d.pools.Release()
	fmt.Printf("mask update release pools\n")
	fmt.Printf("mask update close db start\n")
	d.dbs.Range(func(key, value interface{}) bool {
		db := value.(*sql.DB)
		if db == nil {
			return true
		}
		if err := db.Close(); err != nil {
			fmt.Printf("mask update close db(%s), error(%s)", key, err.Error())
		} else {
			fmt.Printf("mask update close db(%s) success", key)
		}
		return true
	})

	fmt.Println("mask update close db end")
}

func (d *Data) recordProcessCount() {
	atomic.AddInt64(&d.processCount, 1)

	if d.processCount%d.logCount == 0 {
		log.Info(fmt.Sprintf("--- mysql update count(%d) time(%v)", d.processCount, time.Now().Format("2006-01-02 15:04:05")))
	}
}

// New new
func New(globalConfig *config.Info, channelData <-chan *common.ChannelData) (*Data, error) {
	if globalConfig == nil || globalConfig.SourceConf == nil || globalConfig.RedisConf == nil {
		return nil, errors.New("conf or source conf or redis conf is nil")
	}
	if channelData == nil {
		return nil, errors.New("channelData is nil")
	}

	if globalConfig.UserName == "" || globalConfig.Host == "" || globalConfig.Port <= 0 {
		return nil, errors.New("newMySQLDB name or username or addr or port is empty")
	}

	dbs := new(sync.Map) // map[string]*sql.DB
	for _, dbConf := range globalConfig.SourceConf {
		db, err := sql.Open("mysql", fmt.Sprintf("%s:%s@(%s:%d)/%s",
			globalConfig.UserName, globalConfig.Password, globalConfig.Host, globalConfig.Port, dbConf.Name))
		if err != nil {
			return nil, fmt.Errorf("newMySQLDB|sql.Open %s", err.Error())
		}

		db.SetMaxOpenConns(globalConfig.QueryPoolCount)
		db.SetMaxIdleConns(globalConfig.QueryPoolCount / 2)
		dbs.Store(dbConf.Name, db)
	}

	mysqlDB := &Data{
		dbs:      dbs,
		channels: channelData,
		done:     make(chan bool),
		logCount: globalConfig.QueryPrintLogCount,
	}

	log.Info(fmt.Sprintf("update pool count(%d)", globalConfig.UpdatePoolCount))
	pools, err := ants.NewPoolWithFunc(globalConfig.UpdatePoolCount, func(data interface{}) {
		if err := mysqlDB.processOne(data); err != nil {
			log.Error(fmt.Sprintf("mysql update process one error(%s)", err.Error()))
		}
	})

	if err != nil {
		return nil, err
	}

	mysqlDB.pools = pools

	return mysqlDB, nil
}

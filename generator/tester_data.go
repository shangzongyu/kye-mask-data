package generator

import (
	"database/sql"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	"mask-data/common"
	"mask-data/config"

	_ "github.com/go-sql-driver/mysql"
	"github.com/panjf2000/ants"
)

type TesterData struct {
	pool          *ants.PoolWithFunc
	sourceConfigs []*config.DB
	//redisConfig       *config.Redis
	tableFiledCount   int64
	perTableDataCount int64
}

func (t *TesterData) dropTableTemplateSQL(tableName string) string {
	return fmt.Sprintf("DROP TABLE IF EXISTS `%s`", tableName)
}

func (t *TesterData) createTableTemplateSQL(tableName string) string {
	return fmt.Sprintf(`
CREATE TABLE %s (
%s bigint(11) NOT NULL,
%s varchar(255) DEFAULT NULL,
PRIMARY KEY (%s) USING BTREE
) ENGINE=InnoDB AUTO_INCREMENT=0 DEFAULT CHARSET=utf8;`,
		fmt.Sprintf("`%s`", tableName),
		fmt.Sprintf("`id`"),
		fmt.Sprintf("`phone`"),
		fmt.Sprintf("`id`"))
}

func (t *TesterData) createTableTemplateSQL3(tableName string) string {
	return fmt.Sprintf(`
CREATE TABLE %s (
%s int(11) NOT NULL AUTO_INCREMENT,
%s varchar(255) DEFAULT NULL,
%s varchar(255) DEFAULT NULL,
%s varchar(255) DEFAULT NULL,
PRIMARY KEY (%s) USING BTREE
) ENGINE=InnoDB AUTO_INCREMENT=0 DEFAULT CHARSET=utf8;`,
		fmt.Sprintf("`%s`", tableName),
		fmt.Sprintf("`id`"),
		fmt.Sprintf("`phone`"),
		fmt.Sprintf("`phone_1`"),
		fmt.Sprintf("`phone_2`"),
		fmt.Sprintf("`id`"))
}

func (t *TesterData) insertDataSQL(tableName string, phone string) string {
	phoneInt64, _ := strconv.ParseInt(phone, 10, 64)
	return fmt.Sprintf(`INSERT INTO %s (id, phone) value (%v, %s)`, tableName, phoneInt64, phone)
}

func (t *TesterData) insertDataSQL3(tableName string, phones []string) string {
	return fmt.Sprintf(`INSERT INTO %s (phone,phone_1,phone_2) value (%s)`, tableName, strings.Join(phones, ","))
}

func (t *TesterData) dropTable(dbWG *sync.WaitGroup, sourceConfigOne *config.DB) {
	defer dbWG.Done()

	tablesInfo, err := common.GetDBAllTableInfos(sourceConfigOne.DB)
	if err != nil {
		panic(fmt.Errorf("droptables db(%s) GetDBAllTableInfos %s", sourceConfigOne.Name, err.Error()))
	}

	log.Info(fmt.Sprintf("droptables db(%s) have(%d) tables", sourceConfigOne.Name, len(tablesInfo)))
	var tableWG sync.WaitGroup
	for tableName := range tablesInfo {
		tableWG.Add(1)

		go func(tableWG *sync.WaitGroup, sourceConfigOne *config.DB, tableName string) {
			defer tableWG.Done()

			dropTableSQL := t.dropTableTemplateSQL(tableName)
			if _, err := sourceConfigOne.DB.Exec(dropTableSQL); err != nil {
				log.Error(fmt.Sprintf("droptables db(%s)-dropTestTableTemplateSQL: %s", sourceConfigOne.Name, err.Error()))
				return
			}

			log.Info(fmt.Sprintf("db(%s)-dropTestTable(%s) success", sourceConfigOne.Name, tableName))
		}(&tableWG, sourceConfigOne, tableName)
	}
	tableWG.Wait()
}

func (t *TesterData) dropTables() error {
	dbWG := new(sync.WaitGroup)
	for _, sourceConfigOne := range t.sourceConfigs {
		dbWG.Add(1)
		go t.dropTable(dbWG, sourceConfigOne)
	}

	dbWG.Wait()
	return nil
}

func (t *TesterData) createTable(dbWG *sync.WaitGroup, sourceConfig *config.DB) {
	defer dbWG.Done()

	var tableWG sync.WaitGroup
	for _, tableInfo := range sourceConfig.Tables {
		tableWG.Add(1)
		tableName := tableInfo.Name

		go func(tableWG *sync.WaitGroup, sourceConfig *config.DB, tableName string) {
			defer tableWG.Done()

			switch t.tableFiledCount {
			case 1:
				fallthrough
			default:
				crateTableSQL := t.createTableTemplateSQL(tableName)
				if _, err := sourceConfig.DB.Exec(crateTableSQL); err != nil {
					panic(fmt.Errorf("createTable data db(%s)-table(%s)-createTestTableTemplateSQL(%s) failure",
						sourceConfig.Name, tableName, err.Error()))
				}
			case 3:
				crateTableSQL := t.createTableTemplateSQL3(tableName)
				if _, err := sourceConfig.DB.Exec(crateTableSQL); err != nil {
					panic(fmt.Errorf("createTable data db(%s)-table(%s)-createTestTableTemplateSQL3(%s) failure",
						sourceConfig.Name, tableName, err.Error()))
				}
			}
			log.Info(fmt.Sprintf("createTables db(%s)-table(%s) success", sourceConfig.Name, tableName))
		}(&tableWG, sourceConfig, tableName)
	}
	tableWG.Wait()
}

func (t *TesterData) createTables() error {
	dbWG := new(sync.WaitGroup)
	for _, sourceConfig := range t.sourceConfigs {
		dbWG.Add(1)
		go t.createTable(dbWG, sourceConfig)
	}

	dbWG.Wait()
	return nil
}

type insertData struct {
	dbName    string
	tableName string
	db        *sql.DB
	phone     string
}

type insertData3 struct {
	dbName    string
	tableName string
	db        *sql.DB
	phones    []string
}

func (t *TesterData) insertData(data interface{}) {
	if data == nil {
		return
	}

	switch t.tableFiledCount {
	case 1:
		fallthrough
	default:
		insertInfo, ok := data.(*insertData)
		if !ok {
			log.Error(" data not type *insertData")
			return
		}

		insertSQL := t.insertDataSQL(insertInfo.tableName, insertInfo.phone)
		if _, err := insertInfo.db.Exec(insertSQL); err != nil {
			log.Error(fmt.Sprintf("insertDataToTables data db(%s)-table(%s) insert data:(%s)，sql(%s)",
				insertInfo.dbName, insertInfo.tableName, err.Error(), insertSQL))
		}
	case 3:
		insertInfo, ok := data.(*insertData3)
		if !ok {
			log.Error(" data not type *insertData3")
			return
		}

		insertSQL := t.insertDataSQL3(insertInfo.tableName, insertInfo.phones)
		if _, err := insertInfo.db.Exec(insertSQL); err != nil {
			log.Error(fmt.Sprintf("insertDataToTables data db(%s)-table(%s) insert data:(%s)，sql(%s)",
				insertInfo.dbName, insertInfo.tableName, err.Error(), insertSQL))
		}
	}
}

func (t *TesterData) insertDataToTables() error {
	for _, sourceConfig := range t.sourceConfigs {
		for _, tableInfo := range sourceConfig.Tables {
			for index := int64(0); index < t.perTableDataCount; index++ {
				var data interface{}
				switch t.tableFiledCount {
				case 1:
					fallthrough
				default:
					data = &insertData{
						dbName:    sourceConfig.Name,
						db:        sourceConfig.DB,
						tableName: tableInfo.Name,
						phone:     generatePhoneNumber(),
					}
				case 3:
					var phone3 []string
					for i := 0; i < 3; i++ {
						phone3 = append(phone3, generatePhoneNumber())
					}
					data = &insertData3{
						dbName:    sourceConfig.Name,
						db:        sourceConfig.DB,
						tableName: tableInfo.Name,
						phones:    phone3,
					}
				}

				if err := t.pool.Invoke(data); err != nil {
					log.Error(fmt.Sprintf("test data pool Serve (%s)", err.Error()))
					return err
				}
			}
		}
		log.Info(fmt.Sprintf("genereate test data, db(%s) sucess", sourceConfig.Name))
	}
	return nil
}

// Run tester data run
func (t *TesterData) Run() error {
	if err := t.dropTables(); err != nil {
		return err
	}
	log.Info("generate test dropTables success")

	if err := t.createTables(); err != nil {
		return err
	}
	log.Info("generate test createTables success")

	if err := t.insertDataToTables(); err != nil {
		return err
	}

	for {
		log.Info(fmt.Sprintf("pool running insert count(%d)", t.pool.Running()))
		if t.pool.Running() <= 0 {
			break
		}
		time.Sleep(3 * time.Second)
	}
	log.Info(fmt.Sprintf("generate test insertDataToTables success, max count(%v)", startPhone))

	for _, sourceDB := range t.sourceConfigs {
		_ = sourceDB.DB.Close()
	}

	//redis.SetLogger(log)
	//r, err := redis.New(t.redisConfig)
	//if err != nil {
	//	return err
	//}
	//r.ClearData()

	return nil
}

func (t *TesterData) Close() {
	fmt.Printf("generate tester data close db start\n")

	for _, confDB := range t.sourceConfigs {
		if confDB == nil || confDB.DB == nil {
			continue
		}

		if err := confDB.DB.Close(); err != nil {
			fmt.Printf("generate tester data db(%s) close error(%s)\n", confDB.Name, err.Error())
		} else {
			fmt.Printf("generate tester data db(%s) close success\n", confDB.Name)
		}
	}
	fmt.Printf("generate tester data close db done\n")
}

// NewTestData generate test data
func NewTestData(info *config.GenerateTestDataFlag) (*TesterData, error) {
	log.Info("generate test data tart...")
	if info == nil {
		return nil, errors.New(" info is nil")
	}

	if info.DBFile == "" {
		return nil, errors.New("db file is emtpy")
	}

	if info.TableDataCount == 0 {
		return nil, errors.New(" table data count is 0")
	}

	if info.TableCount == 0 {
		return nil, errors.New("table count is 0")
	}

	dbNames, err := common.ScanFileByLine(info.DBFile)
	if err != nil {
		return nil, fmt.Errorf("open dbfile error:%s", err.Error())
	}

	if len(dbNames) <= 0 {
		return nil, errors.New("db names is empty")
	}

	if len(dbNames) <= 0 || info.TableCount <= 0 || info.TableDataCount <= 0 || info.PoolCount <= 0 {
		return nil, errors.New("dbname len <=0 or tableCount<=0 or TableDataCount <= 0 or poolcount <=0")
	}

	log.Info(fmt.Sprintf("dbcount(%v)-per-db-table-count(%v),-perTableDataCount(%v)-tablefieldCount(%v)-poolCount(%v)",
		len(dbNames), info.TableCount, info.TableDataCount, info.TableCount, info.PoolCount))
	log.Info(fmt.Sprintf("db username(%s)-password(%s)-host(%s)-port(%d)", info.Username, info.Password, info.Host, info.Port))
	tablesNames := make([]string, info.TableCount)
	for tableIndex := int64(0); tableIndex < info.TableCount; tableIndex++ {
		tablesNames[tableIndex] = fmt.Sprintf("people_%d", tableIndex)
	}

	globalConfig := config.Info{
		ChannelCount:    10000,
		QueryPoolCount:  10000,
		UpdatePoolCount: 1000,
		LimitCount:      10000,
		UserName:        info.Username,
		Password:        info.Password,
		Host:            info.Host,
		Port:            info.Port,
		RedisConf:       defaultRedisConfig(),
		SourceConf:      make([]*config.DB, 0),
	}
	dbsConfig := make([]*config.DB, 0)
	for _, dbName := range dbNames {
		tablesConfig := make([]*config.Table, info.TableCount)
		for tableIndex, tableName := range tablesNames {
			tablesConfig[tableIndex] = &config.Table{
				Name:   tableName,
				Fields: []string{"id", "phone", "phone_1", "phone_2"},
			}
		}

		log.Info(fmt.Sprintf("db(%s),table-count(%d)", dbName, len(tablesConfig)))
		dbConfig := &config.DB{}
		db, err := sql.Open("mysql", fmt.Sprintf("%s:%s@(%s)/%s",
			globalConfig.UserName, globalConfig.Password, fmt.Sprintf("%v:%v", globalConfig.Host, globalConfig.Port), dbName))
		if err != nil {
			return nil, fmt.Errorf("newTestData sql.Open %s", err.Error())
		}

		if globalConfig.MaxOpenConns == 0 {
			maxCount, err := common.GetTableMaxConnections(db)
			if err == nil {
				globalConfig.MaxOpenConns = maxCount
				globalConfig.MaxIdleConns = maxCount / 2
			} else {
				log.Error(fmt.Sprintf("newTestData get max connections error(%s)", err.Error()))
				globalConfig.MaxOpenConns = 200
				globalConfig.MaxOpenConns = 100
			}
		}

		db.SetMaxOpenConns(globalConfig.MaxOpenConns)
		db.SetMaxIdleConns(globalConfig.MaxIdleConns)

		dbConfig.Tables = tablesConfig
		dbConfig.Name = dbName
		dbConfig.DB = db
		dbsConfig = append(dbsConfig, dbConfig)
	}

	t := &TesterData{
		sourceConfigs: dbsConfig,
		//redisConfig:       globalConfig.RedisConf,
		perTableDataCount: info.TableDataCount,
	}

	pool, err := ants.NewPoolWithFunc(int(info.PoolCount), func(data interface{}) {
		t.insertData(data)
	})
	if err != nil {
		return nil, err
	}

	t.pool = pool

	return t, nil
}

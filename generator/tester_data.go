package generator

import (
    "database/sql"
    "errors"
    "fmt"
    "strings"
    "sync"
    "time"

    "mask-data/common"
    "mask-data/config"

    _ "github.com/go-sql-driver/mysql"
    "github.com/panjf2000/ants"
)

type TesterData struct {
    pool            *ants.PoolWithFunc
    sourceConfigs   []*config.DB
    tableFiledCount int64
    generateDBInfo  map[string]*config.GenerateDBInfo
}

func (t *TesterData) dropTableTemplateSQL(tableName string) string {
    return fmt.Sprintf("DROP TABLE IF EXISTS `%s`", tableName)
}

func (t *TesterData) createTableTemplateSQL(tableName string, fieldNames []string) string {
    var fieldSQL []string
    for _, fieldName := range fieldNames {
        fieldSQL = append(fieldSQL, fmt.Sprintf("%s varchar(255) DEFAULT NULL", fieldName))
    }

    return fmt.Sprintf(`
CREATE TABLE %s (
%s int(11) NOT NULL AUTO_INCREMENT,
%s,
PRIMARY KEY (%s) USING BTREE
) ENGINE=InnoDB AUTO_INCREMENT=0 DEFAULT CHARSET=utf8;`,
        fmt.Sprintf("`%s`", tableName),
        fmt.Sprintf("`id`"),
        strings.Join(fieldNames, ","),
        fmt.Sprintf("`id`"))
}

func (t *TesterData) insertDataSQL(tableName string, fieldsName []string, phones []string) string {
    return fmt.Sprintf(`INSERT INTO %s (%s) value (%s)`, tableName, strings.Join(fieldsName, ","), strings.Join(phones, ","))
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

func (t *TesterData) dropDBsAndTables() error {
    dbWG := new(sync.WaitGroup)
    for _, sourceConfigOne := range t.sourceConfigs {
        dbWG.Add(1)
        go t.dropTable(dbWG, sourceConfigOne)
    }

    dbWG.Wait()
    return nil
}

func (t *TesterData) createDB( sourceConfig *config.DB) {
    sql := fmt.Sprintf("CREATE DATABASE `%s`", sourceConfig.Name)
    sourceConfig.DB.Exec(sql)
}
func (t *TesterData) createTable(dbWG *sync.WaitGroup, sourceConfig *config.DB) {
    defer dbWG.Done()

    var tableWG sync.WaitGroup
    for _, tableInfo := range sourceConfig.Tables {
        tableWG.Add(1)

        go func(tableWG *sync.WaitGroup, sourceConfig *config.DB, tableInfo *config.Table) {
            defer tableWG.Done()

            crateTableSQL := t.createTableTemplateSQL(tableInfo.Name, tableInfo.Fields)
            if _, err := sourceConfig.DB.Exec(crateTableSQL); err != nil {
                panic(fmt.Errorf("createTable data db(%s)-table(%s)-createTestTableTemplateSQL3(%s) failure",
                    sourceConfig.Name, tableInfo.Name, err.Error()))
            }
            log.Info(fmt.Sprintf("createTables db(%s)-table(%s) success", sourceConfig.Name, tableInfo.Name))
        }(&tableWG, sourceConfig, tableInfo)
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
    dbName     string
    tableName  string
    db         *sql.DB
    fieldNames []string
    phones     []string
}

func (t *TesterData) insertData(data interface{}) {
    if data == nil {
        return
    }

    insertInfo, ok := data.(*insertData)
    if !ok {
        log.Error("data not type *insertData3")
        return
    }

    insertSQL := t.insertDataSQL(insertInfo.tableName, insertInfo.fieldNames, insertInfo.phones)
    if _, err := insertInfo.db.Exec(insertSQL); err != nil {
        log.Error(fmt.Sprintf("insertDataToTables data db(%s)-table(%s) insert data:(%s)，sql(%s)",
            insertInfo.dbName, insertInfo.tableName, err.Error(), insertSQL))
    }
}

func (t *TesterData) insertDataToTables() error {
    for _, sourceConfig := range t.sourceConfigs {
        tableCount := t.generateDBInfo[sourceConfig.Name].TableCount
        for _, tableInfo := range sourceConfig.Tables {
            for index := int64(0); index < tableCount; index++ {
                phones := make([]string, len(tableInfo.Fields))
                for i := 0; i < len(tableInfo.Fields); i++ {
                    phones[i] = generatePhoneNumber()
                }

                if err := t.pool.Invoke(&insertData{
                    dbName:    sourceConfig.Name,
                    db:        sourceConfig.DB,
                    tableName: tableInfo.Name,
                    phones:    phones,
                }); err != nil {
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
    if err := t.dropDBsAndTables(); err != nil {
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

// Close
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
func NewTestData(info *config.GenerateConfig) (*TesterData, error) {
    log.Info("generate test data tart...")
    if info == nil {
        return nil, errors.New(" info is nil")
    }

    if info.DBFile == "" {
        return nil, errors.New("db file is empty")
    }

    if info.DBInfos == nil || len(info.DBInfos) <= 0 {
        return nil, errors.New("db info is empty")
    }

    //if info.TableDataCount == 0 {
    //	return nil, errors.New(" table data count is 0")
    //}
    //
    //if info.TableCount == 0 {
    //	return nil, errors.New("table count is 0")
    //}

    //log.Info(fmt.Sprintf("dbcount(%v)-per-db-table-count(%v),-perTableDataCount(%v)-tablefieldCount(%v)-poolCount(%v)",
    //	len(info.DBInfos), info.TableCount, info.TableDataCount, info.TableCount, info.PoolCount))
    //log.Info(fmt.Sprintf("db username(%s)-password(%s)-host(%s)-port(%d)", info.DBUsername, info.DBPassword, info.DBHost, info.DBPort))

    generateDBInfo := make(map[string]*config.GenerateDBInfo)
    for _, dbInfo := range info.DBInfos {
        generateDBInfo[dbInfo.Name] = dbInfo
    }

    dbsConfig := make([]*config.DB, 0)
    for _, dbInfo := range info.DBInfos {
        tablesConfig := make([]*config.Table, dbInfo.TableCount)
        for index := int64(0); index < dbInfo.TableCount; index++ {
            tableName := fmt.Sprintf("%s%d", dbInfo.TablePrefix, index)
            fieldNames := []string{"id"}
            for fieldIndex := int64(0); fieldIndex <= dbInfo.FieldCount; fieldIndex++ {
                fieldNames = append(fieldNames, fmt.Sprintf("%s%s", dbInfo.FieldPrefix, fieldIndex))
            }

            tablesConfig[index] = &config.Table{
                Name:   tableName,
                Fields: fieldNames,
            }
        }

        log.Info(fmt.Sprintf("db(%s),table-count(%d)", dbInfo.Name, len(tablesConfig)))
        dbConfig := &config.DB{}
        db, err := sql.Open("mysql", fmt.Sprintf("%s:%s@(%s)/%s",
            info.DBUserName, info.DBPassword, fmt.Sprintf("%v:%v", info.DBHost, info.DBPort), dbInfo.Name))
        if err != nil {
            return nil, fmt.Errorf("newTestData sql.Open %s", err.Error())
        }

        if info.DBMaxOpenConns == 0 {
            maxCount, err := common.GetTableMaxConnections(db)
            if err == nil {
                info.DBMaxOpenConns = maxCount
                info.DBMaxIdleConns = maxCount / 2
            } else {
                log.Error(fmt.Sprintf("newTestData get max connections error(%s)", err.Error()))
                info.DBMaxOpenConns = 200
                info.DBMaxOpenConns = 100
            }
        }

        db.SetMaxOpenConns(info.DBMaxOpenConns)
        db.SetMaxIdleConns(info.DBMaxIdleConns)

        dbConfig.Tables = tablesConfig
        dbConfig.Name = dbInfo.Name
        dbConfig.DB = db
        dbsConfig = append(dbsConfig, dbConfig)
    }

    t := &TesterData{
        sourceConfigs:  dbsConfig,
        generateDBInfo: generateDBInfo,
    }

    poolCount := 10
    pool, err := ants.NewPoolWithFunc(poolCount, func(data interface{}) {
        t.insertData(data)
    })
    if err != nil {
        return nil, err
    }

    t.pool = pool

    return t, nil
}

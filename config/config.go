package config

import (
    "database/sql"
    "errors"
    "fmt"
    "io/ioutil"
    "os"

    jsoniter "github.com/json-iterator/go"
)

var (
    json = jsoniter.ConfigCompatibleWithStandardLibrary
)

// Info conf info
type Info struct {
    LogLevel            string `json:"log_level"`
    LogFile             string `json:"log_file"`
    ChannelCount        int    `json:"channel_count"`
    QueryPrintLogCount  int64  `json:"query_print_log_count"`
    QueryPoolCount      int    `json:"query_pool_count"`
    UpdatePrintLogCount int64  `json:"update_print_log_count"`
    UpdatePoolCount     int    `json:"update_pool_count"`
    LimitCount          int64  `json:"limit_count"`
    SourceConf          []*DB  `json:"db_infos"`
    RedisConf           *Redis `json:"redis_info"`
    UserName            string `json:"user_name"`
    Password            string `json:"password"`
    Host                string `json:"host"`
    Port                int64  `json:"port"`
    MaxOpenConns        int    `json:"max_open_conns"`
    MaxIdleConns        int    `json:"max_idle_conns"`
}

// DB source store config
type DB struct {
    Name   string   `json:"name"`
    Tables []*Table `json:"tables"`
    DB     *sql.DB  `json:"db,omitempty"`
}

// Table table info config
type Table struct {
    Name   string   `json:"name"`
    Fields []string `json:"fields"` // 0: is primary key, left is other files
}

// Redis target store config
type Redis struct {
    Index    int64  `json:"index"`
    Addr     string `json:"addr"`
    Password string `json:"password,omitempty"`
}

func openConfigFile(filePath string) (*Info, error) {
    if filePath == "" {
        return nil, errors.New("filePath is empty")
    }

    jsonFile, err := os.Open(filePath)
    if err != nil {
        return nil, fmt.Errorf("openConfigFile os.Open(%s) error(%s)", filePath, err.Error())
    }
    // defer the closing of our jsonFile so that we can parse it later on
    defer jsonFile.Close()

    byteValue, err := ioutil.ReadAll(jsonFile)
    if err != nil {
        return nil, fmt.Errorf("openConfigFileioutil.ReadAll(%s) error(%s)", filePath, err.Error())
    }

    var result Info
    if err := json.Unmarshal([]byte(byteValue), &result); err != nil {
        return nil, fmt.Errorf("openConfigFile unmarhal json error(%s)", err.Error())
    }

    return &result, err
}

// New new config
func New(filePath string) (*Info, error) {
    info, err := openConfigFile(filePath)
    if err != nil {
        return nil, err
    }
    return info, nil
}

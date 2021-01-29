package config

import "fmt"

type GenerateDBInfo struct {
    Name          string `json:"name"`
    TablePrefix   string `json:"table_prefix"`
    TableCount    int64  `json:"table_count"`
    TableRowCount int64  `json:"table_row_count"`
    FieldPrefix   int64  `json:"field_prefix"`
    FieldCount    int64  `json:"field_count"`
}

// GenerateDBFlag generate db flag config
type GenerateConfig struct {
    FileName   string            `json:"output_file"`
    DBFile     string            `json:"db_file"`
    DBUserName string            `json:"db_user_name"`
    DBPassword string            `json:"db_password"`
    DBHost     string            `json:"db_host"`
    DBPort     int64             `json:"db_port"`
    DBInfos    []*GenerateDBInfo `json:"db_infos"`

    CalCount   int64
    PerCount   int64
    Percent    float64
    RegexpFile string

    LogFile        string `json:"log_file"`
    LogLevel       string `json:"log_level"`
    DBMaxOpenConns int    `json:"db_max_open_conns"`
    DBMaxIdleConns int    `json:"db_max_idle_conns"`
}

// New new config
func NewGenerateDB(filePath string) (*GenerateConfig, error) {
    byteValue, err := openConfigFile(filePath)
    if err != nil {
        return nil, err
    }

    var info *GenerateConfig
    if err := json.Unmarshal(byteValue, info); err != nil {
        return nil, fmt.Errorf("openConfigFile unmarhal json error(%s)", err.Error())
    }

    return info, nil
}

// GenerateTestDataFlag generate test data flag config
type GenerateTestData struct {
    UserName        string `json:"user_name"`
    Password        string `json:"password"`
    Host            string `json:"host"`
    Port            int64  `json:"port"`
    TableCount      int64  `json:"table_count"`
    TableDataCount  int64  `json:"table_data_count"`
    TableFieldCount int64  `json:"table_field_count"`
    PoolCount       int64  `json:"poll_count"`
    DBFile          string `json:"db_file"`
    LogFile         string `json:"log_file"`
    LogLevel        string `json:"log_level"`
}

// New new config
func NewGenerateTestData(filePath string) (*GenerateTestData, error) {
    byteValue, err := openConfigFile(filePath)
    if err != nil {
        return nil, err
    }

    var info *GenerateTestData
    if err := json.Unmarshal(byteValue, info); err != nil {
        return nil, fmt.Errorf("openConfigFile unmarhal json error(%s)", err.Error())
    }

    return info, nil
}

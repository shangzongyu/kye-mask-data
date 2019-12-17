package config

// GenerateDBFlag generat db flag config
type GenerateDBFlag struct {
    FileName   string
    DBFile     string
    Username   string
    Password   string
    Host       string
    Port       int64
    CalCount   int64
    PerCount   int64
    Percent    float64
    RegexpFile string
    LogFile    string
    LogLevel   string
}

// GenerateTestDataFlag generat test data flag config
type GenerateTestDataFlag struct {
    Username        string
    Password        string
    Host            string
    Port            int64
    TableCount      int64
    TableDataCount  int64
    TableFieldCount int64
    PoolCount       int64
    DBFile          string
    LogFile         string
    LogLevel        string
}

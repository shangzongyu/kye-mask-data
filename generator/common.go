package generator

import (
    "errors"
    "fmt"
    "strconv"
    "sync/atomic"

    "kye-mask-data/config"

    "github.com/json-iterator/go"
    "go.uber.org/zap"
)

var (
    json = jsoniter.ConfigCompatibleWithStandardLibrary
    log  *zap.Logger

    defaultRedisConfig = func() *config.Redis {
        return &config.Redis{
            Index:    0,
            Addr:     "localhost:6379",
            Password: "",
        }
    }
)

// SetLogger set logger
func SetLogger(l *zap.Logger) {
    log = l
}

func configParameterCheck(info *config.GenerateDBFlag) error {
    if info == nil {
        return errors.New("conf files is emtpy")
    }

    if info.FileName == "" {
        return errors.New("config file is emtpy")
    }

    if info.DBFile == "" {
        return errors.New("db file is emtpy")
    }

    if info.Username == "" {
        return errors.New("username is emtpy")
    }

    if info.Password == "" {
        return errors.New("password is emtpy")
    }

    if info.Host == "" {
        return errors.New("host is emtpy")
    }

    if info.PerCount == 0 {
        return errors.New("limit == 0")
    }

    if info.CalCount == 0 {
        return errors.New("CalCount == 0")
    }

    if info.Percent == 0 {
        return errors.New("percent == 0")
    }

    if info.RegexpFile == "" {
        return errors.New("regexp file is empty")
    }

    return nil
}

var startPhone = uint64(17000000000)

func generatePhoneNumber() string {
    atomic.AddUint64(&startPhone, 1)

    if startPhone%100000 == 0 {
        go func(phone uint64) {
            log.Info(fmt.Sprintf("generate phone count(%d)", phone))
        }(startPhone)
    }
    return strconv.FormatUint(startPhone, 10)
}

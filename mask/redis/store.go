package redis

import (
    "errors"
    "fmt"
    "strconv"

    "kye-mask-data/config"

    "github.com/go-redis/redis"
)

// Redis redis store
type Store struct {
    client                    *redis.Client
    unusedDataKey             string
    sourceMaskedDataKeyPrefix string // key(source):value(masked)
    maskedSourceDataKeyPrefix string // key(masked):value(source)
}

// IsMasked is masked
func (r *Store) IsMasked(data string) bool {
    if data == "" {
        log.Error(fmt.Sprintf("IsMasked(%s)====", data))
        return false
    }

    cmd := r.client.Exists(fmt.Sprintf("%s:%s", r.maskedSourceDataKeyPrefix, data))
    result, err := cmd.Result()
    if err != nil {
        log.Info(fmt.Sprintf("redis target IsMasked err(%s)", err.Error()))
        return false
    }

    return result >= int64(1)
}

// GetMaskedData GetMaskedData
func (r *Store) GetMaskedData(data string) string {
    if data == "" {
        log.Error("GetMaskedData empty")
        return ""
    }

    cmd := r.client.Get(fmt.Sprintf("%s:%s", r.sourceMaskedDataKeyPrefix, data))
    result, err := cmd.Result()
    if err != nil && err != redis.Nil {
        log.Error(fmt.Sprintf("redis target GetMaskedData err(%s),key(%v)", err.Error(),
            fmt.Sprintf("%s:%s", r.sourceMaskedDataKeyPrefix, data)))
        return ""
    }

    return result
}

// UpdateMaskedData update data
func (r *Store) UpdateMaskedData(sourceData string, maskedData interface{}) error {
    if sourceData == "" || maskedData == nil {
        return errors.New("phones empty")
    }

    cli := r.client.TxPipeline()
    cli.Set(fmt.Sprintf("%s:%s", r.sourceMaskedDataKeyPrefix, sourceData), maskedData, 0)
    cli.Set(fmt.Sprintf("%s:%s", r.maskedSourceDataKeyPrefix, maskedData), sourceData, 0)
    _, err := cli.Exec()

    return err
}

// GetOneUnusedData get unused data
func (r *Store) GetOneUnusedData() string {
    intCmd := r.client.Incr(r.unusedDataKey)
    result, _ := intCmd.Result()
    log.Debug(fmt.Sprintf("GetOneUnusedData get new(%v)", result))
    return strconv.FormatInt(result, 10)
}

// MaskedCount used count
func (r *Store) MaskedCount() int64 {
    count := int64(0)
    cursor := uint64(0)
    for true {
        scanCmd := r.client.Scan(uint64(cursor), fmt.Sprintf("%s:*", r.sourceMaskedDataKeyPrefix), 10000)
        vals, cursorResult := scanCmd.Val()
        count += int64(len(vals))
        if cursorResult == 0 {
            break
        }

        cursor = cursorResult
    }

    return count
}

// ClearData clear data
func (r *Store) ClearData() error {
    cmd := r.client.FlushAll()

    if _, err := cmd.Result(); err != nil {
        log.Info(fmt.Sprintf("redis target ClearData error(%s)", err.Error()))
        return err
    }

    return nil
}

// New new redis
func New(info *config.Redis) (*Store, error) {
    if info == nil {
        return nil, errors.New("new redis store config is nil")
    }

    client := redis.NewClient(&redis.Options{
        Addr:     info.Addr,
        Password: info.Password,
        DB:       int(info.Index),
    })

    s := &Store{
        client:                    client,
        unusedDataKey:             "kye-mask:unused:index",
        sourceMaskedDataKeyPrefix: "kye-mask:source:masked",
        maskedSourceDataKeyPrefix: "kye-mask:masked:source",
    }

    existCmd := s.client.Exists(s.unusedDataKey)
    exist, err := existCmd.Result()
    if err != nil {
        return nil, fmt.Errorf("redis existCmd cmd error(%s)", err.Error())
    }
    if exist < 1 {
        statusCmd := s.client.Set(s.unusedDataKey, "16000000000", 0)
        if _, err := statusCmd.Result(); err != nil {
            return nil, fmt.Errorf("redis status cmd(%s)", err.Error())
        }
    }

    return s, nil
}

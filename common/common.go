package common

import (
    "bufio"
    "errors"
    "log"
    "os"
    "regexp"
)

// ChannelData channel data for channel
type ChannelData struct {
    DBName      string
    TableName   string
    UpdateDatas map[string]string // key:field, value: value
    Conditions  map[string]string // key:filed, value: value
}

// ValidateRegular Validate Regular
func ValidateRegular(mobileNum string, regulars []string) bool {
    if mobileNum == "" {
        return false
    }

    for _, regular := range regulars {
        reg := regexp.MustCompile(regular)
        if reg.MatchString(mobileNum) {
            return true
        }
    }

    return false
}

// ScanFileByLine scan file by lines
func ScanFileByLine(logfile string) ([]string, error) {
    if logfile == "" {
        return nil, errors.New("logfile is empty")
    }

    lines := make([]string, 0)
    f, err := os.OpenFile(logfile, os.O_RDONLY, os.ModePerm)
    if err != nil {
        return nil, err
    }
    defer f.Close()

    sc := bufio.NewScanner(f)
    for sc.Scan() {
        lines = append(lines, sc.Text())
    }

    if err := sc.Err(); err != nil {
        log.Fatalf("scan file error: %v", err)
        return nil, err
    }

    return lines, nil
}

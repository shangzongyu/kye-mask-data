package config

import (
    "fmt"

    "go.uber.org/zap"
)

// Logger logger
func Logger(fileName, level string) (*zap.Config, error) {
    var rawJSON []byte
    if fileName != "" {
        rawJSON = []byte(fmt.Sprintf(`{
      "level": "%s",
      "encoding": "json",
      "outputPaths": ["%s"],
      "errorOutputPaths": ["%s"],
      "encoderConfig": {
        "messageKey": "message",
        "levelKey": "level",
        "levelEncoder": "lowercase"
      }
    }`, level, fileName, fileName))
    } else {
        rawJSON = []byte(fmt.Sprintf(`{
      "level": "%s",
      "encoding": "json",
      "outputPaths": ["stdout"],
      "errorOutputPaths": ["stderr"],
      "encoderConfig": {
        "messageKey": "message",
        "levelKey": "level",
        "levelEncoder": "lowercase"
      }
    }`, level))
    }

    var cfg zap.Config
    if err := json.Unmarshal(rawJSON, &cfg); err != nil {
        return nil, err
    }

    return &cfg, nil
}

package update

import "go.uber.org/zap"

var log *zap.Logger

// SetLogger set logger
func SetLogger(l *zap.Logger) {
    log = l
    log.Named("source")
}

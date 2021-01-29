package mask

import (
	"errors"
	"fmt"
	"sync"

	"mask-data/common"
	"mask-data/config"
	"mask-data/mask/query"
	"mask-data/mask/update"

	"go.uber.org/zap"
)

// Mask mask struct
type Mask struct {
	queryDB  *query.Data
	updateDB *update.Data
}

// Run run mask program
func (m *Mask) Run() {
	m.queryDB.Run()
	log.Info("query db start...")

	m.updateDB.Run()
	log.Info("update db start...")

	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()

		<-m.queryDB.Done()
		log.Info(fmt.Sprintf("query db done"))
	}()
	go func() {
		defer wg.Done()

		<-m.updateDB.Done()
		log.Info(fmt.Sprintf("update db done"))
	}()
	wg.Wait()
}

// New new mask
func New(conf *config.Info) (*Mask, error) {
	if conf == nil {
		return nil, errors.New("conf is nil")
	}
	if conf.ChannelCount == 0 {
		return nil, errors.New("conf channel count is 0")
	}

	channels := make(chan *common.ChannelData, conf.ChannelCount)

	query.SetLogger(log)
	qData, err := query.New(conf, channels)
	if err != nil {
		return nil, errors.New("new query data err: " + err.Error())
	}

	update.SetLogger(log)
	uData, err := update.New(conf, channels)
	if err != nil {
		return nil, errors.New("new update data err: " + err.Error())
	}

	return &Mask{
		queryDB:  qData,
		updateDB: uData,
	}, nil
}

// Close mask close process
func (m *Mask) Close() {
	fmt.Println("mask data close db start")
	m.queryDB.Close()
	m.updateDB.Close()
	fmt.Println("mask data close db end")
}

var log *zap.Logger

// SetLogger set logger
func SetLogger(l *zap.Logger) {
	log = l
}

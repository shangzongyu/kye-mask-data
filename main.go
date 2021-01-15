package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"

	"kye-mask-data/config"
	"kye-mask-data/generator"
	"kye-mask-data/mask"
)

var (
	help     bool
	model    string
	confFile string

	generateDBConfigFlag       *config.GenerateDBFlag
	generateTestDataConfigFlag *config.GenerateTestDataFlag
	closer                     DBCloser

	sigs    = make(chan os.Signal, 1)
	success = make(chan int)
)

const (
	modelRun            = "run"
	modelGenerateConfig = "generate-conf"
	modelGenerateTest   = "generate-test"
)

func main() {
	flag.Parse()

	if help {
		flag.Usage()
		return
	}

	signal.Notify(sigs,
		syscall.SIGHUP,
		syscall.SIGINT,
		syscall.SIGTERM,
		syscall.SIGQUIT)

	go func() {
		for {
			s := <-sigs
			switch s {
			// kill -SIGHUP XXXX
			case syscall.SIGHUP:
				fmt.Println("Signal: hungup")
				if closer != nil {
					closer.Close()
					fmt.Println("close dbs...")
				}
				os.Exit(0)
				success <- 0
				// kill -SIGINT XXXX or Ctrl+c
			case syscall.SIGINT:
				fmt.Println("Signal: SIGINT")
				if closer != nil {
					closer.Close()
					fmt.Println("close dbs...")
				}
				success <- 0
				// kill -SIGTERM XXXX
			case syscall.SIGTERM:
				fmt.Println("Signal: force stop SIGTERM")
				if closer != nil {
					closer.Close()
					fmt.Println("close dbs...")
				}
				os.Exit(0)
				success <- 0
				// kill -SIGQUIT XXXX
			case syscall.SIGQUIT:
				fmt.Println("Signal: stop and core dump SIGQUIT")
				if closer != nil {
					closer.Close()
					fmt.Println("close dbs...")
				}
				os.Exit(0)
				success <- 0
			default:
				fmt.Println("Unknown signal.")
				if closer != nil {
					closer.Close()
					fmt.Println("close dbs...")
				}
				os.Exit(0)
				success <- 1
			}
		}
	}()

	go modelProcessFunc()

	<-success
}

func init() {
	generateDBConfigFlag = &config.GenerateDBFlag{}
	generateTestDataConfigFlag = &config.GenerateTestDataFlag{}

	// common
	flag.BoolVar(&help, "h", false, "this help")
	flag.StringVar(&model, "model", "", fmt.Sprintf("model: %s or %s or %s",
		modelRun, modelGenerateTest, modelGenerateConfig))
	flag.StringVar(&confFile, "conf", "config.json", "conf file, json format")

	// generate conf
	flag.StringVar(&generateDBConfigFlag.Username, "generate-conf-db-username", "root", "generate conf db username")
	flag.StringVar(&generateDBConfigFlag.Password, "generate-conf-db-password", "root", "generate conf db password")
	flag.StringVar(&generateDBConfigFlag.Host, "generate-conf-db-host", "localhost", "generate conf db host")
	flag.Int64Var(&generateDBConfigFlag.Port, "generate-conf-db-port", 3306, "generate conf db port")
	flag.Int64Var(&generateDBConfigFlag.CalCount, "generate-conf-db-cal-count", 1000, "generate conf db cal count")
	flag.Int64Var(&generateDBConfigFlag.PerCount, "generate-conf-db-per-count", 1000, "generate conf db per count")
	flag.Float64Var(&generateDBConfigFlag.Percent, "generate-conf-db-percent", 0.5, "generate conf db percent")
	flag.StringVar(&generateDBConfigFlag.FileName, "generate-conf-file", "config.json", "generate conf file, json format")
	flag.StringVar(&generateDBConfigFlag.DBFile, "generate-conf-db-file", "dbfile.txt", "generate conf db names file")
	flag.StringVar(&generateDBConfigFlag.RegexpFile, "generate-conf-regexp-file", "regex.txt", "generate conf regexp file")
	flag.StringVar(&generateDBConfigFlag.LogLevel, "generate-conf-log-level", "info", "generate conf log level")
	flag.StringVar(&generateTestDataConfigFlag.LogFile, "generate-conf-test-log-file", "", "generate conf log file")

	// test data
	flag.StringVar(&generateTestDataConfigFlag.Username, "generate-test-db-username", "root", "generate conf db username")
	flag.StringVar(&generateTestDataConfigFlag.Password, "generate-test-db-password", "root", "generate conf db password")
	flag.StringVar(&generateTestDataConfigFlag.Host, "generate-test-db-host", "localhost", "generate conf db host")
	flag.Int64Var(&generateTestDataConfigFlag.Port, "generate-test-db-port", 3306, "generate conf db port")
	flag.Int64Var(&generateTestDataConfigFlag.TableCount, "generate-test-table-count", 1, "generate test table count")
	flag.Int64Var(&generateTestDataConfigFlag.TableDataCount, "generate-test-table-row-count", 10000, "generate test per table count")
	flag.Int64Var(&generateTestDataConfigFlag.TableFieldCount, "generate-test-table-field-count", 1, "generate test table filed count")
	flag.Int64Var(&generateTestDataConfigFlag.PoolCount, "generate-test-table-pool-count", 2000, "generate test pool count")
	flag.StringVar(&generateTestDataConfigFlag.LogFile, "generate-test-log-file", "", "generate test log file")
	flag.StringVar(&generateTestDataConfigFlag.LogLevel, "generate-test-log-level", "info", "generate test log level")
	flag.StringVar(&generateTestDataConfigFlag.DBFile, "generate-test-db-file", "dbfile.txt", "generate test db names file")

	// 改变默认的 Usage
	flag.Usage = usage
}

func usage() {
	fmt.Fprintf(os.Stderr, `kye-mask version: 0.3
Please Follow The Steps:

step 0 - (generate db config):
    ./kye-mask -model generate-conf -conf config.json -generate-db-file dbfile.txt -generate-conf-regexp-file regex.txt\
    -generate-conf-db-username username -generate-db-password password -generate-db-host host -generate-db-port port \
    -db-limit-count limit-count -db-percent 0.5

    -model generate-conf: generate db config
    -conf: generate config file
    -generate-conf-db-file: database file                             (must)
    -generate-conf-regexp-file: database file                             (must)
    -generate-conf-db-usename: db username                            (option, default:root)
    -generate-conf-db-password: db password                           (option, default:root)
    -generate-conf-db-host: db host ip                                (option, default:localhost)
    -generate-conf-db-port: db port                                   (option, default:3306)
    -db-limit-count: limit count for generate config    (option, default:1000)
step 1 - (start process)):
    ./kye-mask -model run -conf file
    -model run: start run
    -conf: config file
`)
}

func modelProcessFunc() {
	var err error
	switch model {
	case modelRun:
		log.Println("run ...")
		err = maskRun()
	case modelGenerateConfig:
		log.Println("generate conf ...")
		err = generateConf()
	case modelGenerateTest:
		log.Println("generate test ...")
		err = generateTestData()
	default:
		flag.Usage()
	}

	if err != nil {
		panic(err)
	}

	success <- 1
}

func maskRun() error {
	globalConfig, err := config.New(confFile)
	if err != nil {
		panic(err)
	}

	cfg, err := config.Logger(globalConfig.LogFile, globalConfig.LogLevel)
	if err != nil {
		panic(err)
	}

	zapLogger, err := cfg.Build()
	if err != nil {
		panic(err)
	}
	defer zapLogger.Sync()

	mask.SetLogger(zapLogger)
	newMask, err := mask.New(globalConfig)
	if err != nil {
		return err
	}
	closer = newMask
	newMask.Run()

	return nil
}

func generateTestData() error {
	cfg, err := config.Logger("", "info")
	if err != nil {
		panic(err)
	}

	zapLogger, err := cfg.Build()
	if err != nil {
		panic(err)
	}
	defer zapLogger.Sync()

	generator.SetLogger(zapLogger)
	t, err := generator.NewTestData(generateTestDataConfigFlag)
	if err != nil {
		return err
	}
	closer = t
	return t.Run()
}

func generateConf() error {
	cfg, err := config.Logger("", "info")
	if err != nil {
		panic(err)
	}

	zapLogger, err := cfg.Build()
	if err != nil {
		panic(err)
	}
	defer zapLogger.Sync()

	generator.SetLogger(zapLogger)
	gen, err := generator.NewConfig(generateDBConfigFlag)
	if err != nil {
		return err
	}
	closer = gen
	return gen.Run()
}

// DBCloser db closer
type DBCloser interface {
	Close()
}

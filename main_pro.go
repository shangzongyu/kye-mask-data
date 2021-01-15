//+build pro

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
	success = make(chan struct{})
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
				//success <- struct{}{}
				// kill -SIGINT XXXX or Ctrl+c
			case syscall.SIGINT:
				fmt.Println("Signal: SIGINT")
				if closer != nil {
					closer.Close()
					fmt.Println("close dbs...")
				}
				success <- struct{}{}
				// kill -SIGTERM XXXX
			case syscall.SIGTERM:
				fmt.Println("Signal: force stop SIGTERM")
				if closer != nil {
					closer.Close()
					fmt.Println("close dbs...")
				}
				os.Exit(0)

				//success <- struct{}{}
				// kill -SIGQUIT XXXX
			case syscall.SIGQUIT:
				fmt.Println("Signal: stop and core dump SIGQUIT")
				if closer != nil {
					closer.Close()
					fmt.Println("close dbs...")
				}
				os.Exit(0)
				//success <- struct{}{}
			default:
				fmt.Println("Unknown signal.")
				if closer != nil {
					closer.Close()
					fmt.Println("close dbs...")
				}
				os.Exit(0)
				//success <- struct{}{}
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

	// 改变默认的 Usage
	flag.Usage = usage
}

func usage() {
	fmt.Fprintf(os.Stderr, `kye-mask version: 0.2
    ./kye-mask -model run -conf file
`)
}

func modelProcessFunc() {
	var err error
	switch model {
	case modelRun:
		log.Println("process ...")
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

	success <- struct{}{}
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
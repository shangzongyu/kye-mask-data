package cmd

import (
    "mask-data/config"
    "mask-data/mask"

    "github.com/urfave/cli/v2"
)

func runAction(ctx *cli.Context) error  {
    confFile := ctx.String("config")
    globalConfig, err := config.New(confFile)
    if err != nil {
        return err
    }

    cfg, err := config.Logger(globalConfig.LogFile, globalConfig.LogLevel)
    if err != nil {
        return err
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

    go processSignal()

    <- success

    return nil
}

var (
    runCmd = &cli.Command{
        Name:         "run",
        Usage:        "run -config config_file",
        Description:  "run server to process",
        ArgsUsage:    "",
        Action:       runAction,
        OnUsageError: nil,
        Flags: []cli.Flag{
            &cli.StringFlag{
                Name:    "config",
                Aliases: []string{"c"},
                Value:   "config.json",
                Usage:   "the process config file",
            },
        },
    }
)

package cmd

import (
    "mask-data/config"

    "github.com/urfave/cli/v2"
)

func generateAction(ctx *cli.Context) error {
    var err error
    configFile := ctx.String("config")
    conf := &config.GenerateConfig{}
    if configFile != "" {
        conf, err = config.NewGenerateDB(configFile)
        if err != nil {
            return err
        }
    }

    if dbUserName := ctx.String("db-username"); dbUserName != "" {
        conf.DBUserName = dbUserName
    }

    if dbPassword := ctx.String("db-password"); dbPassword != "" {
        conf.DBPassword = dbPassword
    }

    if dbHost := ctx.String("db-host"); dbHost != "" {
        conf.DBHost = dbHost
    }

    if dbPort := ctx.Int64("db-port"); dbPort > 0 {
        conf.DBPort = dbPort
    }

    //if dbNames := ctx.StringSlice("db-names"); dbNames != nil && len(dbNames) > 0 {
    //    conf.DBNames = dbNames
    //}

    if outputFile := ctx.String("output_file"); outputFile != "" {
        conf.FileName = outputFile
    }

    if logLevel := ctx.String("log_level"); logLevel != "" {
        conf.LogLevel = logLevel
    }

    if logFile := ctx.String("log_file"); logFile != "" {
        conf.LogFile = logFile
    }

    //flag.Int64Var(&generateDBConfigFlag.CalCount, "generate-conf-db-cal-count", 1000, "generate conf db cal count")
    //flag.Int64Var(&generateDBConfigFlag.PerCount, "generate-conf-db-per-count", 1000, "generate conf db per count")
    //flag.Float64Var(&generateDBConfigFlag.Percent, "generate-conf-db-percent", 0.5, "generate conf db percent")
    //flag.StringVar(&generateDBConfigFlag.RegexpFile, "generate-conf-regexp-file", "regex.txt", "generate conf regexp file")

    return nil
}

var generateCmd = &cli.Command{
    Name:         "run",
    Usage:        "run -config config_file",
    Description:  "run server to process",
    ArgsUsage:    "",
    Action:       generateAction,
    OnUsageError: nil,
    Subcommands: []*cli.Command{
        {
            Name:         "config-file",
            Usage:        "generate config file",
            Description:  "generate config file for run",
            ArgsUsage:    "",
            Action:       generateAction,
            OnUsageError: nil,
            Flags: []cli.Flag{
                &cli.StringFlag{
                    Name:    "db-username",
                    Aliases: []string{"dbc"},
                    Value:   "root",
                    Usage:   "generate conf db username",
                },
                &cli.StringFlag{
                    Name:    "db-password",
                    Aliases: []string{"dbp"},
                    Value:   "root",
                    Usage:   "generate conf db password",
                },
                &cli.StringFlag{
                    Name:    "db-password",
                    Aliases: []string{"dbp"},
                    Value:   "root",
                    Usage:   "generate conf db password",
                },
                &cli.StringFlag{
                    Name:    "db-host",
                    Aliases: []string{"dbh"},
                    Value:   "root",
                    Usage:   "generate conf db host",
                },
                &cli.Int64Flag{
                    Name:    "db-port",
                    Aliases: []string{"dbpp"},
                    Value:   3306,
                    Usage:   "generate conf db port",
                },
                &cli.StringSliceFlag{
                    Name:    "db-names",
                    Aliases: []string{"dbn"},
                    Usage:   "generate conf db names",
                },
                &cli.StringFlag{
                    Name:    "output-file",
                    Aliases: []string{"opf"},
                    Value:   "config.json",
                    Usage:   "generate conf file name",
                },
                &cli.StringFlag{
                    Name:    "log-file",
                    Aliases: []string{"lf"},
                    Value:   "run.log",
                    Usage:   "the log file",
                },
                &cli.StringFlag{
                    Name:    "log-level",
                    Aliases: []string{"ll"},
                    Value:   "INFO",
                    Usage:   "the log level",
                },
            },
        },
        {
            Name:         "test-data",
            Usage:        "run -config config_file",
            Description:  "run server to process",
            ArgsUsage:    "",
            Action:       generateAction,
            OnUsageError: nil,
        },
    },
}

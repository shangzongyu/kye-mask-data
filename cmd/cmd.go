package cmd

import (
    "fmt"
    "os"

    "github.com/urfave/cli/v2"
)

type App struct {
    *cli.App
}

// Run run cli
func (a *App) Run() error {
    err := a.App.Run(os.Args)
    return err
}

// NewApp new app
func NewApp() *App{
    return &App{
        &cli.App{
            Name: "boom",
            Usage: "make an explosive entrance",
            Action: func(c *cli.Context) error {
                fmt.Println("boom! I say!")
                return nil
            },
            Commands: []*cli.Command{
                runCmd,
                generateCmd,
            },
        },
    }
}

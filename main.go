package main

import (
    "fmt"
    "os"

    "mask-data/cmd"
)

var (
sigs    = make(chan os.Signal, 1)
success = make(chan struct{})
)

func main() {
    app := cmd.NewApp()
    if err := app.Run(); err != nil {
        fmt.Printf("run error: %s", err.Error())
    }
}

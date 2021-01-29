package cmd

import (
    "fmt"
    "os"
    "syscall"
)

// DBCloser db closer
type DBCloser interface {
    Close()
}

var (
    sigs    = make(chan os.Signal, 1)
    success = make(chan struct{})
    closer  DBCloser
)

func processSignal() {
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
            success <- struct{}{}
            os.Exit(0)
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
            success <- struct{}{}
            os.Exit(0)
            // kill -SIGQUIT XXXX
        case syscall.SIGQUIT:
            fmt.Println("Signal: stop and core dump SIGQUIT")
            if closer != nil {
                closer.Close()
                fmt.Println("close dbs...")
            }
            success <- struct{}{}
            os.Exit(0)
        default:
            fmt.Println("Unknown signal.")
            if closer != nil {
                closer.Close()
                fmt.Println("close dbs...")
            }
            success <- struct{}{}
            os.Exit(0)
        }
    }
}

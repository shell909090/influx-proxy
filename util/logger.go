package util

import (
    logger "github.com/sirupsen/logrus"
    "io"
    "os"
)

var (
    CustomLog    *logger.Logger
)

func init() {
    CheckPathAndCreate("log")
    
    CustomLog = newLogger(os.Stdout)
}

func newLogger(outPut io.Writer) *logger.Logger {
    log := logger.New()
    log.SetOutput(outPut)
    log.SetLevel(logger.InfoLevel)
    log.SetFormatter(&logger.TextFormatter{
        DisableColors: true,
    })
    log.ReportCaller = true
    return log
}
